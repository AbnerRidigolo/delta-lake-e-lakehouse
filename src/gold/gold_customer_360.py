"""
Camada GOLD - Visao 360 do cliente (`gold.customer_360`).

Uma linha por cliente, reunindo tres angulos que normalmente vivem em sistemas
separados:

* **Comportamento transacional (RFM)** - recencia, frequencia, valor, mix de
  canais, diversidade de lojistas e dispositivos.
* **Credito** - exposicao, pior faixa de atraso, provisao, flag de NPL 90+.
* **Relacionamento** - tempo de casa, safra de aquisicao, risco de churn.

E fechada com um **score agregado (0-1000)** e um rating (AAA..D). O score nao
substitui um modelo estatistico: e uma regra transparente e auditavel, do tipo
que uma area de risco usa como baseline e como sanity check do modelo de ML.

Consumo tipico: CRM, politica de limite, campanha de retencao, e como conjunto
de features de cliente na feature store.

Execucao:
    python -m src.gold.gold_customer_360
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import data_quality as dq
from src.common.config import get_settings
from src.common.delta_io import register_table, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

TABLE = "customer_360"


def _transaction_aggregates(transactions: DataFrame, reference_date) -> DataFrame:
    """Agrega o comportamento transacional por cliente (janela total + 90 dias)."""
    approved = F.col("status") == "approved"

    return transactions.groupBy("customer_id").agg(
        # --- Recencia / Frequencia / Valor (RFM classico) -------------------
        F.max("event_date").alias("last_transaction_date"),
        F.min("event_date").alias("first_transaction_date"),
        F.count("*").alias("txn_count_total"),
        F.sum(F.when(approved, F.col("amount")).otherwise(0.0)).alias("tpv_total"),
        F.round(F.avg(F.when(approved, F.col("amount"))), 2).alias("avg_ticket"),
        F.round(F.expr("percentile_approx(amount, 0.5)"), 2).alias("median_ticket"),
        F.round(F.stddev(F.when(approved, F.col("amount"))), 2).alias("stddev_ticket"),
        F.max("amount").alias("max_ticket"),
        # --- Qualidade da relacao -------------------------------------------
        F.round(F.avg(F.when(approved, 1.0).otherwise(0.0)), 4).alias("approval_rate"),
        F.sum(F.when(F.col("status") == "chargeback", 1).otherwise(0)).alias("chargeback_count"),
        F.sum("chargeback_loss").alias("chargeback_amount"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("confirmed_fraud_count"),
        F.round(F.sum("mdr_revenue"), 2).alias("mdr_revenue_generated"),
        # --- Diversidade (sinal de comportamento atipico) --------------------
        F.countDistinct("merchant_id").alias("distinct_merchants"),
        F.countDistinct("device_id").alias("distinct_devices"),
        F.countDistinct("geo_state").alias("distinct_states"),
        F.countDistinct("event_date").alias("active_days"),
        # --- Mix de canais ---------------------------------------------------
        F.round(F.avg(F.when(F.col("channel") == "pix", 1.0).otherwise(0.0)), 4).alias("pix_share"),
        F.round(F.avg(F.when(F.col("is_card_not_present"), 1.0).otherwise(0.0)), 4).alias(
            "card_not_present_share"
        ),
        F.round(F.avg(F.when(F.col("is_night"), 1.0).otherwise(0.0)), 4).alias("night_share"),
        F.round(F.avg(F.when(F.col("is_weekend"), 1.0).otherwise(0.0)), 4).alias("weekend_share"),
        # --- Janela curta (ultimos 90 dias): tendencia recente ---------------
        F.sum(F.when(F.col("event_date") >= F.date_sub(reference_date, 90), 1).otherwise(0)).alias(
            "txn_count_90d"
        ),
        F.round(
            F.sum(
                F.when(
                    (F.col("event_date") >= F.date_sub(reference_date, 90)) & approved,
                    F.col("amount"),
                ).otherwise(0.0)
            ),
            2,
        ).alias("tpv_90d"),
        F.round(
            F.sum(
                F.when(
                    (F.col("event_date") >= F.date_sub(reference_date, 30)) & approved,
                    F.col("amount"),
                ).otherwise(0.0)
            ),
            2,
        ).alias("tpv_30d"),
    )


def _credit_aggregates(contracts: DataFrame) -> DataFrame:
    """Agrega a posicao de credito por cliente."""
    return contracts.groupBy("customer_id").agg(
        F.count("*").alias("credit_contracts_count"),
        F.sum(F.when(F.col("status").isin("active", "default"), 1).otherwise(0)).alias(
            "active_contracts_count"
        ),
        F.round(F.sum("principal_amount"), 2).alias("credit_principal_total"),
        F.round(F.sum("outstanding_balance"), 2).alias("credit_outstanding_total"),
        F.round(F.sum("provision_amount"), 2).alias("credit_provision_total"),
        F.max("days_past_due").alias("max_days_past_due"),
        F.max(F.col("is_npl").cast("int")).alias("has_npl"),
        F.max(F.col("is_written_off").cast("int")).alias("has_written_off"),
        F.round(F.sum("recovery_amount"), 2).alias("credit_recovery_total"),
        F.round(F.max("debt_to_income"), 4).alias("max_debt_to_income"),
        F.round(F.sum("monthly_installment"), 2).alias("monthly_debt_service"),
    )


def _customer_score() -> F.Column:
    """Score de relacionamento/risco de 0 a 1000 (quanto maior, melhor).

    Regra transparente e auditavel: parte de 1000 e desconta pontos por cada
    fator de risco observado. Cada peso e uma decisao de politica de credito -
    documentada aqui em vez de escondida em um notebook perdido.
    """
    penalties = (
        # Faixa de risco de bureau/onboarding
        F.when(F.col("risk_band") == "A", 0)
        .when(F.col("risk_band") == "B", 60)
        .when(F.col("risk_band") == "C", 140)
        .when(F.col("risk_band") == "D", 240)
        .otherwise(360)
        # Atraso: 1 ponto por dia, limitado a 180
        + F.least(F.coalesce(F.col("max_days_past_due"), F.lit(0)), F.lit(180))
        # Default e prejuizo pesam de forma absoluta
        + F.when(F.col("has_npl") == 1, 200).otherwise(0)
        + F.when(F.col("has_written_off") == 1, 150).otherwise(0)
        # Comprometimento de renda acima de 40% e alerta classico
        + F.when(F.coalesce(F.col("max_debt_to_income"), F.lit(0.0)) > 0.4, 80).otherwise(0)
        # Historico de fraude confirmada e de contestacao
        + F.when(F.coalesce(F.col("confirmed_fraud_count"), F.lit(0)) > 0, 90).otherwise(0)
        + F.least(F.coalesce(F.col("chargeback_count"), F.lit(0)) * 25, F.lit(100))
        # Inatividade recente
        + F.when(F.col("is_churn_risk"), 60).otherwise(0)
    )
    bonuses = (
        # Tempo de casa: ate 60 pontos
        F.least(F.coalesce(F.col("tenure_days"), F.lit(0)) / 12.0, F.lit(60.0))
        # Engajamento transacional: ate 40 pontos
        + F.least(F.coalesce(F.col("txn_count_90d"), F.lit(0)) * 1.5, F.lit(40.0))
        # Alta taxa de aprovacao indica capacidade de pagamento saudavel
        + (F.coalesce(F.col("approval_rate"), F.lit(0.0)) * 40.0)
    )
    return F.greatest(F.least(F.lit(1000) - penalties + bonuses, F.lit(1000)), F.lit(0))


def build(spark: SparkSession) -> DataFrame:
    cfg = get_settings()
    reference_date = F.lit(cfg.data_generation["end_date"]).cast("date")
    churn_days = int(cfg.business_rules["churn_inactivity_days"])

    customers = spark.read.format("delta").load(cfg.table_path("silver", "customers"))
    transactions = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))
    contracts = spark.read.format("delta").load(cfg.table_path("silver", "credit_contracts"))

    txn_agg = _transaction_aggregates(transactions, reference_date)
    credit_agg = _credit_aggregates(contracts)

    base = (
        customers.select(
            "customer_id",
            "customer_key",
            "full_name",
            "cpf_masked",
            "email_masked",
            "city",
            "state",
            "region",
            "segment",
            "risk_band",
            "status",
            "age",
            "age_band",
            "monthly_income",
            "income_band",
            "credit_limit",
            "limit_to_income_ratio",
            "signup_date",
            "signup_cohort",
            "tenure_days",
            "tenure_band",
            "acquisition_channel",
        )
        .join(txn_agg, on="customer_id", how="left")
        .join(credit_agg, on="customer_id", how="left")
    )

    enriched = (
        base
        # --- Recencia e churn ------------------------------------------------
        .withColumn(
            "days_since_last_transaction",
            F.datediff(reference_date, F.col("last_transaction_date")),
        )
        .withColumn(
            "is_churn_risk",
            # Cliente sem transacao ha mais de N dias, ou que nunca transacionou,
            # ou marcado como inativo no cadastro.
            F.coalesce(F.col("days_since_last_transaction"), F.lit(9999)) > churn_days,
        )
        .withColumn(
            "churn_reason",
            F.when(F.col("last_transaction_date").isNull(), "nunca transacionou")
            .when(F.col("days_since_last_transaction") > churn_days * 3, "inativo_longo")
            .when(F.col("days_since_last_transaction") > churn_days, "inativo_recente")
            .otherwise("ativo"),
        )
        # --- Tendencia: os ultimos 30 dias contra a media dos 90 -------------
        .withColumn(
            "spend_trend_ratio",
            F.round(F.col("tpv_30d") / F.greatest(F.col("tpv_90d") / 3.0, F.lit(0.01)), 3),
        )
        # --- Exposicao total e headroom de limite ----------------------------
        .withColumn(
            "total_exposure", F.round(F.coalesce(F.col("credit_outstanding_total"), F.lit(0.0)), 2)
        )
        .withColumn(
            "limit_utilization",
            F.round(F.col("total_exposure") / F.greatest(F.col("credit_limit"), F.lit(1.0)), 4),
        )
        .withColumn("is_delinquent", F.coalesce(F.col("max_days_past_due"), F.lit(0)) > 0)
        .withColumn("has_npl", F.coalesce(F.col("has_npl"), F.lit(0)))
        .withColumn("has_written_off", F.coalesce(F.col("has_written_off"), F.lit(0)))
    )

    scored = (
        enriched.withColumn("customer_score", F.round(_customer_score(), 0).cast("int"))
        .withColumn(
            "rating",
            F.when(F.col("customer_score") >= 850, "AAA")
            .when(F.col("customer_score") >= 720, "AA")
            .when(F.col("customer_score") >= 600, "A")
            .when(F.col("customer_score") >= 480, "B")
            .when(F.col("customer_score") >= 350, "C")
            .otherwise("D"),
        )
        # Segmentacao acionavel para CRM: cruza valor gerado com risco.
        .withColumn(
            "value_risk_segment",
            F.when(
                (F.col("tpv_90d") > 5000) & (F.col("customer_score") >= 700),
                "alto_valor_baixo_risco",
            )
            .when(
                (F.col("tpv_90d") > 5000) & (F.col("customer_score") < 700), "alto_valor_alto_risco"
            )
            .when(F.col("is_churn_risk"), "em_risco_de_churn")
            .when(F.col("customer_score") < 480, "monitorar_credito")
            .otherwise("base_regular"),
        )
        .withColumn("reference_date", reference_date)
        .withColumn("_gold_processed_at", F.current_timestamp())
    )

    # Clientes sem transacao ficam com NULL nos agregados; zeramos os contadores
    # para a tabela ser diretamente consumivel por BI sem tratar nulo.
    zero_fill = [
        "txn_count_total",
        "txn_count_90d",
        "tpv_total",
        "tpv_90d",
        "tpv_30d",
        "chargeback_count",
        "confirmed_fraud_count",
        "distinct_merchants",
        "distinct_devices",
        "credit_contracts_count",
        "credit_outstanding_total",
        "credit_provision_total",
        "max_days_past_due",
        "mdr_revenue_generated",
    ]
    for column in zero_fill:
        scored = scored.withColumn(column, F.coalesce(F.col(column), F.lit(0)))

    return scored


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"gold_{TABLE}")

    with log_stage(log, f"gold.{TABLE}"):
        gold = build(spark)
        path = cfg.table_path("gold", TABLE)

        write_delta(
            gold,
            path,
            mode="overwrite",
            partition_by=["segment"],
            overwrite_schema=True,
            comment="silver -> gold.customer_360",
        )
        register_table(spark, cfg.full_table_name("gold", TABLE), path)

        result = spark.read.format("delta").load(path)
        dq.run_quality_checks(
            spark,
            result,
            dataset=f"gold.{TABLE}",
            expectations=[
                dq.not_null("customer_id"),
                dq.unique("customer_id"),
                dq.between("customer_score", 0, 1000),
                dq.allowed_values("rating", ["AAA", "AA", "A", "B", "C", "D"]),
                dq.between("approval_rate", 0.0, 1.0, severity=dq.SEVERITY_WARN),
                dq.not_null("value_risk_segment"),
                dq.row_count_min(1),
            ],
        )
        count = result.count()
        log.info(
            "gold.%s: %s clientes | churn=%s | NPL=%s",
            TABLE,
            count,
            result.where(F.col("is_churn_risk")).count(),
            result.where(F.col("has_npl") == 1).count(),
        )
        return count


if __name__ == "__main__":
    run()
