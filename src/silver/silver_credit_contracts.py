"""
Camada SILVER - contratos de credito.

Aqui o dado operacional de credito ganha a semantica de risco que a area usa
no dia a dia:

* **Faixa de atraso (DPD bucket)** - o padrao de mercado para acompanhar
  inadimplencia (0, 1-15, 16-30, 31-60, 61-90, 91-180, 180+).
* **Provisao (proxy de PDD/ECL)** - percentual do saldo devedor provisionado
  por faixa, conforme parametros em `config/settings.yaml`. E uma simplificacao
  didatica do IFRS 9 / Res. 4.966, mas com a mesma mecanica: quanto pior a
  faixa, maior a fracao do saldo reconhecida como perda esperada.
* **NPL 90+** - marcador de default (>= 90 dias de atraso), a metrica que vai
  para o board.
* **Safra (vintage)** - mes de originacao, base de toda analise de cohort de
  credito ("a safra de marco esta pior que a de fevereiro?").

Execucao:
    python -m src.silver.silver_credit_contracts
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import data_quality as dq
from src.common import transforms as T
from src.common.config import get_settings
from src.common.delta_io import register_table, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark
from src.silver.quarantine import quarantine

log = get_logger(__name__)

TABLE = "credit_contracts"
VALID_STATUS = ["active", "settled", "default", "written_off"]
VALID_PRODUCTS = ["cartao_rotativo", "emprestimo_pessoal", "consignado", "bnpl", "capital_giro_pj"]


def _delinquency_bucket() -> F.Column:
    """Classifica o contrato na faixa de atraso padrao do mercado."""
    dpd = F.col("days_past_due")
    return (
        F.when(dpd <= 0, "current")
        .when(dpd <= 15, "dpd_1_15")
        .when(dpd <= 30, "dpd_16_30")
        .when(dpd <= 60, "dpd_31_60")
        .when(dpd <= 90, "dpd_61_90")
        .when(dpd <= 180, "dpd_91_180")
        .otherwise("dpd_180_plus")
    )


def _provision_rate(rates: dict) -> F.Column:
    """Mapeia faixa de atraso -> percentual de provisao (parametrizado no YAML)."""
    column = F.lit(None).cast("double")
    for bucket, rate in rates.items():
        column = F.when(F.col("delinquency_bucket") == bucket, F.lit(float(rate))).otherwise(column)
    return F.coalesce(column, F.lit(1.0))


def transform(
    spark: SparkSession, bronze: DataFrame, customers: DataFrame
) -> tuple[DataFrame, DataFrame]:
    cfg = get_settings()
    rules = cfg.business_rules
    reference_date = F.lit(cfg.data_generation["end_date"]).cast("date")

    deduped = T.deduplicate(
        bronze,
        keys=["contract_id"],
        order_by=[F.col("source_extracted_at").desc_nulls_last(), F.col("_ingested_at").desc()],
    )

    typed = deduped.select(
        F.trim(F.col("contract_id")).alias("contract_id"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        T.normalize_text("product").alias("product"),
        T.safe_double("principal_amount").alias("principal_amount"),
        T.safe_double("interest_rate_month").alias("interest_rate_month"),
        T.safe_int("term_months").alias("term_months"),
        T.safe_date("origination_date").alias("origination_date"),
        T.safe_int("installments_paid").alias("installments_paid"),
        T.safe_double("outstanding_balance").alias("outstanding_balance"),
        F.coalesce(T.safe_int("days_past_due"), F.lit(0)).alias("days_past_due"),
        T.normalize_text("status").alias("status"),
        F.coalesce(T.safe_double("recovery_amount"), F.lit(0.0)).alias("recovery_amount"),
        T.normalize_text("collateral").alias("collateral"),
        F.col("_ingested_at"),
    )

    is_valid = (
        F.col("contract_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("principal_amount").isNotNull()
        & (F.col("principal_amount") > 0)
        & F.col("origination_date").isNotNull()
        & F.col("term_months").isNotNull()
        & (F.col("term_months") > 0)
        & F.col("status").isin(VALID_STATUS)
        & F.col("product").isin(VALID_PRODUCTS)
    )
    reject_reason = (
        F.when(F.col("contract_id").isNull(), "contract_id ausente")
        .when(
            F.col("principal_amount").isNull() | (F.col("principal_amount") <= 0),
            "principal invalido",
        )
        .when(F.col("origination_date").isNull(), "data de originacao invalida")
        .when(F.col("term_months").isNull() | (F.col("term_months") <= 0), "prazo invalido")
        .when(~F.col("status").isin(VALID_STATUS), "status fora do dominio")
        .when(~F.col("product").isin(VALID_PRODUCTS), "produto fora do dominio")
        .otherwise("regra desconhecida")
    )
    valid, rejected_technical = T.split_valid_invalid(typed, is_valid, reject_reason)

    # Integridade referencial com o cadastro de clientes.
    customer_dim = customers.select(
        "customer_id",
        "customer_key",
        F.col("segment").alias("customer_segment"),
        F.col("risk_band").alias("customer_risk_band"),
        F.col("monthly_income").alias("customer_income"),
        F.col("state").alias("customer_state"),
    )
    joined = valid.join(F.broadcast(customer_dim), on="customer_id", how="left")
    referenced, rejected_orphan = T.split_valid_invalid(
        joined, F.col("customer_key").isNotNull(), F.lit("cliente inexistente no cadastro")
    )

    reject_cols = [
        "contract_id",
        "customer_id",
        "product",
        "principal_amount",
        "status",
        "_reject_reason",
    ]
    rejected = rejected_technical.select(*reject_cols).unionByName(
        rejected_orphan.select(*reject_cols)
    )

    default_dpd = int(rules["default_days_past_due"])

    silver = (
        referenced.withColumn("delinquency_bucket", _delinquency_bucket())
        .withColumn("provision_rate", _provision_rate(rules["provision_rates"]))
        # Provisao = saldo exposto x percentual da faixa (proxy de perda esperada).
        .withColumn(
            "provision_amount", F.round(F.col("outstanding_balance") * F.col("provision_rate"), 2)
        )
        .withColumn("is_npl", F.col("days_past_due") >= default_dpd)
        .withColumn("is_written_off", F.col("status") == "written_off")
        .withColumn("vintage", F.date_format(F.col("origination_date"), "yyyy-MM"))
        .withColumn(
            "months_on_book", F.floor(F.months_between(reference_date, F.col("origination_date")))
        )
        # Parcela pela Tabela Price: PMT = PV * i / (1 - (1+i)^-n)
        .withColumn(
            "monthly_installment",
            F.when(
                F.col("interest_rate_month") > 0,
                F.round(
                    F.col("principal_amount")
                    * F.col("interest_rate_month")
                    / (1 - F.pow(1 + F.col("interest_rate_month"), -F.col("term_months"))),
                    2,
                ),
            ).otherwise(F.round(F.col("principal_amount") / F.col("term_months"), 2)),
        )
        # Comprometimento de renda: principal driver de inadimplencia.
        .withColumn(
            "debt_to_income",
            F.round(
                F.col("monthly_installment") / F.greatest(F.col("customer_income"), F.lit(1.0)), 4
            ),
        )
        .withColumn(
            "progress_ratio",
            F.round(F.col("installments_paid") / F.greatest(F.col("term_months"), F.lit(1)), 4),
        )
        # EAD (exposure at default) simplificado: saldo em aberto.
        .withColumn("exposure_at_default", F.col("outstanding_balance"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    return silver, rejected


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"silver_{TABLE}")

    with log_stage(log, f"silver.{TABLE}"):
        bronze = spark.read.format("delta").load(cfg.table_path("bronze", TABLE))
        customers = spark.read.format("delta").load(cfg.table_path("silver", "customers"))

        silver, rejected = transform(spark, bronze, customers)

        silver_path = cfg.table_path("silver", TABLE)
        write_delta(
            silver,
            silver_path,
            mode="overwrite",
            partition_by=["vintage"],
            overwrite_schema=True,
            comment="bronze.credit_contracts -> silver.credit_contracts",
        )
        register_table(spark, cfg.full_table_name("silver", TABLE), silver_path)
        quarantine(rejected, TABLE)

        result = spark.read.format("delta").load(silver_path)
        dq.run_quality_checks(
            spark,
            result,
            dataset=f"silver.{TABLE}",
            expectations=[
                dq.not_null("contract_id"),
                dq.unique("contract_id"),
                dq.allowed_values("status", VALID_STATUS),
                dq.allowed_values("product", VALID_PRODUCTS),
                dq.between("provision_rate", 0.0, 1.0),
                dq.between("days_past_due", 0, 3650),
                dq.between("debt_to_income", 0.0, 5.0, severity=dq.SEVERITY_WARN, threshold=0.10),
                dq.row_count_min(1),
            ],
        )
        count = result.count()
        log.info("silver.%s: %s contratos", TABLE, count)
        return count


if __name__ == "__main__":
    run()
