"""
Camada GOLD - KPIs financeiros diarios (`gold.financial_kpis_daily`).

Uma linha por dia, com a **DRE simplificada da fintech**:

    Receita
      + MDR            (taxa cobrada do lojista sobre a transacao aprovada)
      + Juros          (accrual diario sobre o saldo devedor da carteira)
    Perdas
      - Chargeback     (valor devolvido ao portador)
      - Fraude         (perda confirmada em transacoes fraudulentas aprovadas)
      - Write-off      (contratos levados a prejuizo)
    Recuperacao
      + Cobranca       (parte do prejuizo recuperada depois)
    = Resultado liquido do dia

Alem do resultado, a tabela traz os KPIs operacionais que o time acompanha
diariamente: TPV, ticket medio, taxa de aprovacao, taxa de chargeback, clientes
ativos e taxa de fraude (em volume e em valor).

Premissas explicitas (e o que mudaria em producao)
--------------------------------------------------
* O accrual de juros usa o **saldo devedor do snapshot atual** distribuido em
  30 dias. Com dados reais existiria uma tabela de posicao diaria da carteira,
  e o accrual sairia dela.
* As datas de default e de write-off sao derivadas de `days_past_due` contado
  para tras a partir da data de referencia. Em producao viriam do proprio
  sistema de cobranca.
Ambas as simplificacoes estao isoladas em funcoes proprias, para trocar a fonte
sem mexer no restante do job.

Execucao:
    python -m src.gold.gold_financial_kpis_daily
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

TABLE = "financial_kpis_daily"
# Dias entre o primeiro atraso e o reconhecimento do prejuizo (politica de
# write-off). Deve espelhar `WRITE_OFF_DPD` do gerador de dados.
WRITE_OFF_LAG_DAYS = 180
# Defasagem media entre o write-off e a entrada de recuperacao via cobranca.
RECOVERY_LAG_DAYS = 45


def _transaction_kpis(transactions: DataFrame) -> DataFrame:
    """KPIs transacionais por dia (adquirencia e antifraude)."""
    approved = F.col("status") == "approved"

    return transactions.groupBy("event_date").agg(
        F.count("*").alias("txn_count"),
        F.sum(F.when(approved, 1).otherwise(0)).alias("txn_approved"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("txn_declined"),
        F.sum(F.when(F.col("status") == "chargeback", 1).otherwise(0)).alias("txn_chargeback"),
        F.round(F.sum(F.when(approved, F.col("amount")).otherwise(0.0)), 2).alias("tpv"),
        F.round(F.avg(F.when(approved, F.col("amount"))), 2).alias("avg_ticket"),
        F.round(F.sum("mdr_revenue"), 2).alias("revenue_mdr"),
        F.round(F.sum("chargeback_loss"), 2).alias("loss_chargeback"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.countDistinct("merchant_id").alias("active_merchants"),
        # Fraude: contamos apenas o que foi aprovado (o que virou prejuizo real).
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_txn_count"),
        F.round(F.sum(F.when((F.col("is_fraud") == 1) & approved, F.col("amount"))
                      .otherwise(0.0)), 2).alias("loss_fraud"),
        # Mix de canais (para explicar variacao de receita: PIX nao paga MDR de cartao).
        F.round(F.avg(F.when(F.col("channel") == "pix", 1.0).otherwise(0.0)), 4).alias("pix_share"),
        F.round(F.avg(F.when(F.col("is_card_not_present"), 1.0).otherwise(0.0)), 4)
         .alias("cnp_share"),
    )


def _credit_daily(spark: SparkSession, contracts: DataFrame, calendar: DataFrame) -> DataFrame:
    """Accrual diario de juros e eventos de perda/recuperacao da carteira."""
    cfg = get_settings()
    reference_date = F.lit(cfg.data_generation["end_date"]).cast("date")

    # Datas derivadas dos eventos de credito (ver premissas no docstring).
    dated = (
        contracts
        .withColumn("first_default_date",
                    F.when(F.col("days_past_due") > 0,
                           F.date_sub(reference_date, F.col("days_past_due"))))
        .withColumn("write_off_date",
                    F.when(F.col("is_written_off"),
                           F.date_add(F.col("first_default_date"), WRITE_OFF_LAG_DAYS)))
        .withColumn("recovery_date",
                    F.when(F.col("recovery_amount") > 0,
                           F.date_add(F.col("write_off_date"), RECOVERY_LAG_DAYS)))
    )

    # --- Juros: accrual diario sobre a carteira viva naquele dia -------------
    accrual = (
        calendar.join(
            dated.where(F.col("status").isin("active", "default"))
                 .select("contract_id", "origination_date", "outstanding_balance",
                         "interest_rate_month"),
            on=F.col("origination_date") <= F.col("event_date"),
            how="inner",
        )
        .groupBy("event_date")
        .agg(
            F.round(F.sum(F.col("outstanding_balance") * F.col("interest_rate_month") / 30.0), 2)
             .alias("revenue_interest"),
            F.round(F.sum("outstanding_balance"), 2).alias("credit_portfolio_balance"),
            F.count("*").alias("active_contracts"),
        )
    )

    # --- Perdas e recuperacoes: eventos pontuais no tempo --------------------
    write_offs = (
        dated.where(F.col("write_off_date").isNotNull())
        .groupBy(F.col("write_off_date").alias("event_date"))
        .agg(F.round(F.sum("outstanding_balance"), 2).alias("loss_write_off"),
             F.count("*").alias("write_off_contracts"))
    )
    recoveries = (
        dated.where(F.col("recovery_date").isNotNull())
        .groupBy(F.col("recovery_date").alias("event_date"))
        .agg(F.round(F.sum("recovery_amount"), 2).alias("recovery_amount"))
    )

    return (
        accrual
        .join(write_offs, on="event_date", how="full_outer")
        .join(recoveries, on="event_date", how="full_outer")
    )


def build(spark: SparkSession) -> DataFrame:
    cfg = get_settings()
    transactions = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))
    contracts = spark.read.format("delta").load(cfg.table_path("silver", "credit_contracts"))

    # Calendario continuo: dias sem transacao precisam aparecer com zero, senao
    # a serie temporal fica com buracos e todo grafico de tendencia mente.
    bounds = transactions.agg(F.min("event_date").alias("start"),
                              F.max("event_date").alias("end")).collect()[0]
    calendar = spark.sql(
        f"SELECT explode(sequence(to_date('{bounds['start']}'), to_date('{bounds['end']}'), "
        "interval 1 day)) as event_date"
    )

    txn_kpis = _transaction_kpis(transactions)
    credit_kpis = _credit_daily(spark, contracts, calendar)

    daily = (
        calendar
        .join(txn_kpis, on="event_date", how="left")
        .join(credit_kpis, on="event_date", how="left")
    )

    # Zera os nulos: um dia sem evento vale 0, nao NULL.
    numeric_defaults = {
        "txn_count": 0, "txn_approved": 0, "txn_declined": 0, "txn_chargeback": 0,
        "tpv": 0.0, "avg_ticket": 0.0, "revenue_mdr": 0.0, "loss_chargeback": 0.0,
        "active_customers": 0, "active_merchants": 0, "fraud_txn_count": 0, "loss_fraud": 0.0,
        "pix_share": 0.0, "cnp_share": 0.0, "revenue_interest": 0.0,
        "credit_portfolio_balance": 0.0, "active_contracts": 0, "loss_write_off": 0.0,
        "write_off_contracts": 0, "recovery_amount": 0.0,
    }
    for column, default in numeric_defaults.items():
        daily = daily.withColumn(column, F.coalesce(F.col(column), F.lit(default)))

    return (
        daily
        # --- Resultado consolidado -------------------------------------------
        .withColumn("revenue_total", F.round(F.col("revenue_mdr") + F.col("revenue_interest"), 2))
        .withColumn("loss_total", F.round(
            F.col("loss_chargeback") + F.col("loss_fraud") + F.col("loss_write_off"), 2))
        .withColumn("net_result", F.round(
            F.col("revenue_total") - F.col("loss_total") + F.col("recovery_amount"), 2))
        .withColumn("net_margin",
                    F.round(F.col("net_result") / F.greatest(F.col("revenue_total"), F.lit(0.01)), 4))
        # --- Taxas operacionais ----------------------------------------------
        .withColumn("approval_rate",
                    F.round(F.col("txn_approved") / F.greatest(F.col("txn_count"), F.lit(1)), 4))
        .withColumn("chargeback_rate",
                    F.round(F.col("txn_chargeback") / F.greatest(F.col("txn_count"), F.lit(1)), 5))
        .withColumn("fraud_rate_volume",
                    F.round(F.col("fraud_txn_count") / F.greatest(F.col("txn_count"), F.lit(1)), 5))
        .withColumn("fraud_rate_value",
                    F.round(F.col("loss_fraud") / F.greatest(F.col("tpv"), F.lit(0.01)), 5))
        # Take rate: quanto a fintech captura de cada real transacionado.
        .withColumn("take_rate",
                    F.round(F.col("revenue_mdr") / F.greatest(F.col("tpv"), F.lit(0.01)), 5))
        # --- Dimensoes de tempo para o BI ------------------------------------
        .withColumn("year_month", F.date_format(F.col("event_date"), "yyyy-MM"))
        .withColumn("day_of_week", F.date_format(F.col("event_date"), "EEEE"))
        .withColumn("is_weekend", F.dayofweek(F.col("event_date")).isin(1, 7))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"gold_{TABLE}")

    with log_stage(log, f"gold.{TABLE}"):
        gold = build(spark)
        path = cfg.table_path("gold", TABLE)

        write_delta(gold, path, mode="overwrite", partition_by=["year_month"],
                    overwrite_schema=True, comment="silver -> gold.financial_kpis_daily")
        register_table(spark, cfg.full_table_name("gold", TABLE), path)

        result = spark.read.format("delta").load(path)
        dq.run_quality_checks(
            spark, result, dataset=f"gold.{TABLE}",
            expectations=[
                dq.not_null("event_date"),
                dq.unique("event_date"),
                dq.between("approval_rate", 0.0, 1.0),
                dq.between("chargeback_rate", 0.0, 1.0),
                dq.between("take_rate", 0.0, 0.2, severity=dq.SEVERITY_WARN),
                dq.freshness_max_days("event_date", 3650),
                dq.row_count_min(1),
            ],
        )

        totals = result.agg(
            F.round(F.sum("revenue_total"), 2).alias("receita"),
            F.round(F.sum("loss_total"), 2).alias("perdas"),
            F.round(F.sum("recovery_amount"), 2).alias("recuperacao"),
            F.round(F.sum("net_result"), 2).alias("resultado"),
            F.round(F.sum("tpv"), 2).alias("tpv"),
        ).collect()[0]
        log.info("gold.%s: TPV=R$ %s | receita=R$ %s | perdas=R$ %s | recuperacao=R$ %s | "
                 "resultado=R$ %s", TABLE, totals["tpv"], totals["receita"], totals["perdas"],
                 totals["recuperacao"], totals["resultado"])
        return result.count()


if __name__ == "__main__":
    run()
