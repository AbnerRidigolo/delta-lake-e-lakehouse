"""
Camada GOLD - Performance e risco por estabelecimento (`gold.merchant_performance`).

Grao: **estabelecimento x mes**. E a tabela que responde as duas perguntas que
uma adquirente faz sobre a propria carteira de lojistas:

1. *Quanto esse lojista me da?* - TPV, receita de MDR, ticket medio, base de
   clientes unicos, crescimento.
2. *Quanto esse lojista me custa?* - taxa de chargeback, taxa de fraude,
   concentracao de clientes (sinal classico de laranja/conluio).

O campo `merchant_health` combina os dois lados em uma classificacao acionavel:
lojista rentavel e limpo, rentavel mas arriscado, ou candidato a
descredenciamento.

Regra de mercado usada como referencia: bandeiras colocam o lojista em programa
de monitoramento acima de ~1% de chargeback sobre o volume.

Execucao:
    python -m src.gold.gold_merchant_performance
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import data_quality as dq
from src.common.config import get_settings
from src.common.delta_io import register_table, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

TABLE = "merchant_performance"
# Limiar de chargeback a partir do qual a bandeira aciona monitoramento.
CHARGEBACK_ALERT_THRESHOLD = 0.01


def build(spark: SparkSession) -> DataFrame:
    cfg = get_settings()
    transactions = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))
    merchants = spark.read.format("delta").load(cfg.table_path("silver", "merchants"))

    approved = F.col("status") == "approved"

    monthly = transactions.groupBy("merchant_id", "event_month").agg(
        F.count("*").alias("txn_count"),
        F.sum(F.when(approved, 1).otherwise(0)).alias("txn_approved"),
        F.round(F.sum(F.when(approved, F.col("amount")).otherwise(0.0)), 2).alias("tpv"),
        F.round(F.avg(F.when(approved, F.col("amount"))), 2).alias("avg_ticket"),
        F.round(F.sum("mdr_revenue"), 2).alias("mdr_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("event_date").alias("active_days"),
        F.sum(F.when(F.col("status") == "chargeback", 1).otherwise(0)).alias("chargeback_count"),
        F.round(F.sum("chargeback_loss"), 2).alias("chargeback_amount"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("fraud_txn_count"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0.0)), 2)
         .alias("fraud_amount"),
        F.round(F.avg(F.when(F.col("is_card_not_present"), 1.0).otherwise(0.0)), 4).alias("cnp_share"),
        F.round(F.avg(F.when(F.col("is_night"), 1.0).otherwise(0.0)), 4).alias("night_share"),
        F.round(F.max("amount"), 2).alias("max_ticket"),
    )

    dim = merchants.select(
        "merchant_id", "merchant_name", "category", "mcc", "mcc_group", "city", "state",
        "region", "risk_tier", "risk_score", "mdr_rate", "is_new_merchant", "onboarding_date",
    )

    enriched = (
        monthly.join(F.broadcast(dim), on="merchant_id", how="inner")
        # --- Taxas de risco ---------------------------------------------------
        .withColumn("approval_rate",
                    F.round(F.col("txn_approved") / F.greatest(F.col("txn_count"), F.lit(1)), 4))
        .withColumn("chargeback_rate",
                    F.round(F.col("chargeback_count") / F.greatest(F.col("txn_count"), F.lit(1)), 5))
        .withColumn("fraud_rate",
                    F.round(F.col("fraud_txn_count") / F.greatest(F.col("txn_count"), F.lit(1)), 5))
        .withColumn("fraud_amount_rate",
                    F.round(F.col("fraud_amount") / F.greatest(F.col("tpv"), F.lit(0.01)), 5))
        # Concentracao: poucas contas gerando muito volume e sinal de laranja.
        .withColumn("txn_per_customer",
                    F.round(F.col("txn_count") / F.greatest(F.col("unique_customers"), F.lit(1)), 2))
        .withColumn("revenue_per_customer",
                    F.round(F.col("mdr_revenue") / F.greatest(F.col("unique_customers"), F.lit(1)), 2))
    )

    # --- Tendencia mes a mes: o lojista esta crescendo ou esvaziando? --------
    trend_window = Window.partitionBy("merchant_id").orderBy("event_month")
    with_trend = (
        enriched
        .withColumn("tpv_prev_month", F.lag("tpv").over(trend_window))
        .withColumn(
            "tpv_growth",
            F.round((F.col("tpv") - F.col("tpv_prev_month"))
                    / F.greatest(F.col("tpv_prev_month"), F.lit(0.01)), 4),
        )
        # Media movel de 3 meses suaviza sazonalidade antes de acionar alerta.
        .withColumn("tpv_ma3",
                    F.round(F.avg("tpv").over(trend_window.rowsBetween(-2, 0)), 2))
    )

    return (
        with_trend
        .withColumn(
            "merchant_health",
            F.when(F.col("chargeback_rate") >= CHARGEBACK_ALERT_THRESHOLD * 3, "descredenciar")
             .when((F.col("chargeback_rate") >= CHARGEBACK_ALERT_THRESHOLD)
                   | (F.col("fraud_rate") > 0.02), "monitoramento_bandeira")
             .when((F.col("tpv") > 50_000) & (F.col("chargeback_rate") < 0.002), "estrategico")
             .when(F.col("tpv_growth") < -0.5, "risco_de_evasao")
             .otherwise("saudavel"),
        )
        .withColumn("is_under_monitoring",
                    F.col("chargeback_rate") >= CHARGEBACK_ALERT_THRESHOLD)
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"gold_{TABLE}")

    with log_stage(log, f"gold.{TABLE}"):
        gold = build(spark)
        path = cfg.table_path("gold", TABLE)

        write_delta(gold, path, mode="overwrite", partition_by=["event_month"],
                    overwrite_schema=True,
                    comment="silver.transactions + silver.merchants -> gold.merchant_performance")
        register_table(spark, cfg.full_table_name("gold", TABLE), path)

        result = spark.read.format("delta").load(path)
        dq.run_quality_checks(
            spark, result, dataset=f"gold.{TABLE}",
            expectations=[
                dq.not_null("merchant_id"),
                dq.not_null("event_month"),
                dq.between("approval_rate", 0.0, 1.0),
                dq.between("chargeback_rate", 0.0, 1.0),
                dq.allowed_values("merchant_health",
                                  ["saudavel", "estrategico", "risco_de_evasao",
                                   "monitoramento_bandeira", "descredenciar"]),
                dq.row_count_min(1),
            ],
        )

        alerts = result.where(F.col("is_under_monitoring")).select("merchant_id").distinct().count()
        log.info("gold.%s: %s linhas | %s lojistas acima do limiar de chargeback",
                 TABLE, result.count(), alerts)
        return result.count()


if __name__ == "__main__":
    run()
