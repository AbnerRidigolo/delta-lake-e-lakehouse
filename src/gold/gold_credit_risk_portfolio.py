"""
Camada GOLD - Carteira de risco de credito (`gold.credit_risk_portfolio`).

Grao: **safra (vintage) x produto x faixa de risco**. E o corte que a area de
risco usa para responder "a originacao piorou?" - porque olhar a inadimplencia
da carteira inteira esconde o problema: uma safra ruim fica diluida no estoque.

Metricas
--------
* **EAD** (Exposure at Default) - saldo devedor exposto.
* **PD proxy** - fracao de contratos com atraso >= 90 dias na safra. Nao e um
  modelo de PD; e a taxa observada, que serve de ancora e de backtest.
* **LGD** - perda dado o default. Estimada como `1 - taxa de recuperacao
  observada`, com piso conservador e ajuste para produto com garantia
  (consignado tem LGD estruturalmente menor).
* **Perda esperada (EL)** = EAD x PD x LGD - a formula de Basileia, aplicada
  aqui de forma simplificada e transparente.
* **Indice de cobertura** = provisao / saldo inadimplente. Abaixo de 1 significa
  que o balanco esta subprovisionado para a carteira problematica.

Execucao:
    python -m src.gold.gold_credit_risk_portfolio
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

TABLE = "credit_risk_portfolio"

# LGD base por produto. Consignado tem desconto em folha (recuperacao alta);
# rotativo e BNPL sao sem garantia (perda quase total no default).
LGD_BY_PRODUCT = {
    "consignado": 0.25,
    "capital_giro_pj": 0.55,
    "emprestimo_pessoal": 0.65,
    "bnpl": 0.80,
    "cartao_rotativo": 0.85,
}


def _lgd_column() -> "F.Column":
    column = F.lit(0.75)
    for product, lgd in LGD_BY_PRODUCT.items():
        column = F.when(F.col("product") == product, F.lit(lgd)).otherwise(column)
    return column


def build(spark: SparkSession) -> DataFrame:
    cfg = get_settings()
    contracts = spark.read.format("delta").load(cfg.table_path("silver", "credit_contracts"))

    aggregated = contracts.groupBy("vintage", "product", "customer_risk_band").agg(
        # --- Volume -----------------------------------------------------------
        F.count("*").alias("contracts"),
        F.countDistinct("customer_id").alias("customers"),
        F.round(F.sum("principal_amount"), 2).alias("principal_originated"),
        F.round(F.sum("exposure_at_default"), 2).alias("ead"),
        F.round(F.avg("principal_amount"), 2).alias("avg_ticket"),
        F.round(F.avg("term_months"), 1).alias("avg_term_months"),
        F.round(F.avg("interest_rate_month"), 5).alias("avg_monthly_rate"),
        F.round(F.avg("debt_to_income"), 4).alias("avg_debt_to_income"),
        F.round(F.avg("months_on_book"), 1).alias("avg_months_on_book"),

        # --- Inadimplencia ----------------------------------------------------
        F.sum(F.col("is_npl").cast("int")).alias("npl_contracts"),
        F.round(F.sum(F.when(F.col("is_npl"), F.col("outstanding_balance")).otherwise(0.0)), 2)
         .alias("npl_balance"),
        F.round(F.sum("provision_amount"), 2).alias("provision_amount"),
        F.round(F.avg("days_past_due"), 1).alias("avg_days_past_due"),
        F.sum(F.col("is_written_off").cast("int")).alias("written_off_contracts"),
        F.round(F.sum(F.when(F.col("is_written_off"), F.col("outstanding_balance")).otherwise(0.0)), 2)
         .alias("written_off_balance"),
        F.round(F.sum("recovery_amount"), 2).alias("recovery_amount"),

        # --- Distribuicao por faixa de atraso (util para o dashboard) ---------
        F.round(F.avg(F.when(F.col("delinquency_bucket") == "current", 1.0).otherwise(0.0)), 4)
         .alias("share_current"),
        F.round(F.avg(F.when(F.col("delinquency_bucket").isin("dpd_1_15", "dpd_16_30"), 1.0)
                      .otherwise(0.0)), 4).alias("share_early_delinquency"),
        F.round(F.avg(F.when(F.col("delinquency_bucket").isin("dpd_91_180", "dpd_180_plus"), 1.0)
                      .otherwise(0.0)), 4).alias("share_late_delinquency"),
    )

    return (
        aggregated
        # PD observada na safra (taxa de contratos em default).
        .withColumn("pd_observed", F.round(F.col("npl_contracts") / F.col("contracts"), 4))
        # LGD: mistura a premissa por produto com a recuperacao efetivamente vista.
        .withColumn("lgd_assumption", _lgd_column())
        .withColumn(
            "lgd_observed",
            F.when(
                F.col("written_off_balance") > 0,
                F.round(1 - F.least(F.col("recovery_amount") / F.col("written_off_balance"),
                                    F.lit(1.0)), 4),
            ).otherwise(F.col("lgd_assumption")),
        )
        # Perda esperada = EAD x PD x LGD (Basileia, versao simplificada).
        .withColumn(
            "expected_loss",
            F.round(F.col("ead") * F.col("pd_observed") * F.col("lgd_assumption"), 2),
        )
        .withColumn("expected_loss_rate",
                    F.round(F.col("expected_loss") / F.greatest(F.col("ead"), F.lit(1.0)), 5))
        .withColumn("npl_ratio",
                    F.round(F.col("npl_balance") / F.greatest(F.col("ead"), F.lit(1.0)), 4))
        # Cobertura: quanto da carteira problematica ja esta provisionado.
        .withColumn(
            "coverage_ratio",
            F.round(F.col("provision_amount") / F.greatest(F.col("npl_balance"), F.lit(1.0)), 4),
        )
        .withColumn("net_recovery_rate",
                    F.round(F.col("recovery_amount")
                            / F.greatest(F.col("written_off_balance"), F.lit(1.0)), 4))
        # Sinalizacao para o comite de credito.
        .withColumn(
            "portfolio_alert",
            F.when(F.col("pd_observed") >= 0.25, "safra_critica")
             .when(F.col("pd_observed") >= 0.15, "safra_em_deterioracao")
             .when(F.col("coverage_ratio") < 0.6, "subprovisionada")
             .otherwise("dentro_do_esperado"),
        )
        .withColumn("reference_date", F.lit(cfg.data_generation["end_date"]).cast("date"))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"gold_{TABLE}")

    with log_stage(log, f"gold.{TABLE}"):
        gold = build(spark)
        path = cfg.table_path("gold", TABLE)

        write_delta(gold, path, mode="overwrite", partition_by=["product"],
                    overwrite_schema=True,
                    comment="silver.credit_contracts -> gold.credit_risk_portfolio")
        register_table(spark, cfg.full_table_name("gold", TABLE), path)

        result = spark.read.format("delta").load(path)
        dq.run_quality_checks(
            spark, result, dataset=f"gold.{TABLE}",
            expectations=[
                dq.not_null("vintage"),
                dq.between("pd_observed", 0.0, 1.0),
                dq.between("lgd_assumption", 0.0, 1.0),
                dq.between("expected_loss_rate", 0.0, 1.0),
                dq.allowed_values("portfolio_alert",
                                  ["dentro_do_esperado", "subprovisionada",
                                   "safra_em_deterioracao", "safra_critica"]),
                dq.row_count_min(1),
            ],
        )

        totals = result.agg(
            F.round(F.sum("ead"), 2).alias("ead"),
            F.round(F.sum("expected_loss"), 2).alias("expected_loss"),
            F.round(F.sum("npl_balance") / F.sum("ead"), 4).alias("npl_ratio"),
        ).collect()[0]
        log.info("gold.%s: EAD=R$ %s | perda esperada=R$ %s | NPL=%.2f%%", TABLE,
                 totals["ead"], totals["expected_loss"], (totals["npl_ratio"] or 0) * 100)
        return result.count()


if __name__ == "__main__":
    run()
