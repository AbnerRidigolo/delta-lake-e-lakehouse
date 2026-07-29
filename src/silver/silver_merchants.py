"""
Camada SILVER - estabelecimentos (merchants).

Alem da limpeza padrao, esta tabela resolve a **taxonomia de MCC**: o codigo de
categoria que chega da adquirente e cru (`"5812"`), e aqui ele ganha nome de
negocio, agrupamento macro e um indicador de risco intrinseco - usado depois
pelas regras de fraude e pela visao de performance por lojista.

Execucao:
    python -m src.silver.silver_merchants
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
from src.generators import reference_data as ref
from src.silver.quarantine import quarantine

log = get_logger(__name__)

TABLE = "merchants"

# Agrupamento macro de MCC. Em producao viria de uma tabela de dominio mantida
# pelo time de negocio (ou do proprio Unity Catalog como tabela de referencia).
MCC_GROUPS = {
    "supermercado": "essencial",
    "farmacia": "essencial",
    "saude": "essencial",
    "posto_combustivel": "mobilidade",
    "mobilidade_urbana": "mobilidade",
    "restaurante": "consumo",
    "vestuario": "consumo",
    "varejo_diverso": "consumo",
    "eletronicos": "alto_ticket",
    "joalheria": "alto_ticket",
    "agencia_viagem": "alto_ticket",
    "servicos_digitais": "digital",
    "apostas_online": "digital_risco",
    "cripto_cambio": "digital_risco",
}


def _mcc_reference(spark: SparkSession) -> DataFrame:
    """Tabela de dominio de MCC construida a partir do catalogo de referencia."""
    rows = [
        {
            "mcc": mcc,
            "mcc_category": category,
            "mcc_group": MCC_GROUPS.get(category, "outros"),
            "mcc_expected_ticket": float(ticket),
            "mcc_intrinsic_risk": float(risk),
        }
        for mcc, category, _weight, ticket, risk in ref.MCC_CATALOG
    ]
    return spark.createDataFrame(rows)


def transform(spark: SparkSession, bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    deduped = T.deduplicate(
        bronze,
        keys=["merchant_id"],
        order_by=[F.col("source_extracted_at").desc_nulls_last(), F.col("_ingested_at").desc()],
    )

    typed = deduped.select(
        F.trim(F.col("merchant_id")).alias("merchant_id"),
        F.trim(F.col("merchant_name")).alias("merchant_name"),
        F.upper(F.trim(F.col("legal_name"))).alias("legal_name"),
        F.lpad(F.trim(F.col("mcc")), 4, "0").alias("mcc"),
        T.normalize_text("category").alias("category"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        F.initcap(F.trim(F.col("region"))).alias("region"),
        T.safe_date("onboarding_date").alias("onboarding_date"),
        T.safe_double("avg_ticket").alias("avg_ticket"),
        T.safe_double("risk_score").alias("risk_score"),
        T.normalize_text("risk_flag").alias("risk_flag"),
        T.safe_double("mdr_rate").alias("mdr_rate"),
        T.safe_boolean("is_active").alias("is_active"),
        F.col("_ingested_at"),
    )

    is_valid = (
        F.col("merchant_id").isNotNull()
        & F.col("mdr_rate").isNotNull()
        & (F.col("mdr_rate").between(0.0, 0.15))  # MDR fora disso e erro de origem
        & F.col("avg_ticket").isNotNull()
        & (F.col("avg_ticket") > 0)
    )
    reject_reason = (
        F.when(F.col("merchant_id").isNull(), "merchant_id ausente")
        .when(F.col("mdr_rate").isNull(), "MDR nao numerico")
        .when(~F.col("mdr_rate").between(0.0, 0.15), "MDR fora da faixa contratual")
        .when(F.col("avg_ticket").isNull() | (F.col("avg_ticket") <= 0), "ticket medio invalido")
        .otherwise("regra desconhecida")
    )
    valid, rejected = T.split_valid_invalid(typed, is_valid, reject_reason)

    reference_date = F.lit(get_settings().data_generation["end_date"]).cast("date")

    silver = (
        valid.join(F.broadcast(_mcc_reference(spark)), on="mcc", how="left")
        .withColumn("category", F.coalesce(F.col("mcc_category"), F.col("category")))
        .withColumn("mcc_group", F.coalesce(F.col("mcc_group"), F.lit("outros")))
        .withColumn("merchant_age_days", F.datediff(reference_date, F.col("onboarding_date")))
        # Lojista novo tem menos historico -> mais incerteza no monitoramento.
        .withColumn("is_new_merchant", F.col("merchant_age_days") < 180)
        # Reclassifica o risco combinando o score da adquirente com o risco do MCC.
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= 4.0, "alto")
            .when(F.col("risk_score") >= 2.0, "medio")
            .otherwise("baixo"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
        .drop("mcc_category")
    )
    return silver, rejected


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"silver_{TABLE}")

    with log_stage(log, f"silver.{TABLE}"):
        bronze = spark.read.format("delta").load(cfg.table_path("bronze", TABLE))
        silver, rejected = transform(spark, bronze)

        silver_path = cfg.table_path("silver", TABLE)
        write_delta(
            silver,
            silver_path,
            mode="overwrite",
            overwrite_schema=True,
            comment="bronze.merchants -> silver.merchants",
        )
        register_table(spark, cfg.full_table_name("silver", TABLE), silver_path)
        quarantine(rejected, TABLE)

        result = spark.read.format("delta").load(silver_path)
        dq.run_quality_checks(
            spark,
            result,
            dataset=f"silver.{TABLE}",
            expectations=[
                dq.not_null("merchant_id"),
                dq.unique("merchant_id"),
                dq.between("mdr_rate", 0.0, 0.15),
                dq.allowed_values("risk_tier", ["baixo", "medio", "alto"]),
                dq.not_null("mcc_group"),
                dq.row_count_min(1),
            ],
        )
        count = result.count()
        log.info("silver.%s: %s estabelecimentos", TABLE, count)
        return count


if __name__ == "__main__":
    run()
