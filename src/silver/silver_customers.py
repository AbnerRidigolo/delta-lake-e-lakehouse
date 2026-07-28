"""
Camada SILVER - clientes.

Transformacoes aplicadas
------------------------
1. **Deduplicacao**: a origem reenvia o mesmo `customer_id`; ficamos com a
   versao mais recente (janela ordenada por data de extracao/ingestao).
2. **Tipagem segura**: renda e limite viram `double`; datas viram `date`.
   Valor invalido vira NULL e o registro vai para a quarentena.
3. **Normalizacao**: categorias em minusculo e sem espacos (`"  PREMIUM "` ->
   `"premium"`), o que evita que o mesmo segmento apareca 3 vezes no dashboard.
4. **LGPD**: CPF e e-mail sao mascarados e o documento ganha um hash com salt
   (`customer_key`) que permite join sem expor o dado pessoal.
5. **Derivacoes**: idade, faixa etaria, tempo de casa e comprometimento de
   limite - atributos que a Gold e a feature store consomem direto.

Execucao:
    python -m src.silver.silver_customers
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

TABLE = "customers"
VALID_SEGMENTS = ["varejo", "premium", "private", "pj_micro"]
VALID_STATUS = ["active", "inactive", "blocked"]


def transform(spark: SparkSession, bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Aplica limpeza e enriquecimento. Retorna (silver, rejeitados)."""
    cfg = get_settings()
    reference_date = F.lit(cfg.data_generation["end_date"]).cast("date")

    # --- 1) Deduplicacao: uma linha por cliente, a mais recente --------------
    deduped = T.deduplicate(
        bronze,
        keys=["customer_id"],
        order_by=[F.col("source_extracted_at").desc_nulls_last(), F.col("_ingested_at").desc()],
    )

    # --- 2) Tipagem + normalizacao -------------------------------------------
    typed = deduped.select(
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.initcap(F.trim(F.col("full_name"))).alias("full_name"),
        T.mask_document("cpf").alias("cpf_masked"),
        T.mask_email("email").alias("email_masked"),
        T.hash_pii("cpf").alias("customer_key"),
        T.safe_date("birth_date").alias("birth_date"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        F.initcap(F.trim(F.col("region"))).alias("region"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        T.safe_date("signup_date").alias("signup_date"),
        T.normalize_text("acquisition_channel").alias("acquisition_channel"),
        T.normalize_text("segment").alias("segment"),
        T.safe_double("monthly_income").alias("monthly_income"),
        T.safe_double("credit_limit").alias("credit_limit"),
        F.upper(F.trim(F.col("risk_band"))).alias("risk_band"),
        T.normalize_text("status").alias("status"),
        F.col("_ingested_at"),
        F.col("_source_system"),
    )

    # --- 3) Separacao valido / rejeitado --------------------------------------
    is_valid = (
        F.col("customer_id").isNotNull()
        & (F.length(F.col("customer_id")) > 0)
        & F.col("monthly_income").isNotNull()
        & (F.col("monthly_income") > 0)
        & F.col("signup_date").isNotNull()
        & F.col("segment").isin(VALID_SEGMENTS)
    )
    reject_reason = (
        F.when(F.col("customer_id").isNull(), "customer_id ausente")
        .when(F.col("monthly_income").isNull(), "renda nao numerica")
        .when(F.col("monthly_income") <= 0, "renda nao positiva")
        .when(F.col("signup_date").isNull(), "data de cadastro invalida")
        .when(~F.col("segment").isin(VALID_SEGMENTS), "segmento fora do dominio")
        .otherwise("regra desconhecida")
    )
    valid, rejected = T.split_valid_invalid(typed, is_valid, reject_reason)

    # --- 4) Derivacoes de negocio --------------------------------------------
    silver = (
        valid
        .withColumn("age", F.floor(F.months_between(reference_date, F.col("birth_date")) / 12))
        .withColumn("age_band", F.when(F.col("age") < 25, "18-24")
                    .when(F.col("age") < 35, "25-34")
                    .when(F.col("age") < 45, "35-44")
                    .when(F.col("age") < 60, "45-59")
                    .otherwise("60+"))
        .withColumn("tenure_days", F.datediff(reference_date, F.col("signup_date")))
        .withColumn("tenure_band", F.when(F.col("tenure_days") < 90, "0-3m")
                    .when(F.col("tenure_days") < 365, "3-12m")
                    .when(F.col("tenure_days") < 730, "1-2a")
                    .otherwise("2a+"))
        # Safra de aquisicao: essencial para analise de cohort e de churn.
        .withColumn("signup_cohort", F.date_format(F.col("signup_date"), "yyyy-MM"))
        # Quantas vezes o limite cabe na renda: proxy simples de alavancagem.
        .withColumn("limit_to_income_ratio",
                    F.round(F.col("credit_limit") / F.col("monthly_income"), 4))
        .withColumn("income_band", F.when(F.col("monthly_income") < 3000, "ate_3k")
                    .when(F.col("monthly_income") < 8000, "3k_8k")
                    .when(F.col("monthly_income") < 20000, "8k_20k")
                    .otherwise("20k+"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    return silver, rejected


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"silver_{TABLE}")

    with log_stage(log, f"silver.{TABLE}"):
        bronze = spark.read.format("delta").load(cfg.table_path("bronze", TABLE))
        silver, rejected = transform(spark, bronze)

        silver_path = cfg.table_path("silver", TABLE)
        write_delta(silver, silver_path, mode="overwrite", overwrite_schema=True,
                    comment="bronze.customers -> silver.customers")
        register_table(spark, cfg.full_table_name("silver", TABLE), silver_path)
        quarantine(rejected, TABLE)

        result = spark.read.format("delta").load(silver_path)
        dq.run_quality_checks(
            spark, result, dataset=f"silver.{TABLE}",
            expectations=[
                dq.not_null("customer_id"),
                dq.unique("customer_id"),
                dq.not_null("monthly_income"),
                dq.allowed_values("segment", VALID_SEGMENTS),
                dq.allowed_values("status", VALID_STATUS),
                dq.allowed_values("risk_band", ["A", "B", "C", "D", "E"]),
                dq.between("age", 18, 100, severity=dq.SEVERITY_WARN, threshold=0.02),
                dq.between("limit_to_income_ratio", 0.0, 10.0, severity=dq.SEVERITY_WARN,
                           threshold=0.05),
                dq.row_count_min(1),
            ],
        )
        count = result.count()
        log.info("silver.%s: %s clientes", TABLE, count)
        return count


if __name__ == "__main__":
    run()
