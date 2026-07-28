"""
Feature Store simples sobre Delta Lake.

O que uma feature store precisa resolver (e como resolvemos aqui)
-----------------------------------------------------------------
1. **Reuso**: a mesma feature usada por varios modelos, definida uma unica vez.
   -> Cada feature view e uma tabela Delta versionada, com registro de metadados
      (`feature_store._registry`): dono, descricao, fonte e frescor.

2. **Point-in-time correctness** (o erro que mais mata modelo em producao):
   treinar com informacao que so existiria DEPOIS do evento infla a metrica
   offline e o modelo desaba em producao.
   -> As features de cliente sao **snapshots mensais acumulados**: a linha
      `(cliente, 2024-06)` so enxerga transacoes ate 30/06. Uma transacao de
      julho e enriquecida com o snapshot de **junho** - nunca com o do proprio
      mes. O join e por chave, deterministico e auditavel.

3. **Paridade treino/producao**: o mesmo codigo que monta o dataset de treino
   monta as features de inferencia.
   -> `build_training_set()` e usado nas duas situacoes; muda so o recorte.

Tabelas produzidas
------------------
    feature_store.customer_features     - snapshot mensal por cliente
    feature_store.transaction_features  - features por transacao (ja point-in-time)
    feature_store._registry             - catalogo das features

Execucao:
    python -m src.ml.feature_store
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import data_quality as dq
from src.common.config import get_settings
from src.common.delta_io import register_table, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

CUSTOMER_FEATURES = "customer_features"
TRANSACTION_FEATURES = "transaction_features"
REGISTRY = "_registry"


@dataclass
class FeatureViewMeta:
    """Metadados de uma feature view - o "contrato" da feature com quem consome."""

    name: str
    entity: str
    primary_keys: str
    timestamp_key: str
    description: str
    source_tables: str
    owner: str
    refresh: str
    point_in_time: bool


FEATURE_VIEWS: List[FeatureViewMeta] = [
    FeatureViewMeta(
        name="feature_store.customer_features",
        entity="customer",
        primary_keys="customer_id, feature_month",
        timestamp_key="feature_month",
        description=("Snapshot mensal acumulado do comportamento do cliente: volume, "
                     "ticket, mix de canais, diversidade de lojistas/dispositivos e "
                     "historico de contestacao."),
        source_tables="silver.transactions, silver.customers",
        owner="squad-ml-platform",
        refresh="mensal (D+1 do fechamento)",
        point_in_time=True,
    ),
    FeatureViewMeta(
        name="feature_store.transaction_features",
        entity="transaction",
        primary_keys="transaction_id",
        timestamp_key="event_ts",
        description=("Features por transacao para o modelo de fraude: valor, canal, "
                     "velocity, dispositivo novo, desvio do padrao do cliente e sinais "
                     "do estabelecimento."),
        source_tables="gold.transaction_fraud_signals",
        owner="squad-ml-platform",
        refresh="continuo (segue o streaming)",
        point_in_time=True,
    ),
]


# ===========================================================================
# Feature view 1: snapshot mensal do cliente
# ===========================================================================

def build_customer_features(spark: SparkSession) -> DataFrame:
    """Constroi o snapshot mensal acumulado por cliente.

    Estrategia: agregamos por (cliente, mes) e depois acumulamos com uma janela
    `unboundedPreceding -> currentRow` ordenada pelo mes. O resultado e "tudo o
    que a fintech sabia sobre o cliente ate o fim daquele mes" - exatamente o
    que estaria disponivel para uma decisao tomada no mes seguinte.
    """
    cfg = get_settings()
    transactions = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))
    customers = spark.read.format("delta").load(cfg.table_path("silver", "customers"))

    approved = F.col("status") == "approved"

    monthly = transactions.groupBy("customer_id", "event_month").agg(
        F.count("*").alias("m_txn_count"),
        F.sum(F.when(approved, F.col("amount")).otherwise(0.0)).alias("m_tpv"),
        F.avg(F.when(approved, F.col("amount"))).alias("m_avg_ticket"),
        F.max("amount").alias("m_max_ticket"),
        F.sum(F.when(F.col("status") == "chargeback", 1).otherwise(0)).alias("m_chargebacks"),
        F.sum(F.when(F.col("status") == "declined", 1).otherwise(0)).alias("m_declines"),
        F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("m_frauds"),
        F.countDistinct("merchant_id").alias("m_distinct_merchants"),
        F.countDistinct("device_id").alias("m_distinct_devices"),
        F.avg(F.when(F.col("is_card_not_present"), 1.0).otherwise(0.0)).alias("m_cnp_share"),
        F.avg(F.when(F.col("is_night"), 1.0).otherwise(0.0)).alias("m_night_share"),
        F.avg(F.when(F.col("channel") == "pix", 1.0).otherwise(0.0)).alias("m_pix_share"),
    )

    # Janela acumulada: do primeiro mes do cliente ate o mes corrente.
    cumulative = Window.partitionBy("customer_id").orderBy("feature_month") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    last_3m = Window.partitionBy("customer_id").orderBy("feature_month").rowsBetween(-2, 0)

    features = (
        monthly
        .withColumnRenamed("event_month", "feature_month")
        # --- acumulado historico (lifetime ate o mes) ------------------------
        .withColumn("cf_txn_count_lifetime", F.sum("m_txn_count").over(cumulative))
        .withColumn("cf_tpv_lifetime", F.round(F.sum("m_tpv").over(cumulative), 2))
        .withColumn("cf_chargebacks_lifetime", F.sum("m_chargebacks").over(cumulative))
        .withColumn("cf_frauds_lifetime", F.sum("m_frauds").over(cumulative))
        .withColumn("cf_declines_lifetime", F.sum("m_declines").over(cumulative))
        .withColumn("cf_active_months", F.count("*").over(cumulative))
        # --- janela curta (3 meses): captura mudanca de comportamento -------
        .withColumn("cf_txn_count_3m", F.sum("m_txn_count").over(last_3m))
        .withColumn("cf_tpv_3m", F.round(F.sum("m_tpv").over(last_3m), 2))
        .withColumn("cf_avg_ticket_3m", F.round(F.avg("m_avg_ticket").over(last_3m), 2))
        .withColumn("cf_max_ticket_3m", F.round(F.max("m_max_ticket").over(last_3m), 2))
        .withColumn("cf_distinct_merchants_3m", F.round(F.avg("m_distinct_merchants").over(last_3m), 2))
        .withColumn("cf_distinct_devices_3m", F.round(F.avg("m_distinct_devices").over(last_3m), 2))
        .withColumn("cf_cnp_share_3m", F.round(F.avg("m_cnp_share").over(last_3m), 4))
        .withColumn("cf_night_share_3m", F.round(F.avg("m_night_share").over(last_3m), 4))
        .withColumn("cf_pix_share_3m", F.round(F.avg("m_pix_share").over(last_3m), 4))
        # --- taxas derivadas -------------------------------------------------
        .withColumn("cf_chargeback_rate_lifetime",
                    F.round(F.col("cf_chargebacks_lifetime")
                            / F.greatest(F.col("cf_txn_count_lifetime"), F.lit(1)), 5))
        .withColumn("cf_decline_rate_lifetime",
                    F.round(F.col("cf_declines_lifetime")
                            / F.greatest(F.col("cf_txn_count_lifetime"), F.lit(1)), 5))
        .withColumn("cf_avg_ticket_lifetime",
                    F.round(F.col("cf_tpv_lifetime")
                            / F.greatest(F.col("cf_txn_count_lifetime"), F.lit(1)), 2))
    )

    # Atributos cadastrais (mudam pouco) entram como features estaveis.
    profile = customers.select(
        "customer_id",
        F.col("segment").alias("cf_segment"),
        F.col("risk_band").alias("cf_risk_band"),
        F.col("state").alias("cf_state"),
        F.col("age").alias("cf_age"),
        F.col("monthly_income").alias("cf_monthly_income"),
        F.col("credit_limit").alias("cf_credit_limit"),
        F.col("limit_to_income_ratio").alias("cf_limit_to_income"),
        F.col("tenure_days").alias("cf_tenure_days"),
        F.col("acquisition_channel").alias("cf_acquisition_channel"),
    )

    return (
        features.join(F.broadcast(profile), on="customer_id", how="inner")
        .withColumn("_feature_generated_at", F.current_timestamp())
        .drop("m_txn_count", "m_tpv", "m_avg_ticket", "m_max_ticket", "m_chargebacks",
              "m_declines", "m_frauds", "m_distinct_merchants", "m_distinct_devices",
              "m_cnp_share", "m_night_share", "m_pix_share")
    )


# ===========================================================================
# Feature view 2: features por transacao
# ===========================================================================

def build_transaction_features(spark: SparkSession) -> DataFrame:
    """Seleciona as features transacionais a partir dos sinais da Gold.

    Todas as colunas aqui usam apenas informacao disponivel **no instante da
    transacao** (janelas olhando para tras). O rotulo `is_fraud` viaja junto
    porque este e o offline store, usado para treino e avaliacao - na inferencia
    online o rotulo simplesmente nao existe ainda.
    """
    cfg = get_settings()
    signals = spark.read.format("delta").load(cfg.table_path("gold", "transaction_fraud_signals"))

    return signals.select(
        # chaves
        "transaction_id", "customer_id", "merchant_id", "event_ts", "event_date",
        F.date_format(F.add_months(F.col("event_date"), -1), "yyyy-MM").alias("feature_month"),
        # --- features numericas -------------------------------------------
        F.col("amount").alias("tf_amount"),
        F.col("installments").alias("tf_installments"),
        F.col("event_hour").alias("tf_event_hour"),
        F.col("txn_count_10min").alias("tf_txn_count_10min"),
        F.col("txn_count_24h").alias("tf_txn_count_24h"),
        F.col("amount_sum_24h").alias("tf_amount_sum_24h"),
        F.col("distinct_devices_24h").alias("tf_distinct_devices_24h"),
        F.col("distinct_states_24h").alias("tf_distinct_states_24h"),
        F.coalesce(F.col("seconds_since_prev_txn"), F.lit(-1)).alias("tf_seconds_since_prev"),
        F.coalesce(F.col("amount_zscore_customer"), F.lit(0.0)).alias("tf_amount_zscore"),
        F.col("amount_vs_merchant_avg").alias("tf_amount_vs_merchant_avg"),
        F.col("amount_to_income_ratio").alias("tf_amount_to_income"),
        F.col("customer_txn_seq").alias("tf_customer_txn_seq"),
        F.col("merchant_risk_score").alias("tf_merchant_risk_score"),
        F.col("mcc_intrinsic_risk").alias("tf_mcc_intrinsic_risk"),
        # --- features booleanas -------------------------------------------
        F.col("is_new_device").cast("int").alias("tf_is_new_device"),
        F.col("is_card_not_present").cast("int").alias("tf_is_cnp"),
        F.col("is_out_of_home_state").cast("int").alias("tf_is_out_of_state"),
        F.col("is_night").cast("int").alias("tf_is_night"),
        F.col("is_weekend").cast("int").alias("tf_is_weekend"),
        # --- features categoricas ------------------------------------------
        F.col("channel").alias("tf_channel"),
        F.col("payment_method").alias("tf_payment_method"),
        F.col("merchant_category").alias("tf_merchant_category"),
        F.col("merchant_mcc_group").alias("tf_merchant_mcc_group"),
        F.col("merchant_risk_tier").alias("tf_merchant_risk_tier"),
        # --- baseline por regra (para comparar com o modelo) ----------------
        F.col("fraud_score_rule"),
        F.col("triggered_rules_count"),
        # --- rotulo ---------------------------------------------------------
        F.col("is_fraud"),
        F.col("fraud_type"),
    ).withColumn("_feature_generated_at", F.current_timestamp())


# ===========================================================================
# Montagem do dataset de treino (join point-in-time)
# ===========================================================================

def build_training_set(spark: SparkSession) -> DataFrame:
    """Junta features transacionais com o snapshot do cliente do mes ANTERIOR.

    Este e o ponto critico do pipeline de ML: `feature_month` da transacao ja
    aponta para o mes anterior (calculado em `build_transaction_features`), o
    que garante que nenhuma informacao do proprio mes - muito menos do futuro -
    entra no treino.

    Transacoes do primeiro mes de vida do cliente nao tem snapshot anterior;
    elas permanecem no dataset com as features de cliente nulas (o modelo
    aprende a lidar com "cliente novo", que e justamente um cenario de risco).
    """
    cfg = get_settings()
    transaction_features = spark.read.format("delta").load(
        cfg.table_path("feature_store", TRANSACTION_FEATURES))
    customer_features = spark.read.format("delta").load(
        cfg.table_path("feature_store", CUSTOMER_FEATURES))

    return transaction_features.join(
        customer_features.drop("_feature_generated_at"),
        on=["customer_id", "feature_month"],
        how="left",
    )


def run() -> dict:
    cfg = get_settings()
    spark = get_spark("feature_store")
    counts = {}

    with log_stage(log, f"feature_store.{CUSTOMER_FEATURES}"):
        customer_df = build_customer_features(spark)
        path = cfg.table_path("feature_store", CUSTOMER_FEATURES)
        write_delta(customer_df, path, mode="overwrite", partition_by=["feature_month"],
                    overwrite_schema=True, comment="snapshot mensal de features de cliente")
        register_table(spark, cfg.full_table_name("feature_store", CUSTOMER_FEATURES), path)
        counts[CUSTOMER_FEATURES] = spark.read.format("delta").load(path).count()

    with log_stage(log, f"feature_store.{TRANSACTION_FEATURES}"):
        txn_df = build_transaction_features(spark)
        path = cfg.table_path("feature_store", TRANSACTION_FEATURES)
        write_delta(txn_df.repartition("event_date"), path, mode="overwrite",
                    partition_by=["event_date"], overwrite_schema=True,
                    comment="features transacionais para o modelo de fraude")
        register_table(spark, cfg.full_table_name("feature_store", TRANSACTION_FEATURES), path)
        counts[TRANSACTION_FEATURES] = spark.read.format("delta").load(path).count()

        dq.run_quality_checks(
            spark, spark.read.format("delta").load(path),
            dataset=f"feature_store.{TRANSACTION_FEATURES}",
            expectations=[
                dq.not_null("transaction_id"),
                dq.unique("transaction_id"),
                dq.not_null("tf_amount"),
                dq.allowed_values("is_fraud", [0, 1]),
                dq.row_count_min(1),
            ],
        )

    # Registro de metadados das feature views (catalogo da feature store).
    with log_stage(log, f"feature_store.{REGISTRY}"):
        registry_rows = [
            {**asdict(view), "_registered_at": datetime.now(timezone.utc).replace(tzinfo=None)}
            for view in FEATURE_VIEWS
        ]
        registry_df = spark.createDataFrame(registry_rows)
        path = cfg.table_path("feature_store", REGISTRY)
        write_delta(registry_df, path, mode="overwrite", overwrite_schema=True,
                    comment="catalogo das feature views")

    # Sanidade do join point-in-time: quantas transacoes ficaram sem snapshot.
    training = build_training_set(spark)
    total = training.count()
    without_profile = training.where(F.col("cf_txn_count_lifetime").isNull()).count()
    log.info("Training set: %s linhas | %s (%.1f%%) sem snapshot anterior (clientes novos)",
             total, without_profile, 100 * without_profile / max(total, 1))
    counts["training_set"] = total
    return counts


if __name__ == "__main__":
    run()
