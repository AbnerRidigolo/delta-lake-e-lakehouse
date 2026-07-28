"""
Camada SILVER - transacoes (a tabela mais importante do lakehouse).

Esta e a tabela onde o dado bruto vira fato analitico. Ela concentra:

1. **Deduplicacao** por `transaction_id` - a Bronze e append-only e recebe o
   mesmo evento pelo batch e pelo streaming; aqui garantimos exatamente uma
   linha por transacao.
2. **Tipagem segura** de valor, parcelas, flag de fraude e timestamp.
3. **Integridade referencial**: transacao apontando para cliente inexistente
   (chave orfa) nao entra na Silver - vai para a quarentena com o motivo.
4. **Enriquecimento** com atributos de cliente (segmento, faixa de risco,
   cidade de residencia) e de estabelecimento (categoria, MCC, MDR, risco).
5. **Derivacoes temporais e financeiras** que alimentam Gold e feature store:
   data/hora do evento, madrugada, fim de semana, valor da parcela, receita de
   MDR, distancia comportamental (compra fora do estado de residencia).

Particionamento: por `event_date`. Aqui - e nao na Bronze - porque o timestamp
ja foi validado. Particionar por uma coluna potencialmente corrompida criaria
particoes lixo que ninguem consegue apagar depois.

Execucao:
    python -m src.silver.silver_transactions
    python -m src.silver.silver_transactions --reprocess-date 2024-07-15
"""

from __future__ import annotations

import argparse
from typing import Optional

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

TABLE = "transactions"
VALID_CHANNELS = ["pos", "ecommerce", "pix", "wallet", "boleto", "ted", "atm"]
VALID_METHODS = ["credit_card", "debit_card", "pix", "boleto", "ted"]
VALID_STATUS = ["approved", "declined", "reversed", "chargeback"]
# Canais sem presenca fisica do cartao concentram a maior parte da fraude.
CARD_NOT_PRESENT_CHANNELS = ["ecommerce", "wallet"]


def transform(spark: SparkSession, bronze: DataFrame, customers: DataFrame,
              merchants: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Limpa, valida e enriquece as transacoes. Retorna (silver, rejeitados)."""

    # --- 1) Deduplicacao ------------------------------------------------------
    deduped = T.deduplicate(
        bronze, keys=["transaction_id"],
        order_by=[F.col("_ingested_at").desc(), F.col("source_extracted_at").desc_nulls_last()],
    )

    # --- 2) Tipagem + normalizacao -------------------------------------------
    typed = deduped.select(
        F.trim(F.col("transaction_id")).alias("transaction_id"),
        T.safe_timestamp("event_ts").alias("event_ts"),
        F.trim(F.col("customer_id")).alias("customer_id"),
        F.trim(F.col("merchant_id")).alias("merchant_id"),
        F.lpad(F.trim(F.col("mcc")), 4, "0").alias("mcc"),
        T.safe_double("amount").alias("amount"),
        F.upper(F.trim(F.col("currency"))).alias("currency"),
        T.normalize_text("channel").alias("channel"),
        T.normalize_text("payment_method").alias("payment_method"),
        F.coalesce(T.safe_int("installments"), F.lit(1)).alias("installments"),
        F.trim(F.col("device_id")).alias("device_id"),
        F.trim(F.col("ip_address")).alias("ip_address"),
        F.initcap(F.trim(F.col("geo_city"))).alias("geo_city"),
        F.upper(F.trim(F.col("geo_state"))).alias("geo_state"),
        T.normalize_text("status").alias("status"),
        F.coalesce(T.safe_int("is_fraud"), F.lit(0)).alias("is_fraud"),
        F.when(F.trim(F.col("fraud_type")) == "", None)
         .otherwise(T.normalize_text("fraud_type")).alias("fraud_type"),
        F.col("_source_system"),
        F.col("_ingested_at"),
    )

    # --- 3) Validacao tecnica -------------------------------------------------
    is_valid = (
        F.col("transaction_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("channel").isin(VALID_CHANNELS)
        & F.col("payment_method").isin(VALID_METHODS)
        & F.col("status").isin(VALID_STATUS)
        & F.col("customer_id").isNotNull()
    )
    reject_reason = (
        F.when(F.col("transaction_id").isNull(), "transaction_id ausente")
        .when(F.col("event_ts").isNull(), "timestamp invalido")
        .when(F.col("amount").isNull(), "valor nao numerico")
        .when(F.col("amount") <= 0, "valor nao positivo")
        .when(~F.col("channel").isin(VALID_CHANNELS), "canal fora do dominio")
        .when(~F.col("payment_method").isin(VALID_METHODS), "forma de pagamento fora do dominio")
        .when(~F.col("status").isin(VALID_STATUS), "status fora do dominio")
        .when(F.col("customer_id").isNull(), "customer_id ausente")
        .otherwise("regra desconhecida")
    )
    valid, rejected_technical = T.split_valid_invalid(typed, is_valid, reject_reason)

    # --- 4) Enriquecimento + integridade referencial --------------------------
    # As dimensoes sao pequenas: broadcast join evita shuffle da tabela de fatos.
    customer_dim = customers.select(
        F.col("customer_id"),
        F.col("customer_key"),
        F.col("segment").alias("customer_segment"),
        F.col("risk_band").alias("customer_risk_band"),
        F.col("city").alias("customer_city"),
        F.col("state").alias("customer_state"),
        F.col("monthly_income").alias("customer_income"),
        F.col("credit_limit").alias("customer_credit_limit"),
        F.col("age_band").alias("customer_age_band"),
        F.col("tenure_days").alias("customer_tenure_days"),
    )
    merchant_dim = merchants.select(
        F.col("merchant_id"),
        F.col("merchant_name"),
        F.col("category").alias("merchant_category"),
        F.col("mcc_group").alias("merchant_mcc_group"),
        F.col("state").alias("merchant_state"),
        F.col("risk_tier").alias("merchant_risk_tier"),
        F.col("risk_score").alias("merchant_risk_score"),
        F.col("mcc_intrinsic_risk"),
        F.col("mdr_rate"),
        F.col("avg_ticket").alias("merchant_avg_ticket"),
        F.col("is_new_merchant"),
    )

    enriched = (
        valid.join(F.broadcast(customer_dim), on="customer_id", how="left")
             .join(F.broadcast(merchant_dim), on="merchant_id", how="left")
    )

    # Chave orfa: a transacao existe mas o cliente nao esta no cadastro.
    # Em uma fintech isso e incidente de integracao, nao "dado ruim aceitavel".
    has_references = F.col("customer_key").isNotNull() & F.col("mdr_rate").isNotNull()
    orphan_reason = (
        F.when(F.col("customer_key").isNull(), "cliente inexistente no cadastro")
        .otherwise("estabelecimento inexistente no cadastro")
    )
    referenced, rejected_orphan = T.split_valid_invalid(enriched, has_references, orphan_reason)

    rejected = rejected_technical.select(
        "transaction_id", "customer_id", "merchant_id", "amount", "channel",
        "status", "_reject_reason", "_source_system",
    ).unionByName(
        rejected_orphan.select(
            "transaction_id", "customer_id", "merchant_id", "amount", "channel",
            "status", "_reject_reason", "_source_system",
        )
    )

    # --- 5) Derivacoes de negocio ---------------------------------------------
    silver = (
        referenced
        # -- tempo
        .withColumn("event_date", F.to_date(F.col("event_ts")))
        .withColumn("event_hour", F.hour(F.col("event_ts")))
        .withColumn("event_dow", F.dayofweek(F.col("event_ts")))
        .withColumn("event_month", F.date_format(F.col("event_ts"), "yyyy-MM"))
        .withColumn("is_weekend", F.col("event_dow").isin(1, 7))
        .withColumn("is_night", F.col("event_hour").between(0, 5))
        # -- financeiro
        .withColumn("is_approved", F.col("status") == "approved")
        .withColumn("installment_amount",
                    F.round(F.col("amount") / F.greatest(F.col("installments"), F.lit(1)), 2))
        # Receita de adquirencia: MDR aplicado apenas sobre transacao aprovada.
        .withColumn("mdr_revenue",
                    F.when(F.col("status") == "approved",
                           F.round(F.col("amount") * F.col("mdr_rate"), 4)).otherwise(F.lit(0.0)))
        # Perda bruta: chargeback devolve o valor ao portador.
        .withColumn("chargeback_loss",
                    F.when(F.col("status") == "chargeback", F.col("amount")).otherwise(F.lit(0.0)))
        .withColumn("amount_band",
                    F.when(F.col("amount") < 50, "ate_50")
                    .when(F.col("amount") < 200, "50_200")
                    .when(F.col("amount") < 1000, "200_1k")
                    .when(F.col("amount") < 5000, "1k_5k")
                    .otherwise("5k+"))
        # -- comportamento / risco
        .withColumn("is_card_not_present", F.col("channel").isin(CARD_NOT_PRESENT_CHANNELS))
        .withColumn("is_out_of_home_state",
                    F.col("geo_state").isNotNull()
                    & (F.col("geo_state") != F.col("customer_state")))
        # Quantas vezes a compra excede o ticket medio do lojista: outlier local.
        .withColumn("amount_vs_merchant_avg",
                    F.round(F.col("amount") / F.greatest(F.col("merchant_avg_ticket"), F.lit(1.0)), 3))
        # Quanto a compra representa da renda mensal do cliente.
        .withColumn("amount_to_income_ratio",
                    F.round(F.col("amount") / F.greatest(F.col("customer_income"), F.lit(1.0)), 5))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    return silver, rejected


def run(reprocess_date: Optional[str] = None) -> int:
    """Executa a carga Silver de transacoes.

    Args:
        reprocess_date: se informado (YYYY-MM-DD), reprocessa **apenas** aquele
            dia usando `replaceWhere`. E a operacao que separa um pipeline
            profissional de um script: corrigir um dia sem reescrever o ano.
    """
    cfg = get_settings()
    spark = get_spark(f"silver_{TABLE}")

    with log_stage(log, f"silver.{TABLE}"):
        bronze = spark.read.format("delta").load(cfg.table_path("bronze", TABLE))
        customers = spark.read.format("delta").load(cfg.table_path("silver", "customers"))
        merchants = spark.read.format("delta").load(cfg.table_path("silver", "merchants"))

        silver, rejected = transform(spark, bronze, customers, merchants)

        silver_path = cfg.table_path("silver", TABLE)

        if reprocess_date:
            silver = silver.where(F.col("event_date") == F.lit(reprocess_date))
            write_delta(
                silver, silver_path, mode="overwrite", partition_by=["event_date"],
                replace_where=f"event_date = '{reprocess_date}'", merge_schema=True,
                comment=f"reprocessamento pontual de {reprocess_date}",
            )
            log.info("Reprocessamento seletivo concluido para %s", reprocess_date)
        else:
            # `repartition` pela coluna de particao evita centenas de arquivos
            # minusculos (o classico "small files problem" do data lake).
            write_delta(
                silver.repartition("event_date"), silver_path, mode="overwrite",
                partition_by=["event_date"], overwrite_schema=True,
                comment="bronze.transactions -> silver.transactions",
            )

        register_table(spark, cfg.full_table_name("silver", TABLE), silver_path)
        quarantine(rejected, TABLE)

        result = spark.read.format("delta").load(silver_path)
        dq.run_quality_checks(
            spark, result, dataset=f"silver.{TABLE}",
            expectations=[
                dq.not_null("transaction_id"),
                dq.unique("transaction_id"),
                dq.not_null("event_ts"),
                dq.between("amount", 0.01, 1_000_000.0),
                dq.allowed_values("channel", VALID_CHANNELS),
                dq.allowed_values("status", VALID_STATUS),
                dq.allowed_values("currency", ["BRL"]),
                dq.not_null("customer_key"),
                dq.between("installments", 1, 48),
                dq.row_count_min(1),
            ],
        )
        count = result.count()
        log.info("silver.%s: %s transacoes | %s particoes de data", TABLE, count,
                 result.select("event_date").distinct().count())
        return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga silver.transactions.")
    parser.add_argument("--reprocess-date", default=None,
                        help="reprocessa apenas a data informada (YYYY-MM-DD) via replaceWhere")
    args = parser.parse_args()
    run(args.reprocess_date)


if __name__ == "__main__":
    main()
