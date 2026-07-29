"""
Camada GOLD - Sinais de fraude por transacao (`gold.transaction_fraud_signals`).

Esta tabela transforma o fato transacional em um **objeto de decisao**: para
cada transacao, quais regras dispararam, qual o score consolidado e qual a acao
recomendada (aprovar / revisar / bloquear).

Features de janela (o que um motor antifraude realmente olha)
-------------------------------------------------------------
Calculadas com window functions sobre o historico do proprio cliente, usando
`rangeBetween` em segundos - ou seja, janelas de TEMPO, nao de numero de linhas:

* `txn_count_10min`      - velocity de curtissimo prazo (assinatura de card testing);
* `txn_count_24h` / `amount_sum_24h` - intensidade no dia;
* `seconds_since_prev_txn` - intervalo entre compras;
* `distinct_devices_24h` / `distinct_states_24h` - dispersao suspeita;
* `is_new_device`        - primeiro uso daquele aparelho pelo cliente;
* `amount_zscore_customer` - o quanto o valor foge do padrao historico DO CLIENTE
  (um gasto de R$ 900 e trivial para um cliente e gritante para outro).

Por que manter regras se existe modelo de ML?
---------------------------------------------
Porque as duas coisas resolvem problemas diferentes. A regra e explicavel,
entra em producao em horas, funciona no dia 1 (sem historico rotulado) e serve
de baseline honesto para medir se o modelo agrega valor. O modelo captura
interacoes que nenhuma regra pega. Em producao rodam juntos - e este projeto
mostra os dois: as regras aqui, o modelo em `src/ml/`.

Execucao:
    python -m src.gold.gold_transaction_fraud_signals
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

TABLE = "transaction_fraud_signals"

# Peso de cada regra no score final (0-100). Sao "pesos de politica": o time de
# risco os calibra olhando a taxa de captura x taxa de falso positivo.
RULE_WEIGHTS = {
    "rule_velocity_burst": 22,
    "rule_new_device": 18,
    "rule_high_amount": 15,
    "rule_device_dispersion": 15,
    "rule_amount_outlier_merchant": 12,
    "rule_cnp_high_ticket": 12,
    "rule_income_incompatible": 10,
    "rule_risky_merchant": 10,
    "rule_geo_jump": 10,
    "rule_night_activity": 8,
}


def add_window_features(df: DataFrame) -> DataFrame:
    """Adiciona as features de janela temporal por cliente."""
    # `rangeBetween` opera sobre valores numericos: convertemos o timestamp em
    # epoch para que a janela signifique "ultimos N segundos" de verdade.
    df = df.withColumn("event_epoch", F.col("event_ts").cast("long"))

    by_customer = Window.partitionBy("customer_id").orderBy("event_epoch")
    w_10min = by_customer.rangeBetween(-600, 0)
    w_24h = by_customer.rangeBetween(-86_400, 0)
    w_30d = by_customer.rangeBetween(-2_592_000, -1)  # exclui a propria linha
    w_all_prior = by_customer.rowsBetween(Window.unboundedPreceding, -1)

    return (
        df.withColumn("txn_count_10min", F.count("*").over(w_10min))
        .withColumn("txn_count_24h", F.count("*").over(w_24h))
        .withColumn("amount_sum_24h", F.round(F.sum("amount").over(w_24h), 2))
        .withColumn("distinct_devices_24h", F.size(F.collect_set("device_id").over(w_24h)))
        .withColumn("distinct_states_24h", F.size(F.collect_set("geo_state").over(w_24h)))
        .withColumn("prev_event_epoch", F.lag("event_epoch").over(by_customer))
        .withColumn("prev_geo_state", F.lag("geo_state").over(by_customer))
        .withColumn("seconds_since_prev_txn", F.col("event_epoch") - F.col("prev_event_epoch"))
        # Estatisticas dos ultimos 30 dias do proprio cliente (baseline pessoal).
        .withColumn("amount_avg_30d", F.avg("amount").over(w_30d))
        .withColumn("amount_stddev_30d", F.stddev("amount").over(w_30d))
        .withColumn(
            "amount_zscore_customer",
            F.round(
                (F.col("amount") - F.col("amount_avg_30d"))
                / F.greatest(F.col("amount_stddev_30d"), F.lit(1.0)),
                3,
            ),
        )
        # Dispositivo novo: nunca apareceu antes no historico do cliente.
        .withColumn(
            "known_devices_before",
            F.coalesce(F.collect_set("device_id").over(w_all_prior), F.array()),
        )
        .withColumn(
            "is_new_device",
            (F.size(F.col("known_devices_before")) > 0)
            & (~F.array_contains(F.col("known_devices_before"), F.col("device_id"))),
        )
        .withColumn("customer_txn_seq", F.row_number().over(by_customer))
        .drop("known_devices_before", "prev_event_epoch", "amount_avg_30d", "amount_stddev_30d")
    )


def add_rules(df: DataFrame) -> DataFrame:
    """Aplica as regras heuristicas e consolida o score de fraude."""
    rules = get_settings().business_rules["fraud_rules"]

    flagged = (
        df
        # Rajada de transacoes em poucos minutos (card testing).
        .withColumn(
            "rule_velocity_burst",
            F.col("txn_count_10min") > F.lit(int(rules["velocity_max_transactions"])),
        )
        # Primeiro uso de um dispositivo desconhecido.
        .withColumn("rule_new_device", F.col("is_new_device"))
        # Valor absoluto alto.
        .withColumn("rule_high_amount", F.col("amount") > F.lit(float(rules["high_amount_brl"])))
        # Muitos dispositivos diferentes em 24h.
        .withColumn(
            "rule_device_dispersion",
            F.col("distinct_devices_24h") > F.lit(int(rules["max_distinct_devices_24h"])),
        )
        # Valor muito acima do ticket tipico daquele estabelecimento.
        .withColumn("rule_amount_outlier_merchant", F.col("amount_vs_merchant_avg") > F.lit(5.0))
        # Compra sem presenca do cartao com ticket alto.
        .withColumn(
            "rule_cnp_high_ticket", F.col("is_card_not_present") & (F.col("amount") > F.lit(1500.0))
        )
        # Valor incompativel com a renda declarada.
        .withColumn("rule_income_incompatible", F.col("amount_to_income_ratio") > F.lit(0.5))
        # Estabelecimento com risco alto (MCC sensivel ou score da adquirente).
        .withColumn("rule_risky_merchant", F.col("merchant_risk_tier") == F.lit("alto"))
        # Salto geografico: mudou de estado em menos de 1 hora.
        .withColumn(
            "rule_geo_jump",
            (F.col("prev_geo_state").isNotNull())
            & (F.col("prev_geo_state") != F.col("geo_state"))
            & (F.col("seconds_since_prev_txn") < F.lit(3600)),
        )
        # Transacao na madrugada.
        .withColumn(
            "rule_night_activity",
            F.col("event_hour").between(
                int(rules["night_hour_start"]), int(rules["night_hour_end"])
            ),
        )
    )

    # Score = soma dos pesos das regras disparadas, limitado a 100.
    score = F.lit(0)
    for rule, weight in RULE_WEIGHTS.items():
        score = score + F.when(F.col(rule), F.lit(weight)).otherwise(F.lit(0))

    return (
        flagged.withColumn("fraud_score_rule", F.least(score, F.lit(100)).cast("int"))
        .withColumn(
            "triggered_rules",
            F.array_remove(
                F.array(
                    *[
                        F.when(F.col(rule), F.lit(rule)).otherwise(F.lit(None))
                        for rule in RULE_WEIGHTS
                    ]
                ),
                None,
            ),
        )
        .withColumn("triggered_rules_count", F.size(F.col("triggered_rules")))
        .withColumn(
            "risk_level",
            F.when(F.col("fraud_score_rule") >= 70, "critico")
            .when(F.col("fraud_score_rule") >= 45, "alto")
            .when(F.col("fraud_score_rule") >= 20, "medio")
            .otherwise("baixo"),
        )
        # A acao recomendada e o que o motor de decisao consome em tempo real.
        .withColumn(
            "recommended_action",
            F.when(F.col("fraud_score_rule") >= 70, "bloquear")
            .when(F.col("fraud_score_rule") >= 45, "revisar_manualmente")
            .when(F.col("fraud_score_rule") >= 20, "autenticar_2fa")
            .otherwise("aprovar"),
        )
    )


def build(spark: SparkSession) -> DataFrame:
    cfg = get_settings()
    transactions = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))

    featured = add_window_features(transactions)
    scored = add_rules(featured)

    return scored.select(
        "transaction_id",
        "event_ts",
        "event_date",
        "event_hour",
        "customer_id",
        "customer_key",
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "merchant_mcc_group",
        "merchant_risk_tier",
        "merchant_risk_score",
        "mcc_intrinsic_risk",
        "customer_segment",
        "customer_risk_band",
        "amount",
        "installments",
        "channel",
        "payment_method",
        "status",
        "device_id",
        "geo_city",
        "geo_state",
        "is_card_not_present",
        "is_out_of_home_state",
        "is_night",
        "is_weekend",
        "amount_vs_merchant_avg",
        "amount_to_income_ratio",
        # features de janela
        "txn_count_10min",
        "txn_count_24h",
        "amount_sum_24h",
        "distinct_devices_24h",
        "distinct_states_24h",
        "seconds_since_prev_txn",
        "amount_zscore_customer",
        "is_new_device",
        "customer_txn_seq",
        # regras + score
        *RULE_WEIGHTS.keys(),
        "triggered_rules",
        "triggered_rules_count",
        "fraud_score_rule",
        "risk_level",
        "recommended_action",
        # rotulo (para avaliacao offline e treino do modelo)
        "is_fraud",
        "fraud_type",
        "chargeback_loss",
    ).withColumn("_gold_processed_at", F.current_timestamp())


def evaluate_rules(df: DataFrame) -> dict:
    """Mede o desempenho do motor de regras contra o rotulo real.

    Um baseline sem metrica e so opiniao. Aqui calculamos precisao, recall e a
    taxa de revisao manual - o trade-off que a operacao antifraude negocia todo
    trimestre (capturar mais fraude custa mais analista).
    """
    flagged = F.col("fraud_score_rule") >= 45  # limiar operacional
    metrics = df.agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_fraud")).alias("fraud_total"),
        F.sum(F.when(flagged, 1).otherwise(0)).alias("flagged"),
        F.sum(F.when(flagged & (F.col("is_fraud") == 1), 1).otherwise(0)).alias("true_positive"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0.0)), 2).alias(
            "fraud_amount"
        ),
        F.round(
            F.sum(F.when(flagged & (F.col("is_fraud") == 1), F.col("amount")).otherwise(0.0)), 2
        ).alias("fraud_amount_caught"),
    ).collect()[0]

    flagged_n = metrics["flagged"] or 0
    fraud_n = metrics["fraud_total"] or 0
    tp = metrics["true_positive"] or 0
    return {
        "transacoes": metrics["total"],
        "fraudes_reais": fraud_n,
        "transacoes_sinalizadas": flagged_n,
        "precisao": round(tp / flagged_n, 4) if flagged_n else 0.0,
        "recall": round(tp / fraud_n, 4) if fraud_n else 0.0,
        "taxa_de_revisao": round(flagged_n / metrics["total"], 4) if metrics["total"] else 0.0,
        "valor_em_fraude": metrics["fraud_amount"],
        "valor_capturado": metrics["fraud_amount_caught"],
    }


def run() -> int:
    cfg = get_settings()
    spark = get_spark(f"gold_{TABLE}")

    with log_stage(log, f"gold.{TABLE}"):
        gold = build(spark)
        path = cfg.table_path("gold", TABLE)

        write_delta(
            gold.repartition("event_date"),
            path,
            mode="overwrite",
            partition_by=["event_date"],
            overwrite_schema=True,
            comment="silver.transactions -> gold.transaction_fraud_signals",
        )
        register_table(spark, cfg.full_table_name("gold", TABLE), path)

        result = spark.read.format("delta").load(path)
        dq.run_quality_checks(
            spark,
            result,
            dataset=f"gold.{TABLE}",
            expectations=[
                dq.not_null("transaction_id"),
                dq.unique("transaction_id"),
                dq.between("fraud_score_rule", 0, 100),
                dq.allowed_values("risk_level", ["baixo", "medio", "alto", "critico"]),
                dq.allowed_values(
                    "recommended_action",
                    ["aprovar", "autenticar_2fa", "revisar_manualmente", "bloquear"],
                ),
                dq.row_count_min(1),
            ],
        )

        log.info("Desempenho do motor de regras (limiar 45): %s", evaluate_rules(result))
        count = result.count()
        log.info("gold.%s: %s transacoes pontuadas", TABLE, count)
        return count


if __name__ == "__main__":
    run()
