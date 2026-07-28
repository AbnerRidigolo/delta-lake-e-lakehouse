# Databricks notebook source
# MAGIC %md
# MAGIC # NeoPag Lakehouse — exploracao interativa
# MAGIC
# MAGIC Notebook de exploracao das camadas do lakehouse. Funciona em duas situacoes:
# MAGIC
# MAGIC * **Databricks** (Community Edition ou workspace): importe este arquivo como
# MAGIC   notebook — as celulas `# COMMAND ----------` sao reconhecidas automaticamente.
# MAGIC * **Local**: rode com `jupytext --to notebook` ou simplesmente execute o
# MAGIC   arquivo como script (`python notebooks/databricks_lakehouse_demo.py`).
# MAGIC
# MAGIC **Pre-requisito:** o pipeline precisa ter sido executado (`make pipeline`).
# MAGIC
# MAGIC ## Configuracao no Databricks
# MAGIC
# MAGIC 1. Cluster com Runtime 14.x ou superior (ja traz Spark e Delta).
# MAGIC 2. Faca upload do repositorio via Repos, ou clone com `%sh git clone ...`.
# MAGIC 3. Em `config/settings.yaml`:
# MAGIC    - `project.environment: databricks` (habilita o namespace de 3 niveis do Unity Catalog)
# MAGIC    - `paths.root: /dbfs/FileStore/neopag` (ou defina a variavel `LAKEHOUSE_ROOT`)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

import sys
from pathlib import Path

# Garante que a raiz do projeto esteja no PYTHONPATH (necessario no Databricks).
PROJECT_ROOT = Path.cwd()
while not (PROJECT_ROOT / "config" / "settings.yaml").exists() and PROJECT_ROOT.parent != PROJECT_ROOT:
    PROJECT_ROOT = PROJECT_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F  # noqa: E402

from src.common.config import get_settings  # noqa: E402
from src.common.delta_io import describe_history, read_delta  # noqa: E402
from src.common.spark_session import get_spark  # noqa: E402

cfg = get_settings()
spark = get_spark("notebook_exploracao")

print(f"Projeto:  {cfg.project['name']}")
print(f"Ambiente: {cfg.project['environment']}")
print(f"Spark:    {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. O que existe no lakehouse

# COMMAND ----------

from src.common.delta_io import table_exists

inventario = []
for camada, tabelas in {
    "bronze": ["customers", "merchants", "transactions", "credit_contracts"],
    "silver": ["customers", "merchants", "transactions", "credit_contracts"],
    "gold": ["customer_360", "transaction_fraud_signals", "credit_risk_portfolio",
             "financial_kpis_daily", "merchant_performance"],
    "feature_store": ["customer_features", "transaction_features", "fraud_model_scores"],
}.items():
    for tabela in tabelas:
        caminho = cfg.table_path(camada, tabela)
        if table_exists(spark, caminho):
            detalhe = spark.sql(f"DESCRIBE DETAIL delta.`{caminho}`").collect()[0]
            inventario.append({
                "tabela": f"{camada}.{tabela}",
                "linhas": spark.read.format("delta").load(caminho).count(),
                "arquivos": detalhe["numFiles"],
                "tamanho_mb": round((detalhe["sizeInBytes"] or 0) / 1_048_576, 2),
                "particoes": ", ".join(detalhe["partitionColumns"] or []) or "-",
            })

spark.createDataFrame(inventario).show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camada Bronze — a origem preservada
# MAGIC
# MAGIC Repare que **todas as colunas sao STRING** e que existem valores invalidos
# MAGIC (`""`, `"null"`, valores negativos, datas impossiveis). Isso e proposital: a
# MAGIC Bronze preserva o dado como ele chegou. A limpeza acontece na Silver, onde
# MAGIC cada rejeicao e contabilizada.

# COMMAND ----------

bronze = spark.read.format("delta").load(cfg.table_path("bronze", "transactions"))
bronze.printSchema()

print("Exemplos de registros problematicos que a Bronze preservou:")
bronze.where(
    (F.col("amount").isNull()) | (F.col("amount") == "") | (F.col("amount").startswith("-"))
    | (F.col("event_ts") == "") | (F.col("event_ts").startswith("0000"))
).select("transaction_id", "event_ts", "amount", "channel", "_source_system").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch e streaming na mesma tabela
# MAGIC
# MAGIC O ACID do Delta permite que dois processos escrevam na mesma tabela sem
# MAGIC corromper nada. Quem le nao precisa saber por qual caminho o dado chegou.

# COMMAND ----------

bronze.groupBy("_source_system").agg(
    F.count("*").alias("linhas"),
    F.min("_ingested_at").alias("primeira_ingestao"),
    F.max("_ingested_at").alias("ultima_ingestao"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camada Silver — o que foi rejeitado e por que

# COMMAND ----------

quarentena = str(cfg.layer_path("silver") / "_quarantine" / "transactions")
if table_exists(spark, quarentena):
    spark.read.format("delta").load(quarentena) \
        .groupBy("_reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
else:
    print("Nenhum registro em quarentena nesta execucao.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Camada Gold — perguntas de negocio em SQL
# MAGIC
# MAGIC As tabelas estao registradas no metastore, entao da para consultar com SQL puro.

# COMMAND ----------

silver = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))
silver.createOrReplaceTempView("transacoes")

for nome in ["customer_360", "transaction_fraud_signals", "credit_risk_portfolio",
             "financial_kpis_daily", "merchant_performance"]:
    spark.read.format("delta").load(cfg.table_path("gold", nome)) \
        .createOrReplaceTempView(nome)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Onde esta o valor da base de clientes?

# COMMAND ----------

spark.sql("""
    SELECT value_risk_segment                       AS segmento,
           count(*)                                 AS clientes,
           round(avg(customer_score))               AS score_medio,
           round(sum(tpv_90d), 2)                   AS tpv_90d,
           round(sum(total_exposure), 2)            AS exposicao_credito,
           round(avg(limit_utilization) * 100, 1)   AS uso_limite_pct
    FROM customer_360
    GROUP BY value_risk_segment
    ORDER BY tpv_90d DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 O motor de regras esta funcionando?
# MAGIC
# MAGIC Precisao alta com recall baixo e o comportamento tipico de um motor de regras:
# MAGIC ele acerta quando aponta, mas deixa muita fraude passar. E exatamente o espaco
# MAGIC que o modelo de ML precisa preencher.

# COMMAND ----------

spark.sql("""
    SELECT risk_level                                          AS nivel_de_risco,
           recommended_action                                  AS acao,
           count(*)                                            AS transacoes,
           sum(is_fraud)                                       AS fraudes_reais,
           round(100.0 * sum(is_fraud) / count(*), 2)          AS pct_fraude,
           round(sum(amount), 2)                               AS volume
    FROM transaction_fraud_signals
    GROUP BY risk_level, recommended_action
    ORDER BY pct_fraude DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Quais regras individualmente mais acertam?

# COMMAND ----------

from src.gold.gold_transaction_fraud_signals import RULE_WEIGHTS

resultados = []
sinais = spark.table("transaction_fraud_signals")
total_fraudes = sinais.agg(F.sum("is_fraud")).collect()[0][0] or 1

for regra, peso in RULE_WEIGHTS.items():
    linha = sinais.where(F.col(regra)).agg(
        F.count("*").alias("disparos"), F.sum("is_fraud").alias("fraudes")).collect()[0]
    disparos = linha["disparos"] or 0
    fraudes = linha["fraudes"] or 0
    resultados.append({
        "regra": regra,
        "peso": peso,
        "disparos": disparos,
        "fraudes_capturadas": fraudes,
        "precisao": round(fraudes / disparos, 4) if disparos else 0.0,
        "recall": round(fraudes / total_fraudes, 4),
    })

spark.createDataFrame(resultados).orderBy(F.desc("precisao")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 A originacao de credito piorou ao longo do ano?
# MAGIC
# MAGIC Analise de safra: cada linha e um mes de originacao. Se a PD sobe conforme a
# MAGIC safra fica mais recente, a politica de credito afrouxou.

# COMMAND ----------

spark.sql("""
    SELECT vintage                                             AS safra,
           sum(contracts)                                      AS contratos,
           round(sum(ead), 2)                                  AS exposicao,
           round(sum(npl_balance) / sum(ead) * 100, 2)         AS npl_pct,
           round(sum(expected_loss), 2)                        AS perda_esperada,
           round(sum(provision_amount) / greatest(sum(npl_balance), 1), 2) AS cobertura
    FROM credit_risk_portfolio
    GROUP BY vintage
    HAVING sum(contracts) >= 10
    ORDER BY safra
""").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 A DRE mensal da fintech

# COMMAND ----------

spark.sql("""
    SELECT year_month                                   AS mes,
           round(sum(tpv), 2)                           AS tpv,
           round(sum(revenue_mdr), 2)                   AS receita_mdr,
           round(sum(revenue_interest), 2)              AS receita_juros,
           round(sum(loss_chargeback + loss_fraud), 2)  AS perdas_transacionais,
           round(sum(loss_write_off), 2)                AS perdas_credito,
           round(sum(recovery_amount), 2)               AS recuperacao,
           round(sum(net_result), 2)                    AS resultado,
           round(avg(take_rate) * 100, 3)               AS take_rate_pct
    FROM financial_kpis_daily
    GROUP BY year_month ORDER BY mes
""").show(24, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Recursos do Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Historico e auditoria
# MAGIC
# MAGIC Cada commit registra a operacao, as metricas e o `userMetadata` carimbado pelo
# MAGIC pipeline (com o `run_id`). E a trilha de auditoria que um data lake em Parquet
# MAGIC puro simplesmente nao tem.

# COMMAND ----------

describe_history(spark, cfg.table_path("silver", "transactions"), limit=10).show(truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Time travel

# COMMAND ----------

caminho = cfg.table_path("gold", "customer_360")
versoes = [linha["version"] for linha in describe_history(spark, caminho, limit=20).collect()]

for versao in sorted(versoes)[:5]:
    print(f"versao {versao}: {read_delta(spark, caminho, version=versao).count()} linhas")

print("\nPor timestamp:")
print("  spark.read.format('delta').option('timestampAsOf', '2024-07-01').load(path)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Layout fisico e data skipping

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL delta.`{cfg.table_path('silver', 'transactions')}`") \
    .select("format", "numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)

print("Plano de execucao com filtro de particao (repare no PartitionFilters):")
silver.where(F.col("event_date") == "2024-07-15").select("transaction_id", "amount").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Qualidade de dados ao longo do tempo

# COMMAND ----------

caminho_dq = cfg.table_path("quality", "dq_results")
if table_exists(spark, caminho_dq):
    spark.read.format("delta").load(caminho_dq) \
        .groupBy("dataset").agg(
            F.count("*").alias("checagens"),
            F.sum(F.col("passed").cast("int")).alias("aprovadas"),
            F.sum((~F.col("passed")).cast("int")).alias("reprovadas"),
            F.max("violation_ratio").alias("pior_taxa_violacao"),
        ).orderBy("dataset").show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Camada de IA — perguntando em linguagem natural

# COMMAND ----------

from src.ai.rag_pipeline import ask

for pergunta in [
    "Como esta a inadimplencia da carteira de credito?",
    "Qual canal de pagamento concentra mais fraude?",
]:
    resultado = ask(pergunta)
    print("=" * 78)
    print(resultado["resposta"])
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Proximos passos
# MAGIC
# MAGIC * `make demo` — time travel, schema evolution, `MERGE`, `RESTORE` e constraints
# MAGIC * `make test` — testes automatizados (incluindo o de vazamento point-in-time)
# MAGIC * `reports/` — relatorio analitico gerado automaticamente pela camada de IA
# MAGIC * `docs/architecture.md` — as decisoes de arquitetura e seus trade-offs
