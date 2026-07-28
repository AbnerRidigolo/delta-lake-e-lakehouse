"""
Camada BRONZE - ingestao em STREAMING das transacoes (Structured Streaming).

O que este job simula
---------------------
Em producao, as transacoes chegariam de um topico Kafka (ou do Kinesis/Event
Hubs) e seriam lidas com `readStream.format("kafka")`. Aqui o "topico" e o
diretorio `data/raw/streaming/transactions/`, onde o gerador despeja micro-lotes
em JSONL - exatamente o formato que um conector de sink escreveria no storage.

A troca de fonte e de uma linha: mudar `format("json")` por `format("kafka")`
(ou `cloudFiles`, se for Auto Loader no Databricks). O restante do job -
schema, colunas de auditoria, checkpoint, sink Delta - permanece igual.

Conceitos demonstrados
----------------------
* **Exactly-once**: o checkpoint (`data/_checkpoints/...`) guarda o offset dos
  arquivos ja processados. Rodar o job duas vezes NAO duplica dados.
* **Trigger AvailableNow**: processa tudo que existe e encerra. E o padrao
  moderno para streaming "incremental batch" (custa como batch, tem a semantica
  de streaming). Use `processingTime` para um stream continuo de verdade.
* **Mesma tabela para batch e streaming**: os dois caminhos escrevem em
  `bronze.transactions`. Isso e o coracao do Lakehouse - uma unica tabela,
  transacional (ACID), servindo cargas historicas e tempo real.

Execucao:
    python -m src.bronze.bronze_transactions_stream
    python -m src.bronze.bronze_transactions_stream --continuous  # nao encerra
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.bronze.schemas import TRANSACTIONS_SCHEMA
from src.common.config import get_settings
from src.common.delta_io import register_table, with_audit_columns
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

CHECKPOINT_NAME = "bronze_transactions_stream"
SOURCE_SYSTEM = "payments_stream"


def build_stream(spark: SparkSession, max_files_per_trigger: int = 2) -> DataFrame:
    """Cria o DataFrame de streaming a partir do diretorio de eventos JSONL.

    `maxFilesPerTrigger` limita quantos arquivos entram por micro-batch: e o
    controle de vazao que evita que um backlog de horas vire um unico batch
    gigante e estoure a memoria do cluster.
    """
    cfg = get_settings()
    source_dir = str(cfg.layer_path("raw") / "streaming" / "transactions")

    stream = (
        spark.readStream.schema(TRANSACTIONS_SCHEMA)
        .option("maxFilesPerTrigger", max_files_per_trigger)
        # Ignora arquivos parcialmente escritos pelo produtor.
        .option("cleanSource", "off")
        .json(source_dir)
    )

    return (
        with_audit_columns(stream, SOURCE_SYSTEM)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingestion_date", F.current_date())
        # Marca a latencia de ingestao: diferenca entre o evento e a chegada.
        .withColumn("_ingestion_lag_seconds",
                    F.unix_timestamp(F.current_timestamp())
                    - F.unix_timestamp(F.to_timestamp(F.col("event_ts"))))
    )


def run(continuous: bool = False, processing_time: str = "10 seconds") -> None:
    """Executa a ingestao de streaming ate esgotar a fonte (ou continuamente)."""
    cfg = get_settings()
    spark = get_spark("bronze_transactions_stream")

    bronze_path = cfg.table_path("bronze", "transactions")
    checkpoint = cfg.checkpoint_path(CHECKPOINT_NAME)

    with log_stage(log, "bronze.transactions (streaming)"):
        stream_df = build_stream(spark)

        writer = (
            stream_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            # Schema evolution tambem no streaming: a coluna extra
            # `_ingestion_lag_seconds` e absorvida pela tabela criada no batch.
            .option("mergeSchema", "true")
            .partitionBy("_ingestion_date")
        )

        if continuous:
            log.info("Modo continuo: trigger a cada %s (Ctrl+C para parar)", processing_time)
            query = writer.trigger(processingTime=processing_time).start(bronze_path)
        else:
            log.info("Modo AvailableNow: processa o backlog e encerra")
            query = writer.trigger(availableNow=True).start(bronze_path)

        query.awaitTermination()

        register_table(spark, cfg.full_table_name("bronze", "transactions"), bronze_path)

        stream_rows = (
            spark.read.format("delta").load(bronze_path)
            .where(F.col("_source_system") == SOURCE_SYSTEM).count()
        )
        total_rows = spark.read.format("delta").load(bronze_path).count()
        log.info("bronze.transactions: %s linhas via streaming | %s no total",
                 stream_rows, total_rows)
        log.info("Progresso do ultimo micro-batch: %s", query.lastProgress)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestao streaming de transacoes -> Bronze.")
    parser.add_argument("--continuous", action="store_true",
                        help="mantem o stream ativo em vez de encerrar apos o backlog")
    parser.add_argument("--processing-time", default="10 seconds",
                        help="intervalo do trigger no modo continuo")
    args = parser.parse_args()
    run(continuous=args.continuous, processing_time=args.processing_time)


if __name__ == "__main__":
    main()
