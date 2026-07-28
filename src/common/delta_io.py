"""
Helpers de escrita/leitura Delta usados por todas as camadas.

Concentrar isso aqui evita repetir `format("delta").mode(...)` em 15 jobs e
padroniza tres coisas importantes num lakehouse de verdade:

* **Auditoria**: toda escrita carrega colunas tecnicas (`_ingested_at`,
  `_source_system`, `_pipeline_run_id`).
* **Commit info**: cada operacao grava metadados no historico Delta
  (`userMetadata`), o que torna o time travel realmente investigavel.
* **Idempotencia**: `overwrite` + `replaceWhere` para reprocessar apenas uma
  particao de data sem apagar o resto da tabela.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.logging_utils import get_logger

log = get_logger(__name__)

# Identificador unico da execucao. Compartilhado por todos os jobs do mesmo
# processo (e propagado pelo orquestrador via variavel de ambiente).
PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID") or uuid.uuid4().hex[:12]


def with_audit_columns(df: DataFrame, source_system: str) -> DataFrame:
    """Adiciona as colunas tecnicas de auditoria/linhagem ao DataFrame."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    )


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partition_by: Optional[Iterable[str]] = None,
    merge_schema: bool = False,
    overwrite_schema: bool = False,
    replace_where: Optional[str] = None,
    comment: Optional[str] = None,
) -> None:
    """Escreve um DataFrame como tabela Delta.

    Args:
        mode: `overwrite` ou `append`.
        partition_by: colunas de particionamento (tipicamente a data do evento).
        merge_schema: habilita **schema evolution** (colunas novas sao aceitas).
        overwrite_schema: substitui o schema inteiro (mudanca incompativel).
        replace_where: predicado para sobrescrever apenas parte da tabela.
            Ex.: `"event_date = '2024-07-01'"` reprocessa so aquele dia.
        comment: texto salvo no historico Delta (`userMetadata`), visivel em
            `DESCRIBE HISTORY`.
    """
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(list(partition_by))
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)

    metadata = {
        "pipeline_run_id": PIPELINE_RUN_ID,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "note": comment or "",
    }
    writer = writer.option("userMetadata", json.dumps(metadata, ensure_ascii=False))

    writer.save(path)
    log.info("Delta escrito em %s (mode=%s, partition_by=%s)", path, mode, partition_by)


def read_delta(spark: SparkSession, path: str, version: Optional[int] = None,
               timestamp: Optional[str] = None) -> DataFrame:
    """Le uma tabela Delta, opcionalmente em uma versao/timestamp passado.

    Passar `version` ou `timestamp` e exatamente o **time travel** do Delta:
    a leitura reconstroi o estado da tabela naquele commit.
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    if timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(path)


def table_exists(spark: SparkSession, path: str) -> bool:
    """Verifica se ja existe uma tabela Delta no caminho informado."""
    try:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, path)
    except Exception:  # pragma: no cover - caminho inexistente
        return False


def register_table(spark: SparkSession, full_name: str, path: str) -> None:
    """Registra a tabela Delta no metastore como tabela externa.

    Isso permite consultar com SQL puro (`SELECT * FROM fintech.gold.customer_360`),
    que e como um analista usaria o lakehouse. No Databricks, o mesmo comando
    registra a tabela no Unity Catalog.
    """
    schema = ".".join(full_name.split(".")[:-1])
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")


def describe_history(spark: SparkSession, path: str, limit: int = 10) -> DataFrame:
    """Retorna o historico de commits da tabela (auditoria + time travel)."""
    from delta.tables import DeltaTable

    return (
        DeltaTable.forPath(spark, path)
        .history(limit)
        .select("version", "timestamp", "operation", "operationMetrics", "userMetadata")
    )
