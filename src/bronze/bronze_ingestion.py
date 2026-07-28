"""
Camada BRONZE - ingestao batch dos arquivos brutos em tabelas Delta.

Principios aplicados
--------------------
1. **Fidelidade a origem**: nenhuma regra de negocio, nenhum cast. O que chegou
   e o que fica gravado. Bronze e o "backup consultavel" do sistema de origem.
2. **Rastreabilidade**: cada linha carrega de qual arquivo veio
   (`_source_file`), quando foi ingerida (`_ingested_at`) e por qual execucao
   do pipeline (`_pipeline_run_id`).
3. **Particionamento por data de ingestao**: a Bronze e um log de chegadas, nao
   um modelo analitico. Particionar por `_ingestion_date` permite reprocessar
   uma carga especifica e casa com o padrao append-only.
   (A particao por data do *evento* aparece na Silver, onde o timestamp ja foi
   validado - particionar por uma coluna que pode estar corrompida seria pedir
   para criar particoes lixo.)
4. **Schema evolution ligado**: `mergeSchema=true`. Se a origem adicionar uma
   coluna nova, ela e absorvida sem quebrar o job.

Execucao:
    python -m src.bronze.bronze_ingestion                 # todas as tabelas
    python -m src.bronze.bronze_ingestion --table customers
"""

from __future__ import annotations

import argparse
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.bronze.schemas import BRONZE_SCHEMAS
from src.common import data_quality as dq
from src.common.config import get_settings
from src.common.delta_io import (register_table, table_exists, with_audit_columns,
                                 write_delta)
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

# Configuracao declarativa das fontes batch: diretorio na raw, schema, tabela
# Delta de destino e modo de escrita.
BATCH_SOURCES: Dict[str, Dict[str, object]] = {
    "customers": {
        "raw_glob": "customers/*.csv",
        "source_system": "core_banking_crm",
        # Cadastro: substituimos o snapshot inteiro a cada carga (full load).
        "mode": "overwrite",
        "partition_by": None,
    },
    "merchants": {
        "raw_glob": "merchants/*.csv",
        "source_system": "acquiring_platform",
        "mode": "overwrite",
        "partition_by": None,
    },
    "credit_contracts": {
        "raw_glob": "credit_contracts/*.csv",
        "source_system": "credit_core",
        "mode": "overwrite",
        "partition_by": None,
    },
    "transactions": {
        "raw_glob": "transactions/*.csv",
        "source_system": "payments_ledger",
        # Transacoes sao um log append-only, MAS a carga precisa ser idempotente:
        # rodar o job duas vezes no mesmo dia nao pode duplicar o volume. A
        # solucao e `replaceWhere` na particao do dia (ver `run()`), que substitui
        # apenas a carga corrente e preserva todo o historico anterior.
        "mode": "overwrite",
        "partition_by": ["_ingestion_date"],
        "idempotent_by_ingestion_date": True,
    },
}


def read_raw_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Le arquivos CSV da camada raw aplicando o schema string do contrato.

    `mode=PERMISSIVE` + `columnNameOfCorruptRecord` garante que uma linha
    malformada nao derrube a carga: ela e preservada na coluna
    `_corrupt_record` para investigacao posterior.
    """
    return (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("escape", '"')
        .schema(schema.add("_corrupt_record", "string"))
        .csv(path)
    )


def ingest_table(spark: SparkSession, table: str) -> int:
    """Ingere uma fonte batch da camada raw para a Bronze. Retorna a contagem."""
    cfg = get_settings()
    source = BATCH_SOURCES[table]
    schema = BRONZE_SCHEMAS[table]

    raw_path = str(cfg.layer_path("raw") / str(source["raw_glob"]))
    bronze_path = cfg.table_path("bronze", table)

    with log_stage(log, f"bronze.{table}"):
        raw_df = read_raw_csv(spark, raw_path, schema)

        bronze_df = (
            with_audit_columns(raw_df, str(source["source_system"]))
            # `_metadata.file_path` (Spark 3.3+) da a origem exata de cada linha.
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingestion_date", F.current_date())
        )

        # Carga idempotente: reescreve somente a particao do dia corrente.
        # Rodar o pipeline tres vezes hoje deixa a tabela igual a rodar uma vez,
        # sem tocar nas cargas dos dias anteriores.
        # (Na primeira carga a tabela ainda nao existe: `replaceWhere` nao se
        # aplica. E por isso que o batch roda ANTES do streaming no DAG - se
        # rodasse depois, apagaria os eventos ja ingeridos no mesmo dia.)
        replace_where = None
        if source.get("idempotent_by_ingestion_date") and table_exists(spark, bronze_path):
            today = spark.sql("SELECT current_date() AS d").collect()[0]["d"]
            replace_where = f"_ingestion_date = '{today}'"

        write_delta(
            bronze_df,
            bronze_path,
            mode=str(source["mode"]),
            partition_by=source["partition_by"],   # type: ignore[arg-type]
            merge_schema=True,
            replace_where=replace_where,
            comment=f"ingestao batch raw -> bronze.{table}",
        )

        register_table(spark, cfg.full_table_name("bronze", table), bronze_path)

        count = spark.read.format("delta").load(bronze_path).count()
        log.info("bronze.%s: %s linhas na tabela", table, count)

        # Bronze tem contrato minimo: a chave primaria precisa existir e o
        # volume nao pode desabar (sinal de arquivo truncado na origem).
        pk = {"customers": "customer_id", "merchants": "merchant_id",
              "transactions": "transaction_id", "credit_contracts": "contract_id"}[table]
        dq.run_quality_checks(
            spark,
            spark.read.format("delta").load(bronze_path),
            dataset=f"bronze.{table}",
            expectations=[
                dq.not_null(pk, severity=dq.SEVERITY_ERROR, threshold=0.001),
                dq.row_count_min(1),
                # Duplicatas sao ESPERADAS na Bronze (a origem reprocessa):
                # registramos como aviso e resolvemos na Silver.
                dq.unique(pk, severity=dq.SEVERITY_WARN),
            ],
        )
        return count


def run(tables: Optional[List[str]] = None) -> Dict[str, int]:
    """Executa a ingestao Bronze das tabelas informadas (ou de todas)."""
    spark = get_spark("bronze_ingestion")
    targets = tables or list(BATCH_SOURCES.keys())
    return {table: ingest_table(spark, table) for table in targets}


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestao batch raw -> Bronze (Delta).")
    parser.add_argument("--table", action="append", choices=list(BATCH_SOURCES.keys()),
                        help="tabela especifica (pode repetir); padrao: todas")
    args = parser.parse_args()
    run(args.table)


if __name__ == "__main__":
    main()
