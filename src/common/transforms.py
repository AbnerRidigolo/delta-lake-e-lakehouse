"""
Transformacoes reutilizaveis da camada Silver.

Sao as operacoes que aparecem em todo job de limpeza: normalizar texto,
converter string em numero/data sem estourar, deduplicar mantendo o registro
mais recente e mascarar dado pessoal.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def normalize_text(column: str) -> Column:
    """Padroniza texto categorico: sem espacos nas bordas, minusculo, sem tabs.

    Resolve exatamente o tipo de sujeira que a origem manda:
    `"  POS "`, `"Pos\\t"`, `"pos"` -> todos viram `"pos"`.
    """
    return F.lower(F.trim(F.regexp_replace(F.col(column), r"[\t\r\n]+", " ")))


def safe_double(column: str) -> Column:
    """Converte string em double; valores nao numericos viram NULL (sem quebrar).

    O `try_cast` do Spark 3.x devolve NULL em vez de lancar excecao - e o que
    queremos: o registro invalido e capturado depois pela regra de qualidade,
    nao derruba o job inteiro.
    """
    return F.expr(f"try_cast(nullif(trim(`{column}`), '') as double)")


def safe_int(column: str) -> Column:
    """Converte string em inteiro de forma tolerante a lixo."""
    return F.expr(f"try_cast(nullif(trim(`{column}`), '') as int)")


def safe_timestamp(column: str) -> Column:
    """Converte string em timestamp aceitando os formatos mais comuns.

    Testa ISO (`2024-07-01 13:45:00`), ISO com `T` e o formato brasileiro
    (`01/07/2024 13:45:00`). O que nao casar com nenhum vira NULL.
    """
    col = F.nullif(F.trim(F.col(column)), F.lit(""))
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(col, "yyyy-MM-dd"),
    )


def safe_date(column: str) -> Column:
    """Converte string em date de forma tolerante."""
    col = F.nullif(F.trim(F.col(column)), F.lit(""))
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "dd/MM/yyyy"),
    )


def safe_boolean(column: str) -> Column:
    """Converte variacoes textuais de booleano ('true', 'SIM', '1') em boolean."""
    normalized = F.lower(F.trim(F.col(column)))
    return (
        F.when(normalized.isin("true", "t", "1", "sim", "s", "yes", "y"), F.lit(True))
        .when(normalized.isin("false", "f", "0", "nao", "n", "no"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def deduplicate(df: DataFrame, keys: Sequence[str], order_by: Sequence[Column]) -> DataFrame:
    """Mantem apenas um registro por chave, escolhendo pela ordenacao informada.

    A origem reprocessa arquivos e manda a mesma chave mais de uma vez. Em vez
    de `dropDuplicates` (que escolhe uma linha arbitraria), usamos uma janela
    ordenada para ficar com a versao mais recente - deterministico e auditavel.
    """
    window = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*order_by)
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .where(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def mask_email(column: str) -> Column:
    """Mascara e-mail preservando o dominio: `ana.silva@x.com` -> `a***@x.com`."""
    col = F.col(column)
    local = F.substring_index(col, "@", 1)
    domain = F.substring_index(col, "@", -1)
    return F.when(
        col.contains("@"),
        F.concat(F.substring(local, 1, 1), F.lit("***@"), domain),
    ).otherwise(F.lit(None).cast("string"))


def mask_document(column: str) -> Column:
    """Mascara CPF mantendo apenas os digitos centrais: `***.456.789-**`."""
    digits = F.regexp_replace(F.col(column), r"\D", "")
    return F.when(
        F.length(digits) == 11,
        F.concat(
            F.lit("***."),
            F.substring(digits, 4, 3),
            F.lit("."),
            F.substring(digits, 7, 3),
            F.lit("-**"),
        ),
    ).otherwise(F.lit("***"))


def hash_pii(column: str, salt: str = "neopag") -> Column:
    """Pseudonimiza um identificador com SHA-256 + salt.

    Permite juntar tabelas por cliente sem expor o documento - o padrao usado
    para compartilhar dado com times de analytics sob LGPD.
    """
    return F.sha2(F.concat(F.lit(salt), F.coalesce(F.col(column), F.lit(""))), 256)


def split_valid_invalid(
    df: DataFrame, condition: Column, reason_expr: Column
) -> tuple[DataFrame, DataFrame]:
    """Separa o DataFrame em (validos, rejeitados-com-motivo).

    Registros rejeitados nao sao descartados: vao para uma tabela de quarentena,
    para que o time de dados investigue a origem do problema em vez de descobrir
    meses depois que 3% do faturamento sumiu.
    """
    valid = df.where(condition)
    invalid = df.where(~condition | condition.isNull()).withColumn("_reject_reason", reason_expr)
    return valid, invalid


def audit_columns() -> list[str]:
    """Colunas tecnicas adicionadas pela ingestao (uteis para excluir de joins)."""
    return [
        "_ingested_at",
        "_source_system",
        "_pipeline_run_id",
        "_source_file",
        "_ingestion_date",
        "_corrupt_record",
        "_ingestion_lag_seconds",
    ]
