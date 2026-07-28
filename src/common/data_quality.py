"""
Framework minimalista de Data Quality (estilo Great Expectations, sem a dependencia).

Motivacao
---------
Um lakehouse sem contrato de qualidade vira um "data swamp". Aqui cada job
declara as expectativas da tabela que produz; o resultado de cada checagem e
persistido em uma tabela Delta (`quality/dq_results`), o que permite montar um
painel historico de qualidade e disparar alertas.

Design:
* Checagens **row-level** (nulo, dominio, faixa, regex) sao avaliadas em uma
  unica passada com `agg`, para nao varrer o DataFrame N vezes.
* Checagens **table-level** (unicidade, volume minimo, frescor) rodam em
  queries dedicadas.
* Severidade `error` derruba o job (`raise`); `warn` apenas registra.

Exemplo:

    checks = [
        not_null("transaction_id"),
        unique("transaction_id"),
        allowed_values("status", ["approved", "declined"]),
        between("amount", 0.01, 1_000_000),
        row_count_min(1000),
    ]
    run_quality_checks(spark, df, "silver.transactions", checks)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import PIPELINE_RUN_ID, write_delta
from src.common.logging_utils import get_logger

log = get_logger(__name__)

SEVERITY_ERROR = "error"
SEVERITY_WARN = "warn"


class DataQualityError(RuntimeError):
    """Levantada quando uma expectativa de severidade `error` falha."""


@dataclass
class Expectation:
    """Uma regra de qualidade a ser aplicada sobre um DataFrame."""

    name: str
    kind: str
    column: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    severity: str = SEVERITY_ERROR
    # Tolerancia: fracao maxima de linhas que pode violar a regra (0 = zero tolerancia)
    threshold: float = 0.0


# ---------------------------------------------------------------------------
# Construtores de expectativas (acucar sintatico para deixar os jobs legiveis)
# ---------------------------------------------------------------------------

def not_null(column: str, severity: str = SEVERITY_ERROR, threshold: float = 0.0) -> Expectation:
    return Expectation(f"not_null::{column}", "not_null", column, severity=severity, threshold=threshold)


def unique(column: str, severity: str = SEVERITY_ERROR) -> Expectation:
    return Expectation(f"unique::{column}", "unique", column, severity=severity)


def allowed_values(column: str, values: List[Any], severity: str = SEVERITY_ERROR,
                   threshold: float = 0.0) -> Expectation:
    return Expectation(
        f"allowed_values::{column}", "allowed_values", column,
        {"values": values}, severity=severity, threshold=threshold,
    )


def between(column: str, minimum: float, maximum: float, severity: str = SEVERITY_ERROR,
            threshold: float = 0.0) -> Expectation:
    return Expectation(
        f"between::{column}", "between", column,
        {"min": minimum, "max": maximum}, severity=severity, threshold=threshold,
    )


def matches_regex(column: str, pattern: str, severity: str = SEVERITY_WARN,
                  threshold: float = 0.0) -> Expectation:
    return Expectation(
        f"regex::{column}", "regex", column, {"pattern": pattern},
        severity=severity, threshold=threshold,
    )


def row_count_min(minimum: int, severity: str = SEVERITY_ERROR) -> Expectation:
    return Expectation("row_count_min", "row_count_min", None, {"min": minimum}, severity=severity)


def freshness_max_days(column: str, max_days: int, severity: str = SEVERITY_WARN) -> Expectation:
    """Garante que o dado mais recente da tabela nao esteja velho demais."""
    return Expectation(
        f"freshness::{column}", "freshness", column, {"max_days": max_days}, severity=severity
    )


# ---------------------------------------------------------------------------
# Motor de execucao
# ---------------------------------------------------------------------------

def _violation_condition(exp: Expectation) -> Optional[Column]:
    """Traduz uma expectativa row-level em uma condicao booleana de violacao."""
    col = F.col(exp.column) if exp.column else None

    if exp.kind == "not_null":
        return col.isNull()
    if exp.kind == "allowed_values":
        return (~col.isin(exp.params["values"])) & col.isNotNull()
    if exp.kind == "between":
        return (col < F.lit(exp.params["min"])) | (col > F.lit(exp.params["max"]))
    if exp.kind == "regex":
        return (~col.rlike(exp.params["pattern"])) & col.isNotNull()
    return None  # checagem table-level


def run_quality_checks(
    spark: SparkSession,
    df: DataFrame,
    dataset: str,
    expectations: List[Expectation],
    persist: bool = True,
    raise_on_error: bool = True,
) -> DataFrame:
    """Executa as expectativas e devolve um DataFrame com o resultado.

    Args:
        dataset: nome logico da tabela avaliada (ex.: "silver.transactions").
        persist: grava o resultado em `data/quality/dq_results` (append).
        raise_on_error: interrompe o pipeline se alguma regra `error` falhar.
    """
    df = df.cache()
    total_rows = df.count()
    results: List[Dict[str, Any]] = []

    # ---- 1) Checagens row-level: uma unica passada com varios agregados -----
    row_level = [(e, _violation_condition(e)) for e in expectations]
    row_level = [(e, cond) for e, cond in row_level if cond is not None]

    if row_level and total_rows > 0:
        aggregations = [
            F.sum(F.when(cond, F.lit(1)).otherwise(F.lit(0))).alias(f"v_{idx}")
            for idx, (_, cond) in enumerate(row_level)
        ]
        counts = df.agg(*aggregations).collect()[0]
        for idx, (exp, _) in enumerate(row_level):
            violations = int(counts[f"v_{idx}"] or 0)
            results.append(_build_result(exp, dataset, total_rows, violations))
    elif row_level:
        for exp, _ in row_level:
            results.append(_build_result(exp, dataset, total_rows, 0))

    # ---- 2) Checagens table-level -------------------------------------------
    for exp in expectations:
        if _violation_condition(exp) is not None:
            continue

        if exp.kind == "unique":
            distinct = df.select(exp.column).where(F.col(exp.column).isNotNull()).distinct().count()
            non_null = df.where(F.col(exp.column).isNotNull()).count()
            violations = max(non_null - distinct, 0)
            results.append(_build_result(exp, dataset, total_rows, violations))

        elif exp.kind == "row_count_min":
            violations = 0 if total_rows >= exp.params["min"] else 1
            results.append(
                _build_result(exp, dataset, total_rows, violations,
                              detail=f"linhas={total_rows}, minimo={exp.params['min']}")
            )

        elif exp.kind == "freshness":
            max_value = df.agg(F.max(F.col(exp.column))).collect()[0][0]
            if max_value is None:
                violations, detail = 1, "coluna sem valores"
            else:
                age_days = (datetime.now().date() - _as_date(max_value)).days
                violations = 0 if age_days <= exp.params["max_days"] else 1
                detail = f"ultimo evento ha {age_days} dia(s)"
            results.append(_build_result(exp, dataset, total_rows, violations, detail=detail))

    df.unpersist()

    result_df = spark.createDataFrame(results) if results else spark.createDataFrame([], _empty_schema())
    _log_results(dataset, results)

    if persist and results:
        cfg = get_settings()
        write_delta(
            result_df,
            cfg.table_path("quality", "dq_results"),
            mode="append",
            partition_by=["dataset"],
            merge_schema=True,
            comment=f"data quality run :: {dataset}",
        )

    failures = [r for r in results if not r["passed"] and r["severity"] == SEVERITY_ERROR]
    if failures and raise_on_error:
        names = ", ".join(f["expectation"] for f in failures)
        raise DataQualityError(f"Data Quality falhou em `{dataset}`: {names}")

    return result_df


def _build_result(exp: Expectation, dataset: str, total_rows: int, violations: int,
                  detail: str = "") -> Dict[str, Any]:
    ratio = (violations / total_rows) if total_rows else 0.0
    passed = ratio <= exp.threshold if exp.kind not in ("row_count_min", "freshness") else violations == 0
    return {
        "run_id": PIPELINE_RUN_ID,
        "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "dataset": dataset,
        "expectation": exp.name,
        "kind": exp.kind,
        "column": exp.column or "",
        "severity": exp.severity,
        "total_rows": int(total_rows),
        "violations": int(violations),
        "violation_ratio": float(round(ratio, 6)),
        "threshold": float(exp.threshold),
        "passed": bool(passed),
        "detail": detail,
    }


def _empty_schema():
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                   StructField, StructType, TimestampType)

    return StructType([
        StructField("run_id", StringType()),
        StructField("checked_at", TimestampType()),
        StructField("dataset", StringType()),
        StructField("expectation", StringType()),
        StructField("kind", StringType()),
        StructField("column", StringType()),
        StructField("severity", StringType()),
        StructField("total_rows", LongType()),
        StructField("violations", LongType()),
        StructField("violation_ratio", DoubleType()),
        StructField("threshold", DoubleType()),
        StructField("passed", BooleanType()),
        StructField("detail", StringType()),
    ])


def _as_date(value):
    return value.date() if hasattr(value, "date") else value


def _log_results(dataset: str, results: List[Dict[str, Any]]) -> None:
    for res in results:
        status = "PASS" if res["passed"] else ("FAIL" if res["severity"] == SEVERITY_ERROR else "WARN")
        log.info(
            "DQ %-4s | %-24s | %-28s | violacoes=%s/%s %s",
            status, dataset, res["expectation"], res["violations"], res["total_rows"],
            res["detail"],
        )
