"""
Contratos de schema: detectar mudanca de estrutura ANTES de ela virar incidente.

O problema
----------
A origem adiciona, remove ou muda o tipo de uma coluna e ninguem avisa. Com
`mergeSchema` ligado (necessario na Bronze), a coluna nova simplesmente entra —
e a coluna REMOVIDA passa a chegar como NULL em todas as linhas novas, sem erro
nenhum. O dashboard so quebra semanas depois, quando alguem percebe que uma
metrica esta zerada desde uma data qualquer.

A solucao
---------
Congelar o schema de cada tabela em um arquivo JSON versionado no Git
(`contracts/`) e comparar a cada execucao. As mudancas sao classificadas:

* **BREAKING** — coluna removida, tipo alterado de forma incompativel, ou
  coluna que era obrigatoria virou opcional. Falha o pipeline.
* **ADDITIVE** — coluna nova. Passa, mas fica registrado (e o contrato precisa
  ser atualizado deliberadamente, com `--update`, o que gera diff no Git e
  passa por code review).
* **COMPATIBLE** — alargamento de tipo seguro (int -> long, float -> double).
  Passa.

Por que isso vale um modulo proprio: e a diferenca entre descobrir a mudanca no
pull request e descobrir no relatorio do fechamento do mes.

Execucao:
    python -m src.quality.schema_contract --check
    python -m src.quality.schema_contract --update   # aceita o schema atual
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from src.common.config import get_settings
from src.common.delta_io import table_exists
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

BREAKING = "BREAKING"
ADDITIVE = "ADDITIVE"
COMPATIBLE = "COMPATIBLE"

# Alargamentos de tipo considerados seguros: o dado antigo continua cabendo.
SAFE_WIDENING: dict[str, tuple[str, ...]] = {
    "byte": ("short", "int", "bigint", "float", "double", "decimal"),
    "short": ("int", "bigint", "float", "double", "decimal"),
    "int": ("bigint", "float", "double", "decimal"),
    "bigint": ("double", "decimal"),
    "float": ("double",),
    "date": ("timestamp",),
}

# Tabelas cobertas por contrato. Bronze fica de fora de proposito: a Bronze
# EXISTE para absorver mudanca de origem sem quebrar. O contrato vale da Silver
# em diante, que e o que os consumidores enxergam.
CONTRACTED_TABLES: list[tuple[str, str]] = [
    ("silver", "customers"),
    ("silver", "merchants"),
    ("silver", "transactions"),
    ("silver", "credit_contracts"),
    ("gold", "customer_360"),
    ("gold", "transaction_fraud_signals"),
    ("gold", "credit_risk_portfolio"),
    ("gold", "financial_kpis_daily"),
    ("gold", "merchant_performance"),
    ("feature_store", "customer_features"),
    ("feature_store", "transaction_features"),
]

# Colunas tecnicas mudam com o pipeline e nao fazem parte do contrato publico.
IGNORED_PREFIXES = ("_",)


@dataclass
class SchemaChange:
    """Uma diferenca entre o contrato congelado e o schema atual."""

    column: str
    kind: str  # BREAKING | ADDITIVE | COMPATIBLE
    detail: str


@dataclass
class ContractResult:
    """Resultado da verificacao de contrato de uma tabela."""

    table: str
    status: str  # ok | novo | violado | alterado
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def breaking(self) -> list[SchemaChange]:
        return [c for c in self.changes if c.kind == BREAKING]


class SchemaContractViolation(RuntimeError):
    """Levantada quando uma mudanca BREAKING e detectada."""


# ---------------------------------------------------------------------------
# Serializacao do contrato
# ---------------------------------------------------------------------------


def schema_to_contract(schema: StructType) -> dict[str, dict[str, object]]:
    """Converte um schema Spark no formato do contrato (colunas publicas)."""
    return {
        field.name: {
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in schema.fields
        if not field.name.startswith(IGNORED_PREFIXES)
    }


def contract_path(table_key: str) -> Path:
    cfg = get_settings()
    directory = cfg.project_root / str(cfg.raw["quality"]["contracts_dir"])
    return directory / f"{table_key.replace('.', '__')}.json"


def load_contract(table_key: str) -> dict | None:
    path = contract_path(table_key)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_contract(table_key: str, schema: StructType, note: str = "") -> Path:
    """Congela o schema atual como contrato (gera diff revisavel no Git)."""
    path = contract_path(table_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "table": table_key,
        "frozen_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "note": note,
        "columns": schema_to_contract(schema),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Comparacao
# ---------------------------------------------------------------------------


def _is_safe_widening(old_type: str, new_type: str) -> bool:
    """Verifica se a mudanca de tipo preserva os dados existentes."""
    base_old = old_type.split("(")[0]
    base_new = new_type.split("(")[0]
    return base_new in SAFE_WIDENING.get(base_old, ())


def compare(contract: dict, schema: StructType) -> list[SchemaChange]:
    """Compara o contrato congelado com o schema atual."""
    expected: dict[str, dict] = contract["columns"]
    current = schema_to_contract(schema)
    changes: list[SchemaChange] = []

    for column, spec in expected.items():
        if column not in current:
            changes.append(
                SchemaChange(
                    column,
                    BREAKING,
                    f"coluna removida (era `{spec['type']}`) - consumidores a jusante vao quebrar",
                )
            )
            continue

        old_type, new_type = str(spec["type"]), str(current[column]["type"])
        if old_type != new_type:
            if _is_safe_widening(old_type, new_type):
                changes.append(
                    SchemaChange(
                        column, COMPATIBLE, f"tipo alargado de `{old_type}` para `{new_type}`"
                    )
                )
            else:
                changes.append(
                    SchemaChange(
                        column,
                        BREAKING,
                        f"tipo alterado de `{old_type}` para `{new_type}` - conversao nao e segura",
                    )
                )

        # Uma coluna que era obrigatoria e virou opcional quebra quem assumia
        # que ela sempre viria preenchida.
        if not spec["nullable"] and current[column]["nullable"]:
            changes.append(
                SchemaChange(
                    column, BREAKING, "coluna deixou de ser obrigatoria (NOT NULL -> NULL)"
                )
            )

    for column, spec in current.items():
        if column not in expected:
            changes.append(
                SchemaChange(
                    column, ADDITIVE, f"coluna nova (`{spec['type']}`) - atualize o contrato"
                )
            )

    return changes


def check_table(
    spark: SparkSession, layer: str, table: str, update: bool = False
) -> ContractResult:
    """Verifica (ou atualiza) o contrato de uma tabela."""
    cfg = get_settings()
    table_key = f"{layer}.{table}"
    path = cfg.table_path(layer, table)

    if not table_exists(spark, path):
        return ContractResult(table_key, "ausente")

    schema = spark.read.format("delta").load(path).schema
    contract = load_contract(table_key)

    if contract is None:
        save_contract(table_key, schema, note="contrato criado automaticamente")
        log.info(
            "Contrato criado: %s (%s colunas publicas)", table_key, len(schema_to_contract(schema))
        )
        return ContractResult(table_key, "novo")

    changes = compare(contract, schema)

    if update and changes:
        save_contract(table_key, schema, note="contrato atualizado deliberadamente")
        log.info("Contrato atualizado: %s (%s mudancas absorvidas)", table_key, len(changes))
        return ContractResult(table_key, "alterado", changes)

    status = "violado" if any(c.kind == BREAKING for c in changes) else "ok"
    return ContractResult(table_key, status, changes)


def run(update: bool = False, raise_on_breaking: bool = True) -> list[ContractResult]:
    """Verifica os contratos de todas as tabelas cobertas."""
    spark = get_spark("schema_contract")
    results: list[ContractResult] = []

    with log_stage(log, "quality.schema_contract"):
        for layer, table in CONTRACTED_TABLES:
            result = check_table(spark, layer, table, update=update)
            results.append(result)

            for change in result.changes:
                level = log.error if change.kind == BREAKING else log.info
                level(
                    "CONTRATO %-9s | %-34s | %-22s | %s",
                    change.kind,
                    result.table,
                    change.column,
                    change.detail,
                )

            if result.status == "ok":
                log.info("CONTRATO OK        | %-34s | schema estavel", result.table)

        violated = [r for r in results if r.status == "violado"]
        if violated and raise_on_breaking:
            detalhe = "; ".join(
                f"{r.table}: {', '.join(c.column for c in r.breaking)}" for r in violated
            )
            raise SchemaContractViolation(
                f"Mudanca de schema incompativel detectada -> {detalhe}. "
                "Se a mudanca for intencional, rode "
                "`python -m src.quality.schema_contract --update` e revise o diff."
            )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Contratos de schema das tabelas do lakehouse.")
    parser.add_argument("--check", action="store_true", help="verifica os contratos (padrao)")
    parser.add_argument(
        "--update", action="store_true", help="congela o schema atual como novo contrato"
    )
    args = parser.parse_args()
    run(update=args.update, raise_on_breaking=not args.update)


if __name__ == "__main__":
    main()
