"""
Manutencao das tabelas Delta: OPTIMIZE, Z-ORDER e VACUUM.

Por que manutencao e parte do pipeline, nao um extra
---------------------------------------------------
Um lakehouse que so escreve degrada sozinho:

* **Small files**: cada micro-batch de streaming e cada reprocessamento cria
  arquivos pequenos. Milhares deles fazem o planejamento da query custar mais
  que a leitura em si. `OPTIMIZE` compacta.
* **Data skipping ineficiente**: o Delta guarda min/max por arquivo e pula os
  que nao interessam. Isso so funciona se os dados estiverem *co-localizados*.
  `ZORDER BY` reorganiza os arquivos pelas colunas mais filtradas.
* **Custo de storage**: o time travel guarda versoes antigas indefinidamente.
  `VACUUM` remove os arquivos que ja passaram da janela de retencao.

A escolha das colunas de Z-ORDER e uma decisao de arquitetura: valem as colunas
usadas em `WHERE` e em `JOIN` de alta seletividade - nunca a coluna de particao
(essa ja e resolvida pelo particionamento).

ATENCAO ao VACUUM: ele apaga a capacidade de fazer time travel para versoes
anteriores a janela de retencao. O padrao do Delta e 7 dias por um bom motivo.
Aqui a retencao e configuravel e o modo padrao e conservador.

Execucao:
    python -m src.maintenance.delta_maintenance
    python -m src.maintenance.delta_maintenance --vacuum --retention-hours 168
"""

from __future__ import annotations

import argparse

from src.common.config import get_settings
from src.common.delta_io import table_exists
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

# (camada, tabela, colunas de Z-ORDER)
# As colunas foram escolhidas a partir dos filtros reais dos jobs a jusante.
MAINTENANCE_PLAN: list[tuple[str, str, list[str] | None]] = [
    ("bronze", "transactions", None),
    ("silver", "transactions", ["customer_id", "merchant_id"]),
    ("silver", "credit_contracts", ["customer_id", "product"]),
    ("silver", "customers", ["customer_id"]),
    ("silver", "merchants", ["merchant_id"]),
    # ATENCAO: o Delta so coleta estatisticas (min/max) das 32 PRIMEIRAS colunas
    # da tabela. Fazer Z-ORDER em uma coluna alem disso nao gera data skipping -
    # o proprio Delta rejeita a operacao. Por isso as colunas escolhidas aqui
    # estao sempre no inicio do schema. Para indexar uma coluna posterior, o
    # caminho e reordenar o SELECT ou aumentar `delta.dataSkippingNumIndexedCols`.
    ("gold", "transaction_fraud_signals", ["customer_id", "merchant_id"]),
    # `segment` nao entra: e a coluna de particao da tabela, ja resolvida pelo
    # particionamento. Z-ORDER nela seria trabalho jogado fora.
    ("gold", "customer_360", ["customer_id", "risk_band"]),
    ("gold", "merchant_performance", ["merchant_id"]),
    ("gold", "credit_risk_portfolio", ["vintage"]),
    ("gold", "financial_kpis_daily", ["event_date"]),
    ("feature_store", "transaction_features", ["customer_id"]),
    ("feature_store", "customer_features", ["customer_id"]),
]


def optimize_table(spark, path: str, zorder_columns: list[str] | None) -> dict:
    """Roda OPTIMIZE (com Z-ORDER quando aplicavel) e devolve as metricas."""
    statement = f"OPTIMIZE delta.`{path}`"
    if zorder_columns:
        statement += f" ZORDER BY ({', '.join(zorder_columns)})"

    result = spark.sql(statement).collect()
    metrics = result[0].asDict().get("metrics") if result else None

    summary = {"path": path, "zorder": zorder_columns or []}
    if metrics is not None:
        summary["arquivos_removidos"] = getattr(metrics, "numFilesRemoved", None)
        summary["arquivos_criados"] = getattr(metrics, "numFilesAdded", None)
    return summary


def vacuum_table(spark, path: str, retention_hours: int) -> None:
    """Remove arquivos fora da janela de retencao.

    Retencao abaixo de 168h (7 dias) exige desabilitar a trava de seguranca do
    Delta - fazemos isso apenas porque este e um ambiente de demonstracao. Em
    producao, retencao curta pode apagar arquivos que uma query longa ainda esta
    lendo.
    """
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")


def table_stats(spark, path: str) -> dict:
    """Estatisticas fisicas da tabela (arquivos, tamanho, versao atual)."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict()
    return {
        "arquivos": detail.get("numFiles"),
        "tamanho_mb": round((detail.get("sizeInBytes") or 0) / 1_048_576, 2),
        "particoes": detail.get("partitionColumns"),
    }


def run(vacuum: bool = False, retention_hours: int = 168) -> dict[str, dict]:
    """Executa o plano de manutencao em todas as tabelas materializadas."""
    cfg = get_settings()
    spark = get_spark("delta_maintenance")
    report: dict[str, dict] = {}

    with log_stage(log, "maintenance.delta"):
        for layer, table, zorder in MAINTENANCE_PLAN:
            path = cfg.table_path(layer, table)
            name = f"{layer}.{table}"

            if not table_exists(spark, path):
                log.warning("Tabela nao encontrada, pulando: %s", name)
                continue

            before = table_stats(spark, path)
            optimize_table(spark, path, zorder)
            after = table_stats(spark, path)

            if vacuum:
                vacuum_table(spark, path, retention_hours)
                after = table_stats(spark, path)

            report[name] = {"antes": before, "depois": after, "zorder": zorder or []}
            log.info(
                "%-38s | arquivos %s -> %s | %s MB | zorder=%s",
                name,
                before["arquivos"],
                after["arquivos"],
                after["tamanho_mb"],
                ", ".join(zorder or []) or "-",
            )

        total_before = sum(r["antes"]["arquivos"] or 0 for r in report.values())
        total_after = sum(r["depois"]["arquivos"] or 0 for r in report.values())
        log.info(
            "Compactacao total: %s -> %s arquivos (%s tabelas)",
            total_before,
            total_after,
            len(report),
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Manutencao das tabelas Delta.")
    parser.add_argument(
        "--vacuum", action="store_true", help="tambem remove arquivos fora da janela de retencao"
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=168,
        help="janela de retencao do VACUUM em horas (padrao: 168 = 7 dias)",
    )
    args = parser.parse_args()
    run(vacuum=args.vacuum, retention_hours=args.retention_hours)


if __name__ == "__main__":
    main()
