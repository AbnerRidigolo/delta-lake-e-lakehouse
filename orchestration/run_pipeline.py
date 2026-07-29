"""
Orquestrador do pipeline do lakehouse (executavel, sem depender de Airflow).

Por que existe
--------------
Airflow e otimo em producao, mas exige scheduler, banco e webserver. Para
desenvolvimento, CI e demonstracao, o que se quer e um comando so que rode o
pipeline inteiro na ordem certa e falhe alto quando algo quebra.

Este modulo define o **DAG como dado** (uma lista de `Stage` com dependencias)
e resolve a ordem topologicamente. O mesmo grafo alimenta o DAG do Airflow em
`orchestration/airflow/dags/lakehouse_medallion_dag.py`, entao nao existe risco
de as duas definicoes divergirem.

Recursos
--------
* ordem topologica automatica a partir das dependencias declaradas;
* retry com backoff exponencial por etapa;
* `--from`, `--only` e `--skip` para reprocessar pedacos do pipeline;
* resumo final com duracao e status de cada etapa;
* `PIPELINE_RUN_ID` propagado para todas as tabelas (rastreabilidade ponta a ponta).

Execucao
--------
    python -m orchestration.run_pipeline                     # pipeline completo
    python -m orchestration.run_pipeline --from silver_transactions
    python -m orchestration.run_pipeline --only gold_customer_360
    python -m orchestration.run_pipeline --skip generate_data --skip train_fraud_model
    python -m orchestration.run_pipeline --list
"""

from __future__ import annotations

import argparse
import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

# O run_id precisa existir ANTES de qualquer import que toque em delta_io,
# porque e ele que carimba todas as tabelas escritas nesta execucao.
os.environ.setdefault("PIPELINE_RUN_ID", uuid.uuid4().hex[:12])

from src.common.logging_utils import get_logger  # noqa: E402

log = get_logger("orchestration")


# ===========================================================================
# Definicao do DAG
# ===========================================================================


@dataclass
class Stage:
    """Uma etapa do pipeline."""

    name: str
    description: str
    runner: Callable[[], object]
    depends_on: list[str] = field(default_factory=list)
    retries: int = 1
    # Etapas opcionais nao quebram o pipeline se falharem (ex.: manutencao).
    critical: bool = True


def _generate_data():
    from src.generators.generate_synthetic_data import generate

    return generate()


def _bronze_batch():
    from src.bronze.bronze_ingestion import run

    return run()


def _bronze_stream():
    from src.bronze.bronze_transactions_stream import run

    return run(continuous=False)


def _silver_customers():
    from src.silver.silver_customers import run

    return run()


def _silver_merchants():
    from src.silver.silver_merchants import run

    return run()


def _silver_transactions():
    from src.silver.silver_transactions import run

    return run()


def _silver_credit_contracts():
    from src.silver.silver_credit_contracts import run

    return run()


def _gold_customer_360():
    from src.gold.gold_customer_360 import run

    return run()


def _gold_fraud_signals():
    from src.gold.gold_transaction_fraud_signals import run

    return run()


def _gold_credit_risk():
    from src.gold.gold_credit_risk_portfolio import run

    return run()


def _gold_financial_kpis():
    from src.gold.gold_financial_kpis_daily import run

    return run()


def _gold_merchant_performance():
    from src.gold.gold_merchant_performance import run

    return run()


def _feature_store():
    from src.ml.feature_store import run

    return run()


def _train_fraud_model():
    from src.ml.train_fraud_model import run

    return run()


def _ai_insights():
    from src.ai.insight_generator import run

    return run()


def _rag_index():
    from src.ai.rag_pipeline import build_index

    return build_index()


def _quality_constraints():
    from src.quality.constraints import run

    return run()


def _quality_contracts():
    from src.quality.schema_contract import run

    return {"tabelas": len(run())}


def _quality_advanced():
    from src.quality.expectations import run_advanced_checks

    return {"checagens": len(run_advanced_checks())}


def _quality_reconciliation():
    from src.quality.reconciliation import run

    return {"identidades": len(run())}


def _quality_drift():
    from src.quality.drift import run

    return {"sinais": len(run())}


def _quality_scorecard():
    from src.quality.scorecard import run

    return run()


def _catalog_metadata():
    from src.governance.apply_catalog_metadata import run

    return run()


def _delta_maintenance():
    from src.maintenance.delta_maintenance import run

    return run()


# O grafo do pipeline. Ler esta lista deve ser suficiente para entender a
# arquitetura inteira - e o "indice" do projeto.
PIPELINE: list[Stage] = [
    Stage("generate_data", "Gera os dados sinteticos da fintech na camada raw", _generate_data, []),
    Stage(
        "bronze_batch",
        "Ingestao batch dos CSVs raw -> Bronze (Delta)",
        _bronze_batch,
        ["generate_data"],
    ),
    Stage(
        "bronze_stream",
        "Ingestao streaming dos eventos JSONL -> Bronze",
        _bronze_stream,
        ["bronze_batch"],
    ),
    Stage(
        "silver_customers",
        "Limpeza, LGPD e enriquecimento de clientes",
        _silver_customers,
        ["bronze_batch"],
    ),
    Stage(
        "silver_merchants",
        "Limpeza e taxonomia de MCC dos estabelecimentos",
        _silver_merchants,
        ["bronze_batch"],
    ),
    Stage(
        "silver_transactions",
        "Deduplicacao, validacao e enriquecimento de transacoes",
        _silver_transactions,
        ["bronze_stream", "silver_customers", "silver_merchants"],
    ),
    Stage(
        "silver_credit_contracts",
        "Faixas de atraso, provisao e safra dos contratos",
        _silver_credit_contracts,
        ["bronze_batch", "silver_customers"],
    ),
    Stage(
        "gold_customer_360",
        "Visao 360 do cliente com score e churn",
        _gold_customer_360,
        ["silver_transactions", "silver_credit_contracts"],
    ),
    Stage(
        "gold_fraud_signals",
        "Sinais e score de fraude por transacao",
        _gold_fraud_signals,
        ["silver_transactions"],
    ),
    Stage(
        "gold_credit_risk",
        "Carteira de credito por safra (PD/LGD/EL)",
        _gold_credit_risk,
        ["silver_credit_contracts"],
    ),
    Stage(
        "gold_financial_kpis",
        "KPIs financeiros diarios (receita, perdas, recuperacao)",
        _gold_financial_kpis,
        ["silver_transactions", "silver_credit_contracts"],
    ),
    Stage(
        "gold_merchant_performance",
        "Performance e risco por estabelecimento",
        _gold_merchant_performance,
        ["silver_transactions", "silver_merchants"],
    ),
    Stage(
        "feature_store",
        "Materializa as feature views (point-in-time)",
        _feature_store,
        ["gold_fraud_signals", "gold_customer_360"],
    ),
    Stage(
        "train_fraud_model",
        "Treina e avalia o modelo de fraude",
        _train_fraud_model,
        ["feature_store"],
    ),
    # --- Portao de qualidade -------------------------------------------------
    # Roda DEPOIS das camadas materializadas e ANTES da IA e da publicacao:
    # relatorio e indice RAG so devem ser gerados sobre dado que passou no portao.
    Stage(
        "quality_constraints",
        "Aplica as CHECK constraints do Delta nas tabelas",
        _quality_constraints,
        ["gold_customer_360", "gold_fraud_signals", "gold_credit_risk", "gold_financial_kpis"],
    ),
    Stage(
        "quality_contracts",
        "Verifica os contratos de schema (quebra vs. aditivo)",
        _quality_contracts,
        ["feature_store", "gold_merchant_performance"],
    ),
    Stage(
        "quality_advanced",
        "Expectativas estendidas (FK, chave composta, SQL, agregados)",
        _quality_advanced,
        ["feature_store", "gold_merchant_performance"],
    ),
    Stage(
        "quality_reconciliation",
        "Concilia linhas e valores entre as camadas",
        _quality_reconciliation,
        ["feature_store", "gold_financial_kpis", "gold_credit_risk"],
    ),
    Stage(
        "quality_drift",
        "Detecta drift de volume e de distribuicao (PSI)",
        _quality_drift,
        ["quality_advanced"],
    ),
    Stage(
        "quality_scorecard",
        "Consolida a qualidade em uma nota e aplica o portao",
        _quality_scorecard,
        ["quality_advanced", "quality_reconciliation", "quality_drift", "quality_contracts"],
    ),
    # Depende do treino (o relatorio incorpora as metricas do modelo) e do
    # portao de qualidade: nao se publica insight sobre dado reprovado.
    Stage(
        "ai_insights",
        "Gera o relatorio analitico automatico da Gold",
        _ai_insights,
        ["train_fraud_model", "quality_scorecard"],
    ),
    Stage(
        "rag_index", "Indexa a camada Gold no vetor store para o RAG", _rag_index, ["ai_insights"]
    ),
    # Depende da feature store para que TODAS as tabelas declaradas no catalogo
    # ja existam quando os metadados forem aplicados.
    Stage(
        "catalog_metadata",
        "Aplica os metadados de governanca nas tabelas Delta",
        _catalog_metadata,
        ["gold_customer_360", "gold_financial_kpis", "feature_store"],
        critical=False,
    ),
    Stage(
        "delta_maintenance",
        "OPTIMIZE / Z-ORDER / VACUUM das tabelas Delta",
        _delta_maintenance,
        ["catalog_metadata"],
        critical=False,
    ),
]

STAGES_BY_NAME: dict[str, Stage] = {stage.name: stage for stage in PIPELINE}


# ===========================================================================
# Motor de execucao
# ===========================================================================


def topological_order(stages: list[Stage]) -> list[Stage]:
    """Ordena as etapas respeitando as dependencias (algoritmo de Kahn)."""
    pending = {stage.name: set(stage.depends_on) for stage in stages}
    available = {stage.name for stage in stages}
    ordered: list[Stage] = []

    while pending:
        ready = sorted(
            name for name, deps in pending.items() if not (deps & available & set(pending))
        )
        if not ready:
            raise RuntimeError(f"Ciclo detectado no DAG: {sorted(pending)}")
        for name in ready:
            ordered.append(STAGES_BY_NAME[name])
            pending.pop(name)
    return ordered


def select_stages(
    start_from: str | None, only: list[str] | None, skip: list[str] | None
) -> list[Stage]:
    """Aplica os filtros de linha de comando sobre o DAG."""
    stages = list(PIPELINE)

    if only:
        stages = [s for s in stages if s.name in set(only)]
    if start_from:
        names = [s.name for s in PIPELINE]
        if start_from not in names:
            raise SystemExit(f"Etapa desconhecida: {start_from}")
        allowed = set(names[names.index(start_from) :])
        stages = [s for s in stages if s.name in allowed]
    if skip:
        stages = [s for s in stages if s.name not in set(skip)]

    # Dependencias que ficaram de fora do recorte sao ignoradas na ordenacao.
    selected = {s.name for s in stages}
    trimmed = [
        Stage(
            s.name,
            s.description,
            s.runner,
            [d for d in s.depends_on if d in selected],
            s.retries,
            s.critical,
        )
        for s in stages
    ]
    return topological_order(trimmed)


def run_stage(stage: Stage) -> dict:
    """Executa uma etapa com retry e backoff exponencial."""
    attempt = 0
    started = time.perf_counter()

    while True:
        attempt += 1
        try:
            log.info("=" * 78)
            log.info(">>> %s | %s (tentativa %s)", stage.name, stage.description, attempt)
            result = stage.runner()
            elapsed = time.perf_counter() - started
            log.info("<<< %s concluido em %.1fs", stage.name, elapsed)
            return {
                "stage": stage.name,
                "status": "sucesso",
                "duracao_s": round(elapsed, 1),
                "tentativas": attempt,
                "resultado": _summarize(result),
            }
        except Exception as exc:
            if attempt > stage.retries:
                elapsed = time.perf_counter() - started
                log.error("!!! %s FALHOU apos %s tentativa(s): %s", stage.name, attempt, exc)
                return {
                    "stage": stage.name,
                    "status": "falha",
                    "duracao_s": round(elapsed, 1),
                    "tentativas": attempt,
                    "erro": str(exc)[:400],
                }
            backoff = 2**attempt
            log.warning("%s falhou (%s). Nova tentativa em %ss...", stage.name, exc, backoff)
            time.sleep(backoff)


def _summarize(result) -> str:
    """Resume o retorno de uma etapa para caber no relatorio final."""
    if isinstance(result, dict):
        compact = {k: v for k, v in list(result.items())[:6] if isinstance(v, int | float | str)}
        return str(compact)
    return str(result)[:200] if result is not None else ""


def run_pipeline(
    start_from: str | None = None,
    only: list[str] | None = None,
    skip: list[str] | None = None,
    stop_on_failure: bool = True,
) -> list[dict]:
    """Executa o pipeline e devolve o relatorio de cada etapa."""
    stages = select_stages(start_from, only, skip)
    run_id = os.environ["PIPELINE_RUN_ID"]

    log.info("#" * 78)
    log.info("# PIPELINE LAKEHOUSE | run_id=%s | %s etapas", run_id, len(stages))
    log.info("# Ordem: %s", " -> ".join(s.name for s in stages))
    log.info("#" * 78)

    started = time.perf_counter()
    report: list[dict] = []
    failed: set = set()

    for stage in stages:
        # Se um pai falhou, a etapa e pulada (mesma semantica do Airflow).
        blocked = [d for d in stage.depends_on if d in failed]
        if blocked:
            log.warning("--- %s PULADO (dependencia falhou: %s)", stage.name, blocked)
            report.append({"stage": stage.name, "status": "pulado", "motivo": str(blocked)})
            failed.add(stage.name)
            continue

        result = run_stage(stage)
        report.append(result)

        if result["status"] == "falha":
            failed.add(stage.name)
            if stage.critical and stop_on_failure:
                log.error("Etapa critica falhou; interrompendo o pipeline.")
                break

    _print_report(report, time.perf_counter() - started, run_id)
    return report


def _print_report(report: list[dict], elapsed: float, run_id: str) -> None:
    log.info("#" * 78)
    log.info("# RESUMO DA EXECUCAO | run_id=%s | tempo total %.1fs", run_id, elapsed)
    log.info("#" * 78)
    icons = {"sucesso": "[OK]  ", "falha": "[FALHA]", "pulado": "[PULO]"}
    for row in report:
        log.info(
            "%s %-26s %6ss  %s",
            icons.get(row["status"], "[?]"),
            row["stage"],
            row.get("duracao_s", "-"),
            row.get("resultado", row.get("erro", "")),
        )
    ok = sum(1 for r in report if r["status"] == "sucesso")
    log.info("Etapas concluidas: %s/%s", ok, len(report))


def main() -> None:
    parser = argparse.ArgumentParser(description="Orquestrador do pipeline do lakehouse.")
    parser.add_argument(
        "--from", dest="start_from", default=None, help="executa a partir desta etapa"
    )
    parser.add_argument(
        "--only", action="append", default=None, help="executa apenas esta etapa (pode repetir)"
    )
    parser.add_argument(
        "--skip", action="append", default=None, help="pula esta etapa (pode repetir)"
    )
    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="nao interrompe o pipeline quando uma etapa critica falha",
    )
    parser.add_argument("--list", action="store_true", help="lista as etapas e sai")
    args = parser.parse_args()

    if args.list:
        print(f"{'ETAPA':<26} {'DEPENDE DE':<52} DESCRICAO")
        for stage in PIPELINE:
            print(f"{stage.name:<26} {', '.join(stage.depends_on) or '-':<52} {stage.description}")
        return

    report = run_pipeline(
        args.start_from, args.only, args.skip, stop_on_failure=not args.continue_on_failure
    )
    if any(r["status"] == "falha" for r in report):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
