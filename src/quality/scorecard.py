"""
Scorecard de qualidade: transformar centenas de checagens em uma decisao.

O problema
----------
O pipeline executa expectativas, conciliacoes, contratos de schema e deteccao
de drift. Isso vira facilmente 80 resultados por execucao — que ninguem le. Um
log que ninguem le nao e governanca; e ruido com carimbo de qualidade.

O scorecard consolida tudo em:

* uma **nota por tabela** (0 a 100), ponderada por severidade;
* uma **nota por camada** e uma nota geral do lakehouse;
* um **relatorio em Markdown** que vai para o `$GITHUB_STEP_SUMMARY` e aparece
  direto na aba do Pull Request;
* um **portao objetivo**: abaixo da nota minima configurada, o build falha.

Ponderacao: uma falha `error` custa 3x uma falha `warn`, porque nem toda
violacao tem o mesmo peso — e um scorecard que trata tudo igual vira um numero
sem significado.

Execucao:
    python -m src.quality.scorecard
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import PIPELINE_RUN_ID, table_exists, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

SCORECARD_TABLE = "scorecard"
PESO_ERROR = 3.0
PESO_WARN = 1.0


class QualityGateFailure(RuntimeError):
    """Levantada quando a nota do scorecard fica abaixo do minimo exigido."""


@dataclass
class TableScore:
    table: str
    layer: str
    checks: int
    failures_error: int
    failures_warn: int
    score: float

    @property
    def status(self) -> str:
        if self.failures_error:
            return "reprovado"
        if self.failures_warn:
            return "atencao"
        return "aprovado"


def _score(checks: int, erros: int, avisos: int) -> float:
    """Nota 0-100 penalizada por severidade.

    A penalidade e proporcional ao numero de checagens da tabela: uma falha em
    uma tabela com 3 regras pesa mais do que em uma com 12, o que e justo —
    cobertura maior deveria dar mais folga, nao menos.
    """
    if checks == 0:
        return 100.0
    penalidade = (erros * PESO_ERROR + avisos * PESO_WARN) / (checks * PESO_ERROR)
    return round(max(0.0, 100.0 * (1 - penalidade)), 1)


def collect_scores(spark: SparkSession, run_id: str | None = None) -> list[TableScore]:
    """Consolida os resultados de qualidade da execucao em notas por tabela."""
    cfg = get_settings()
    run_id = run_id or PIPELINE_RUN_ID

    dq_path = cfg.table_path("quality", "dq_results")
    if not table_exists(spark, dq_path):
        return []

    df = spark.read.format("delta").load(dq_path).where(F.col("run_id") == F.lit(run_id))

    agregado = (
        df.groupBy("dataset")
        .agg(
            F.count("*").alias("checks"),
            F.sum(
                F.when((~F.col("passed")) & (F.col("severity") == "error"), 1).otherwise(0)
            ).alias("erros"),
            F.sum(F.when((~F.col("passed")) & (F.col("severity") == "warn"), 1).otherwise(0)).alias(
                "avisos"
            ),
        )
        .collect()
    )

    scores: list[TableScore] = []
    for linha in agregado:
        dataset = linha["dataset"]
        camada = dataset.split(".")[0] if "." in dataset else "outros"
        scores.append(
            TableScore(
                table=dataset,
                layer=camada,
                checks=int(linha["checks"]),
                failures_error=int(linha["erros"]),
                failures_warn=int(linha["avisos"]),
                score=_score(int(linha["checks"]), int(linha["erros"]), int(linha["avisos"])),
            )
        )

    return sorted(scores, key=lambda s: (s.layer, s.table))


def collect_extra_signals(spark: SparkSession, run_id: str | None = None) -> dict[str, dict]:
    """Recolhe os sinais que nao vivem em `dq_results`: conciliacao e drift."""
    cfg = get_settings()
    run_id = run_id or PIPELINE_RUN_ID
    sinais: dict[str, dict] = {}

    recon_path = cfg.table_path("quality", "reconciliation_results")
    if table_exists(spark, recon_path):
        df = spark.read.format("delta").load(recon_path).where(F.col("run_id") == F.lit(run_id))
        total = df.count()
        if total:
            falhas = df.where(~F.col("passed")).count()
            sinais["conciliacao"] = {
                "total": total,
                "falhas": falhas,
                "detalhes": [
                    r["check_name"]
                    for r in df.where(~F.col("passed")).select("check_name").collect()
                ],
            }

    drift_path = cfg.table_path("quality", "drift_results")
    if table_exists(spark, drift_path):
        df = spark.read.format("delta").load(drift_path).where(F.col("run_id") == F.lit(run_id))
        total = df.count()
        if total:
            relevantes = df.where(F.col("severity") == "relevante").count()
            sinais["drift"] = {
                "total": total,
                "relevantes": relevantes,
                "detalhes": [
                    f"{r['kind']}:{r['target']} ({r['metric']})"
                    for r in df.where(F.col("severity") != "estavel")
                    .select("kind", "target", "metric")
                    .limit(8)
                    .collect()
                ],
            }

    return sinais


def render_markdown(
    scores: list[TableScore], sinais: dict[str, dict], nota_geral: float, minimo: float
) -> str:
    """Gera o relatorio do scorecard (usado no resumo do GitHub Actions)."""
    cfg = get_settings()
    icone = {"aprovado": "✅", "atencao": "⚠️", "reprovado": "❌"}
    aprovado = nota_geral >= minimo

    linhas: list[str] = [
        f"# {'✅' if aprovado else '❌'} Scorecard de qualidade de dados",
        "",
        f"**Nota geral: {nota_geral:.1f}/100** (minimo exigido: {minimo:.1f}) · "
        f"perfil `{cfg.profile}` · run `{PIPELINE_RUN_ID}`",
        "",
    ]

    # --- nota por camada ----------------------------------------------------
    por_camada: dict[str, list[TableScore]] = {}
    for s in scores:
        por_camada.setdefault(s.layer, []).append(s)

    linhas += [
        "## Nota por camada",
        "",
        "| Camada | Tabelas | Checagens | Falhas (erro) | Falhas (aviso) | Nota |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for camada, itens in sorted(por_camada.items()):
        checks = sum(i.checks for i in itens)
        erros = sum(i.failures_error for i in itens)
        avisos = sum(i.failures_warn for i in itens)
        nota = _score(checks, erros, avisos)
        linhas.append(
            f"| `{camada}` | {len(itens)} | {checks} | {erros} | {avisos} | " f"**{nota:.1f}** |"
        )

    # --- detalhe por tabela -------------------------------------------------
    linhas += [
        "",
        "## Detalhe por tabela",
        "",
        "| | Tabela | Checagens | Erro | Aviso | Nota |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for s in scores:
        linhas.append(
            f"| {icone[s.status]} | `{s.table}` | {s.checks} | {s.failures_error} | "
            f"{s.failures_warn} | {s.score:.1f} |"
        )

    # --- conciliacao e drift ------------------------------------------------
    if "conciliacao" in sinais:
        c = sinais["conciliacao"]
        ok = c["falhas"] == 0
        linhas += [
            "",
            "## Conciliacao entre camadas",
            "",
            f"{'✅' if ok else '❌'} {c['total'] - c['falhas']}/{c['total']} "
            "identidades fecharam.",
        ]
        if not ok:
            linhas.append("")
            linhas += [f"- ❌ `{d}`" for d in c["detalhes"]]

    if "drift" in sinais:
        d = sinais["drift"]
        ok = d["relevantes"] == 0
        linhas += [
            "",
            "## Drift",
            "",
            f"{'✅' if ok else '⚠️'} {d['relevantes']} sinal(is) de drift relevante "
            f"em {d['total']} avaliacoes.",
        ]
        if d["detalhes"]:
            linhas.append("")
            linhas += [f"- `{item}`" for item in d["detalhes"]]

    linhas += [
        "",
        "---",
        "",
        "*Gerado por `src/quality/scorecard.py` a partir das tabelas "
        "`quality.dq_results`, `quality.reconciliation_results` e "
        "`quality.drift_results`.*",
        "",
    ]
    return "\n".join(linhas)


def publish_to_github_summary(markdown: str) -> bool:
    """Publica o scorecard no resumo do job do GitHub Actions, se disponivel."""
    caminho = os.getenv("GITHUB_STEP_SUMMARY")
    if not caminho:
        return False
    with open(caminho, "a", encoding="utf-8") as handle:
        handle.write(markdown + "\n")
    log.info("Scorecard publicado no resumo do GitHub Actions.")
    return True


def run(raise_on_gate: bool = True) -> dict[str, object]:
    cfg = get_settings()
    spark = get_spark("quality_scorecard")
    minimo = float(cfg.raw["quality"]["scorecard_min_score"])

    with log_stage(log, "quality.scorecard"):
        scores = collect_scores(spark)
        if not scores:
            log.warning("Nenhum resultado de qualidade nesta execucao - scorecard vazio.")
            return {"nota_geral": 100.0, "tabelas": 0}

        sinais = collect_extra_signals(spark)

        checks = sum(s.checks for s in scores)
        erros = sum(s.failures_error for s in scores)
        avisos = sum(s.failures_warn for s in scores)
        nota_geral = _score(checks, erros, avisos)

        # Conciliacao e drift entram como penalidade direta: sao falhas
        # estruturais, nao violacoes pontuais de uma coluna.
        nota_geral -= 10.0 * sinais.get("conciliacao", {}).get("falhas", 0)
        nota_geral -= 5.0 * sinais.get("drift", {}).get("relevantes", 0)
        nota_geral = round(max(nota_geral, 0.0), 1)

        for s in scores:
            log.info(
                "SCORECARD %-9s | %-34s | %3s checagens | erro=%s aviso=%s | nota=%.1f",
                s.status,
                s.table,
                s.checks,
                s.failures_error,
                s.failures_warn,
                s.score,
            )
        log.info("SCORECARD GERAL: %.1f/100 (minimo %.1f)", nota_geral, minimo)

        markdown = render_markdown(scores, sinais, nota_geral, minimo)

        reports_dir = cfg.layer_path("reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        destino = reports_dir / f"scorecard_qualidade_{date.today().isoformat()}.md"
        destino.write_text(markdown, encoding="utf-8")
        publish_to_github_summary(markdown)

        registros = [
            {
                "run_id": PIPELINE_RUN_ID,
                "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "layer": s.layer,
                "table": s.table,
                "checks": s.checks,
                "failures_error": s.failures_error,
                "failures_warn": s.failures_warn,
                "score": float(s.score),
                "status": s.status,
                "overall_score": float(nota_geral),
            }
            for s in scores
        ]
        write_delta(
            spark.createDataFrame(registros),
            cfg.table_path("quality", SCORECARD_TABLE),
            mode="append",
            merge_schema=True,
            comment="scorecard de qualidade",
        )

        if nota_geral < minimo and raise_on_gate:
            raise QualityGateFailure(
                f"Portao de qualidade reprovado: nota {nota_geral:.1f} < minimo {minimo:.1f}. "
                f"Relatorio em {destino}."
            )

        return {
            "nota_geral": nota_geral,
            "tabelas": len(scores),
            "checagens": checks,
            "erros": erros,
            "avisos": avisos,
            "relatorio": str(destino),
        }


if __name__ == "__main__":
    run()
