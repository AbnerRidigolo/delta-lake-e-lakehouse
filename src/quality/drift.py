"""
Deteccao de drift: quando o dado muda sem ninguem mudar o codigo.

Dois tipos de drift, dois problemas diferentes
----------------------------------------------

**1. Drift de volume** — a tabela veio com metade das linhas de sempre.
Nenhuma expectativa por coluna pega isso: as linhas que chegaram estao
perfeitas. Comparamos o volume atual contra a mediana das execucoes anteriores
(registradas na propria tabela de qualidade). Usamos mediana, e nao media,
porque uma unica execucao anomala envenena a media e mascara as seguintes.

**2. Drift de distribuicao (PSI)** — o volume esta igual, as colunas estao
validas, mas a POPULACAO mudou. Um modelo treinado no perfil antigo comeca a
errar sem que nada quebre. O Population Stability Index e o padrao de mercado
em risco de credito:

    PSI = SUM( (%atual - %referencia) * ln(%atual / %referencia) )

    PSI < 0.10          populacao estavel
    0.10 <= PSI < 0.25  mudanca moderada, investigar
    PSI >= 0.25         mudanca relevante, considerar retreinar o modelo

Aplicamos PSI sobre as features da feature store, comparando o periodo de
treino com o periodo mais recente — exatamente a comparacao que responde
"o modelo ainda vale?".

Execucao:
    python -m src.quality.drift
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import PIPELINE_RUN_ID, table_exists, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

DRIFT_TABLE = "drift_results"
DQ_TABLE = "dq_results"

STABLE = "estavel"
MODERATE = "moderado"
RELEVANT = "relevante"


class DriftError(RuntimeError):
    """Levantada quando um drift acima do limiar de erro e detectado."""


@dataclass
class DriftResult:
    kind: str  # "volume" | "psi"
    target: str  # tabela ou feature avaliada
    metric: float
    severity: str  # estavel | moderado | relevante
    detail: str

    @property
    def passed(self) -> bool:
        return self.severity != RELEVANT


# ---------------------------------------------------------------------------
# 1) Drift de volume
# ---------------------------------------------------------------------------


def detect_volume_drift(spark: SparkSession) -> list[DriftResult]:
    """Compara o volume atual de cada tabela com a mediana historica.

    O historico sai da propria tabela `quality.dq_results`, que ja registra
    `total_rows` por dataset a cada execucao. Nao e preciso manter um segundo
    lugar de verdade — a tabela de qualidade e o baseline.
    """
    cfg = get_settings()
    quality_cfg = cfg.raw["quality"]
    max_deviation = float(quality_cfg["volume_drift_max_deviation"])
    min_history = int(quality_cfg["volume_drift_min_history"])

    dq_path = cfg.table_path("quality", DQ_TABLE)
    if not table_exists(spark, dq_path):
        log.info("Sem historico de qualidade ainda: drift de volume nao avaliado.")
        return []

    historico = (
        spark.read.format("delta")
        .load(dq_path)
        .where(F.col("expectation") == "row_count_min")
        .select("run_id", "dataset", "total_rows", "checked_at")
        .toPandas()
    )
    if historico.empty:
        return []

    resultados: list[DriftResult] = []
    atual_run = PIPELINE_RUN_ID

    for dataset, grupo in historico.groupby("dataset"):
        corrente = grupo[grupo["run_id"] == atual_run]
        anterior = grupo[grupo["run_id"] != atual_run]

        if corrente.empty or len(anterior) < min_history:
            continue

        volume_atual = float(corrente["total_rows"].iloc[-1])
        mediana = float(anterior["total_rows"].median())
        if mediana <= 0:
            continue

        desvio = (volume_atual - mediana) / mediana
        severidade = RELEVANT if abs(desvio) > max_deviation else STABLE
        direcao = "acima" if desvio > 0 else "abaixo"

        resultados.append(
            DriftResult(
                kind="volume",
                target=dataset,
                metric=round(desvio, 4),
                severity=severidade,
                detail=(
                    f"{int(volume_atual)} linhas, {abs(desvio) * 100:.1f}% {direcao} da mediana "
                    f"historica ({int(mediana)} linhas em {len(anterior)} execucoes)"
                ),
            )
        )

    return resultados


# ---------------------------------------------------------------------------
# 2) Drift de distribuicao (PSI)
# ---------------------------------------------------------------------------


def population_stability_index(
    reference: np.ndarray, current: np.ndarray, buckets: int = 10
) -> float:
    """Calcula o PSI entre duas amostras numericas.

    Os cortes vem dos quantis da amostra de REFERENCIA (nao da atual): o
    objetivo e medir quanto a populacao nova se desloca em relacao a base
    conhecida. Cortes recalculados na amostra atual esconderiam o proprio
    deslocamento que queremos medir.
    """
    reference = reference[~np.isnan(reference)]
    current = current[~np.isnan(current)]
    if len(reference) < 50 or len(current) < 50:
        return 0.0

    cortes = np.unique(np.quantile(reference, np.linspace(0, 1, buckets + 1)))
    if len(cortes) < 3:
        return 0.0
    cortes[0], cortes[-1] = -np.inf, np.inf

    ref_freq, _ = np.histogram(reference, bins=cortes)
    cur_freq, _ = np.histogram(current, bins=cortes)

    ref_pct = ref_freq / max(ref_freq.sum(), 1)
    cur_pct = cur_freq / max(cur_freq.sum(), 1)

    # Piso pequeno evita divisao por zero e log(0) em buckets vazios.
    epsilon = 1e-6
    ref_pct = np.clip(ref_pct, epsilon, None)
    cur_pct = np.clip(cur_pct, epsilon, None)

    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def detect_feature_drift(spark: SparkSession, split_ratio: float = 0.75) -> list[DriftResult]:
    """Mede o PSI das features entre o periodo de treino e o periodo recente.

    E a mesma divisao temporal usada no treino do modelo: se as features do
    periodo de teste ja divergem das de treino, o modelo nasce desatualizado.
    """
    cfg = get_settings()
    quality_cfg = cfg.raw["quality"]
    warn = float(quality_cfg["psi_warn_threshold"])
    error = float(quality_cfg["psi_error_threshold"])
    buckets = int(quality_cfg["psi_buckets"])

    features_path = cfg.table_path("feature_store", "transaction_features")
    if not table_exists(spark, features_path):
        return []

    df = spark.read.format("delta").load(features_path)
    colunas = [c for c in df.columns if c.startswith("tf_")]
    numericas = [
        c
        for c in colunas
        if df.schema[c].dataType.simpleString() in ("double", "int", "bigint", "float")
    ]
    if not numericas:
        return []

    pdf = df.select("event_date", *numericas).toPandas().sort_values("event_date")
    if len(pdf) < 200:
        log.info("Amostra pequena demais para PSI (%s linhas).", len(pdf))
        return []

    corte = int(len(pdf) * split_ratio)
    referencia, atual = pdf.iloc[:corte], pdf.iloc[corte:]

    resultados: list[DriftResult] = []
    for coluna in numericas:
        psi = population_stability_index(
            referencia[coluna].astype("float64").to_numpy(),
            atual[coluna].astype("float64").to_numpy(),
            buckets=buckets,
        )
        severidade = RELEVANT if psi >= error else MODERATE if psi >= warn else STABLE
        if severidade == STABLE:
            continue  # so reportamos o que merece atencao

        resultados.append(
            DriftResult(
                kind="psi",
                target=coluna,
                metric=round(psi, 4),
                severity=severidade,
                detail=(
                    f"PSI={psi:.4f} entre o periodo de treino ({len(referencia)} linhas) e o "
                    f"periodo recente ({len(atual)} linhas)"
                ),
            )
        )

    return sorted(resultados, key=lambda r: r.metric, reverse=True)


# ---------------------------------------------------------------------------
# Execucao
# ---------------------------------------------------------------------------


def run(raise_on_error: bool = True) -> list[DriftResult]:
    cfg = get_settings()
    spark = get_spark("quality_drift")
    severidade_volume = str(cfg.raw["quality"].get("volume_drift_severity", "error"))

    with log_stage(log, "quality.drift"):
        volume = detect_volume_drift(spark)
        features = detect_feature_drift(spark)
        resultados = volume + features

        for r in resultados:
            nivel = log.error if r.severity == RELEVANT else log.warning
            nivel("DRIFT %-6s | %-9s | %-34s | %s", r.severity.upper(), r.kind, r.target, r.detail)

        if not resultados:
            log.info("Nenhum drift relevante detectado.")

        if resultados:
            registros = [
                {
                    "run_id": PIPELINE_RUN_ID,
                    "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "kind": r.kind,
                    "target": r.target,
                    "metric": float(r.metric),
                    "severity": r.severity,
                    "detail": r.detail,
                    "passed": bool(r.passed),
                }
                for r in resultados
            ]
            write_delta(
                spark.createDataFrame(registros),
                cfg.table_path("quality", DRIFT_TABLE),
                mode="append",
                merge_schema=True,
                comment="deteccao de drift",
            )

        # --- Politica de bloqueio -------------------------------------------
        # Volume: no CI a base historica e sempre nova, entao vira apenas aviso.
        volume_bloqueante = [
            r
            for r in resultados
            if r.kind == "volume" and r.severity == RELEVANT and severidade_volume != "warn"
        ]
        # PSI: uma feature deslocando e esperado (populacao muda, base amadurece).
        # Varias ao mesmo tempo indica mudanca estrutural - ai sim bloqueia.
        psi_bloqueante = [r for r in resultados if r.kind == "psi" and r.severity == RELEVANT]
        limite_psi = int(cfg.raw["quality"].get("psi_max_drifting_features", 2))

        if psi_bloqueante:
            log.warning(
                "PSI relevante em %s feature(s) (limite para bloqueio: %s): %s",
                len(psi_bloqueante),
                limite_psi,
                ", ".join(r.target for r in psi_bloqueante),
            )

        bloqueantes = list(volume_bloqueante)
        if len(psi_bloqueante) > limite_psi:
            bloqueantes += psi_bloqueante

        if bloqueantes and raise_on_error:
            alvos = ", ".join(f"{r.kind}:{r.target}" for r in bloqueantes)
            raise DriftError(f"Drift relevante detectado em -> {alvos}")

    return resultados


if __name__ == "__main__":
    run()
