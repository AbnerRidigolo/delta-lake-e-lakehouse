"""
Conciliacao entre camadas: provar que nada sumiu no caminho.

O problema que isso resolve
--------------------------
Expectativa por coluna nao ve perda de dados. Uma tabela pode passar em TODAS
as regras de nulo, dominio e faixa — e ainda assim ter perdido 12% das linhas
num join mal escrito. Do ponto de vista das expectativas, a tabela esta
perfeita; ela so esta incompleta.

A conciliacao ataca isso de frente, com duas identidades que precisam fechar:

**1. Conciliacao de linhas**

    linhas da Bronze  ==  linhas da Silver  +  linhas em quarentena

Se a soma nao fecha, existe um caminho no codigo que descarta registro sem
registrar o motivo. Nao importa qual — o pipeline para ate alguem explicar.

**2. Conciliacao financeira**

    soma dos valores na Silver  ==  soma dos valores nas tabelas Gold

Contagem batendo nao garante valor batendo: um `explode` acidental duplica
dinheiro sem mudar a contagem de chaves. Em dado financeiro, conciliar valor e
obrigatorio — e a mesma logica de fechamento contabil.

Este e o tipo de checagem que uma auditoria pede e que quase nenhum pipeline tem.

Execucao:
    python -m src.quality.reconciliation
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import PIPELINE_RUN_ID, table_exists, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

RECONCILIATION_TABLE = "reconciliation_results"


class ReconciliationError(RuntimeError):
    """Levantada quando uma identidade de conciliacao nao fecha."""


@dataclass
class ReconciliationCheck:
    """Resultado de uma identidade conciliada."""

    name: str
    description: str
    expected: float
    actual: float
    tolerance: float
    unit: str  # "linhas" | "R$"

    @property
    def difference(self) -> float:
        return self.actual - self.expected

    @property
    def relative_difference(self) -> float:
        return abs(self.difference) / max(abs(self.expected), 1.0)

    @property
    def passed(self) -> bool:
        if self.unit == "R$":
            return abs(self.difference) <= self.tolerance
        return self.relative_difference <= self.tolerance


# ---------------------------------------------------------------------------
# Identidade 1: contagem de linhas Bronze -> Silver + quarentena
# ---------------------------------------------------------------------------


def _aggregate_tolerance(base: float, groups: int) -> float:
    """Tolerancia de conciliacao para um total somado a partir de grupos arredondados.

    Cada grupo arredondado para centavos erra no maximo meio centavo, e o erro
    dos dois lados da identidade se acumula - dai o fator 2. E o limite teorico,
    nao um chute: uma diferenca maior que isso nao e arredondamento, e defeito.
    """
    return max(base, round(0.005 * 2 * max(groups, 1), 2))


def _count(spark: SparkSession, path: str, distinct_column: str | None = None) -> int:
    if not table_exists(spark, path):
        return 0
    df = spark.read.format("delta").load(path)
    return df.select(distinct_column).distinct().count() if distinct_column else df.count()


def _producing_run_id(spark: SparkSession, path: str) -> str | None:
    """Descobre qual execucao do pipeline gravou a versao atual da tabela.

    Le o `userMetadata` do historico de commits do Delta - carimbado por
    `write_delta` em toda escrita. Isso importa para reexecucoes parciais: se
    alguem roda `--from feature_store`, a Silver e a quarentena continuam sendo
    de uma execucao ANTERIOR, e conciliar contra o run_id atual acusaria uma
    perda de dados que nao existe.

    E um bom exemplo de por que o historico do Delta vale como metadado de
    verdade, e nao so como curiosidade: aqui ele responde uma pergunta que os
    dados da tabela sozinhos nao respondem.
    """
    import json

    if not table_exists(spark, path):
        return None
    try:
        from delta.tables import DeltaTable

        historico = DeltaTable.forPath(spark, path).history(10).select("userMetadata").collect()
        for linha in historico:
            bruto = linha["userMetadata"]
            if bruto:
                run_id = json.loads(bruto).get("pipeline_run_id")
                if run_id:
                    return str(run_id)
    except Exception as exc:  # pragma: no cover - historico indisponivel
        log.warning("Nao consegui ler o historico de %s: %s", path, exc)
    return None


def reconcile_row_counts(spark: SparkSession) -> list[ReconciliationCheck]:
    """Verifica, para cada tabela, se Bronze == Silver + quarentena.

    Usamos a contagem de chaves DISTINTAS na Bronze porque ela e append-only e
    recebe o mesmo registro pelo batch e pelo streaming: a deduplicacao da
    Silver e legitima e nao pode ser contada como perda.
    """
    cfg = get_settings()
    tolerance = float(cfg.raw["quality"]["reconciliation_tolerance"])
    checks: list[ReconciliationCheck] = []

    tabelas = {
        "customers": "customer_id",
        "merchants": "merchant_id",
        "transactions": "transaction_id",
        "credit_contracts": "contract_id",
    }

    for tabela, chave in tabelas.items():
        bronze = _count(spark, cfg.table_path("bronze", tabela), distinct_column=chave)
        if bronze == 0:
            continue

        silver_path = cfg.table_path("silver", tabela)
        silver = _count(spark, silver_path)

        # A quarentena e append-only entre execucoes: contamos apenas a execucao
        # que de fato produziu a versao atual da Silver (que pode nao ser a atual,
        # em reexecucoes parciais).
        run_produtor = _producing_run_id(spark, silver_path) or PIPELINE_RUN_ID
        quarentena_path = str(cfg.layer_path("silver") / "_quarantine" / tabela)
        quarentena = 0
        if table_exists(spark, quarentena_path):
            quarentena = (
                spark.read.format("delta")
                .load(quarentena_path)
                .where(F.col("_pipeline_run_id") == F.lit(run_produtor))
                .select(chave)
                .distinct()
                .count()
            )

        checks.append(
            ReconciliationCheck(
                name=f"linhas::{tabela}",
                description=(
                    f"bronze({bronze}) == silver({silver}) + quarentena({quarentena}) "
                    f"[run produtor: {run_produtor}]"
                ),
                expected=float(bronze),
                actual=float(silver + quarentena),
                tolerance=tolerance,
                unit="linhas",
            )
        )

    return checks


# ---------------------------------------------------------------------------
# Identidade 2: valores financeiros Silver -> Gold
# ---------------------------------------------------------------------------


def reconcile_amounts(spark: SparkSession) -> list[ReconciliationCheck]:
    """Verifica se o dinheiro que entrou na Silver aparece integro na Gold."""
    cfg = get_settings()
    tolerance = float(cfg.raw["quality"]["reconciliation_amount_tolerance"])
    checks: list[ReconciliationCheck] = []

    silver_path = cfg.table_path("silver", "transactions")
    if not table_exists(spark, silver_path):
        return checks

    silver = spark.read.format("delta").load(silver_path)
    totais = silver.agg(
        F.sum(F.when(F.col("status") == "approved", F.col("amount")).otherwise(0.0)).alias("tpv"),
        F.sum("mdr_revenue").alias("mdr"),
        F.count("*").alias("linhas"),
    ).collect()[0]

    # --- TPV: silver.transactions == gold.financial_kpis_daily ---------------
    kpis_path = cfg.table_path("gold", "financial_kpis_daily")
    if table_exists(spark, kpis_path):
        kpis_df = spark.read.format("delta").load(kpis_path)
        kpis = kpis_df.agg(F.sum("tpv").alias("tpv"), F.sum("revenue_mdr").alias("mdr")).collect()[
            0
        ]
        # A Gold arredonda para centavos DENTRO de cada grupo (um por dia), entao
        # somar os grupos acumula ate meio centavo por grupo. Uma tolerancia fixa
        # de R$ 0,01 daria falso positivo em qualquer serie longa - e um alarme
        # que dispara sozinho e pior do que nenhum alarme, porque treina o time
        # a ignora-lo. A tolerancia acompanha o numero de grupos.
        grupos_dia = kpis_df.count()
        tolerancia_agregada = _aggregate_tolerance(tolerance, grupos_dia)

        checks.append(
            ReconciliationCheck(
                name="valor::tpv_silver_vs_gold",
                description="TPV aprovado na silver.transactions == TPV na gold.financial_kpis_daily",
                expected=round(float(totais["tpv"] or 0), 2),
                actual=round(float(kpis["tpv"] or 0), 2),
                tolerance=tolerancia_agregada,
                unit="R$",
            )
        )
        checks.append(
            ReconciliationCheck(
                name="valor::mdr_silver_vs_gold",
                description="Receita de MDR na silver == receita na gold.financial_kpis_daily",
                expected=round(float(totais["mdr"] or 0), 2),
                actual=round(float(kpis["mdr"] or 0), 2),
                tolerance=tolerancia_agregada,
                unit="R$",
            )
        )

    # --- Contagem: toda transacao da Silver foi pontuada pelo antifraude -----
    sinais_path = cfg.table_path("gold", "transaction_fraud_signals")
    if table_exists(spark, sinais_path):
        sinais = spark.read.format("delta").load(sinais_path).count()
        checks.append(
            ReconciliationCheck(
                name="linhas::silver_vs_fraud_signals",
                description="toda transacao da silver recebeu score na gold.transaction_fraud_signals",
                expected=float(totais["linhas"]),
                actual=float(sinais),
                tolerance=0.0,
                unit="linhas",
            )
        )

    # --- Exposicao de credito: silver == gold.credit_risk_portfolio ----------
    contratos_path = cfg.table_path("silver", "credit_contracts")
    carteira_path = cfg.table_path("gold", "credit_risk_portfolio")
    if table_exists(spark, contratos_path) and table_exists(spark, carteira_path):
        ead_silver = (
            spark.read.format("delta")
            .load(contratos_path)
            .agg(F.sum("exposure_at_default"))
            .collect()[0][0]
            or 0.0
        )
        carteira = spark.read.format("delta").load(carteira_path)
        ead_gold = carteira.agg(F.sum("ead")).collect()[0][0] or 0.0
        checks.append(
            ReconciliationCheck(
                name="valor::ead_silver_vs_gold",
                description="EAD dos contratos na silver == EAD agregado na gold.credit_risk_portfolio",
                expected=round(float(ead_silver), 2),
                actual=round(float(ead_gold), 2),
                # Um grupo por (safra x produto x faixa de risco).
                tolerance=_aggregate_tolerance(tolerance, carteira.count()),
                unit="R$",
            )
        )

    # --- Feature store nao pode perder nem inventar transacao ---------------
    features_path = cfg.table_path("feature_store", "transaction_features")
    if table_exists(spark, features_path):
        features = spark.read.format("delta").load(features_path).count()
        checks.append(
            ReconciliationCheck(
                name="linhas::fraud_signals_vs_feature_store",
                description="a feature store cobre exatamente as transacoes pontuadas",
                expected=float(totais["linhas"]),
                actual=float(features),
                tolerance=0.0,
                unit="linhas",
            )
        )

    return checks


# ---------------------------------------------------------------------------
# Execucao
# ---------------------------------------------------------------------------


def run(raise_on_failure: bool = True) -> list[ReconciliationCheck]:
    """Executa todas as conciliacoes e persiste o resultado em Delta."""
    cfg = get_settings()
    spark = get_spark("quality_reconciliation")

    with log_stage(log, "quality.reconciliation"):
        checks = reconcile_row_counts(spark) + reconcile_amounts(spark)

        for check in checks:
            status = "OK  " if check.passed else "FALHA"
            log.info(
                "CONCILIACAO %s | %-38s | esperado=%s obtido=%s (dif=%s %s)",
                status,
                check.name,
                round(check.expected, 2),
                round(check.actual, 2),
                round(check.difference, 2),
                check.unit,
            )
            if not check.passed:
                log.error("           -> %s", check.description)

        if checks:
            registros = [
                {
                    "run_id": PIPELINE_RUN_ID,
                    "checked_at": datetime.now(timezone.utc).replace(tzinfo=None),
                    "check_name": c.name,
                    "description": c.description,
                    "expected": float(c.expected),
                    "actual": float(c.actual),
                    "difference": float(round(c.difference, 4)),
                    "relative_difference": float(round(c.relative_difference, 6)),
                    "unit": c.unit,
                    "passed": bool(c.passed),
                }
                for c in checks
            ]

            write_delta(
                spark.createDataFrame(registros),
                cfg.table_path("quality", RECONCILIATION_TABLE),
                mode="append",
                merge_schema=True,
                comment="conciliacao entre camadas",
            )

        falhas = [c for c in checks if not c.passed]
        if falhas and raise_on_failure:
            nomes = ", ".join(c.name for c in falhas)
            raise ReconciliationError(
                f"Conciliacao nao fechou em: {nomes}. "
                "Ha perda (ou duplicacao) de dados entre camadas."
            )

        log.info(
            "Conciliacao concluida: %s/%s identidades fecharam",
            len(checks) - len(falhas),
            len(checks),
        )

    return checks


if __name__ == "__main__":
    run()
