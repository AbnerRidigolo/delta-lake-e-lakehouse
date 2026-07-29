"""
Tipos adicionais de expectativa, complementando `src/common/data_quality.py`.

O motor basico cobre as regras por coluna (nulo, unico, dominio, faixa, regex,
volume, frescor). Aqui entram as que precisam olhar mais de uma coluna, mais de
uma tabela, ou a distribuicao inteira:

* `composite_unique`      - chave composta (grao da tabela), nao so a PK
* `referential_integrity` - a FK existe mesmo na tabela pai
* `non_negative`          - atalho para valores monetarios
* `mean_between`          - a MEDIA mudou, mesmo com todas as linhas validas
* `distinct_count_between`- cardinalidade explodiu ou desabou
* `custom_sql`            - regra de negocio que nao cabe em um predicado simples

As tres ultimas pegam problemas de agregado: cada linha passa individualmente,
mas o conjunto esta errado. E o tipo de defeito que sobrevive a uma bateria de
expectativas por coluna.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.data_quality import SEVERITY_ERROR, SEVERITY_WARN, Expectation, _build_result
from src.common.logging_utils import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Construtores
# ---------------------------------------------------------------------------


def composite_unique(columns: list[str], severity: str = SEVERITY_ERROR) -> Expectation:
    """Garante que a combinacao de colunas identifica uma linha (o grao da tabela).

    `gold.merchant_performance` tem uma linha por (lojista, mes): nenhuma das
    duas colunas e unica sozinha, mas o par precisa ser. Sem esta regra, um join
    duplicando o mes passa despercebido.
    """
    return Expectation(
        name=f"composite_unique::{'+'.join(columns)}",
        kind="composite_unique",
        column=None,
        params={"columns": columns},
        severity=severity,
    )


def referential_integrity(
    column: str,
    parent_table: str,
    parent_column: str,
    severity: str = SEVERITY_ERROR,
    threshold: float = 0.0,
) -> Expectation:
    """Verifica que toda chave estrangeira existe na tabela pai."""
    return Expectation(
        name=f"fk::{column}->{parent_table}.{parent_column}",
        kind="referential_integrity",
        column=column,
        params={"parent_table": parent_table, "parent_column": parent_column},
        severity=severity,
        threshold=threshold,
    )


def non_negative(
    column: str, severity: str = SEVERITY_ERROR, threshold: float = 0.0
) -> Expectation:
    """Atalho para valores monetarios que nunca podem ser negativos."""
    return Expectation(
        name=f"non_negative::{column}",
        kind="non_negative",
        column=column,
        severity=severity,
        threshold=threshold,
    )


def mean_between(
    column: str, minimum: float, maximum: float, severity: str = SEVERITY_WARN
) -> Expectation:
    """A media da coluna precisa cair em uma faixa esperada.

    Pega o caso em que toda linha e valida mas o conjunto mudou: o ticket medio
    saltou de R$ 180 para R$ 1.800 porque a origem passou a mandar centavos como
    reais. Nenhuma regra de faixa por linha detecta isso.
    """
    return Expectation(
        name=f"mean_between::{column}",
        kind="mean_between",
        column=column,
        params={"min": minimum, "max": maximum},
        severity=severity,
    )


def distinct_count_between(
    column: str, minimum: int, maximum: int, severity: str = SEVERITY_WARN
) -> Expectation:
    """A cardinalidade da coluna precisa ficar em uma faixa esperada."""
    return Expectation(
        name=f"distinct_count::{column}",
        kind="distinct_count_between",
        column=column,
        params={"min": minimum, "max": maximum},
        severity=severity,
    )


def custom_sql(
    name: str, condition: str, severity: str = SEVERITY_ERROR, threshold: float = 0.0
) -> Expectation:
    """Regra de negocio arbitraria, escrita como predicado SQL de linha valida.

    Ex.: `custom_sql("parcela_coerente", "installments = 1 OR payment_method = 'credit_card'")`
    """
    return Expectation(
        name=f"sql::{name}",
        kind="custom_sql",
        column=None,
        params={"condition": condition},
        severity=severity,
        threshold=threshold,
    )


# ---------------------------------------------------------------------------
# Avaliacao
# ---------------------------------------------------------------------------


def evaluate(
    spark: SparkSession, df: DataFrame, dataset: str, expectations: list[Expectation]
) -> list[dict]:
    """Avalia as expectativas estendidas e devolve os resultados no formato padrao.

    O formato de retorno e identico ao de `run_quality_checks`, entao os
    resultados podem ser gravados na mesma tabela `quality.dq_results` e entram
    no scorecard sem tratamento especial.
    """
    total = df.count()
    resultados: list[dict] = []

    for exp in expectations:
        violacoes, detalhe = 0, ""

        if exp.kind == "composite_unique":
            colunas = exp.params["columns"]
            distintas = df.select(*colunas).distinct().count()
            violacoes = max(total - distintas, 0)
            detalhe = f"{distintas} combinacoes distintas em {total} linhas"

        elif exp.kind == "referential_integrity":
            pai = spark.read.format("delta").load(exp.params["parent_table"])
            chaves_pai = pai.select(F.col(exp.params["parent_column"]).alias("_pk")).distinct()
            violacoes = (
                df.select(F.col(exp.column).alias("_fk"))
                .where(F.col("_fk").isNotNull())
                .join(chaves_pai, F.col("_fk") == F.col("_pk"), "left_anti")
                .count()
            )
            detalhe = f"{violacoes} chaves sem correspondencia na tabela pai"

        elif exp.kind == "non_negative":
            violacoes = df.where(F.col(exp.column) < 0).count()

        elif exp.kind == "mean_between":
            media = df.agg(F.avg(F.col(exp.column))).collect()[0][0]
            dentro = media is not None and exp.params["min"] <= media <= exp.params["max"]
            violacoes = 0 if dentro else 1
            detalhe = (
                f"media={round(media, 4) if media is not None else 'n/d'} "
                f"(esperado entre {exp.params['min']} e {exp.params['max']})"
            )

        elif exp.kind == "distinct_count_between":
            distintos = df.select(exp.column).distinct().count()
            dentro = exp.params["min"] <= distintos <= exp.params["max"]
            violacoes = 0 if dentro else 1
            detalhe = (
                f"{distintos} valores distintos "
                f"(esperado entre {exp.params['min']} e {exp.params['max']})"
            )

        elif exp.kind == "custom_sql":
            violacoes = df.where(f"NOT ({exp.params['condition']})").count()
            detalhe = exp.params["condition"]

        else:  # pragma: no cover - tipo desconhecido
            log.warning("Tipo de expectativa nao suportado aqui: %s", exp.kind)
            continue

        # Regras de agregado sao binarias: passou ou nao passou.
        if exp.kind in ("mean_between", "distinct_count_between"):
            resultado = _build_result(exp, dataset, total, violacoes, detalhe)
            resultado["passed"] = violacoes == 0
        else:
            resultado = _build_result(exp, dataset, total, violacoes, detalhe)

        resultados.append(resultado)

        status = (
            "PASS"
            if resultado["passed"]
            else ("FAIL" if exp.severity == SEVERITY_ERROR else "WARN")
        )
        log.info(
            "DQ %-4s | %-24s | %-34s | violacoes=%s/%s %s",
            status,
            dataset,
            exp.name,
            violacoes,
            total,
            detalhe,
        )

    return resultados


# ---------------------------------------------------------------------------
# Suites por tabela
# ---------------------------------------------------------------------------


def build_suites(cfg) -> dict:
    """Define as expectativas estendidas de cada tabela.

    Ficam separadas dos jobs de transformacao de proposito: sao regras de
    *contrato* da tabela, revisadas pelo time de dados, nao detalhes de
    implementacao do job que a produz.
    """
    return {
        ("silver", "transactions"): [
            referential_integrity(
                "customer_id", cfg.table_path("silver", "customers"), "customer_id"
            ),
            referential_integrity(
                "merchant_id", cfg.table_path("silver", "merchants"), "merchant_id"
            ),
            non_negative("mdr_revenue"),
            non_negative("chargeback_loss"),
            # Ticket medio fora desta faixa indica erro de escala na origem
            # (centavos lidos como reais, por exemplo).
            mean_between("amount", 20.0, 2000.0),
            distinct_count_between("channel", 3, 10),
            custom_sql(
                "parcelamento_so_em_credito", "installments = 1 OR payment_method = 'credit_card'"
            ),
            custom_sql("mdr_zerado_quando_nao_aprovada", "status = 'approved' OR mdr_revenue = 0"),
        ],
        ("silver", "credit_contracts"): [
            referential_integrity(
                "customer_id", cfg.table_path("silver", "customers"), "customer_id"
            ),
            non_negative("outstanding_balance"),
            non_negative("provision_amount"),
            custom_sql("npl_coerente_com_atraso", "(days_past_due >= 90) = is_npl"),
            custom_sql(
                "saldo_zerado_quando_quitado", "status <> 'settled' OR outstanding_balance = 0"
            ),
        ],
        ("gold", "customer_360"): [
            referential_integrity(
                "customer_id", cfg.table_path("silver", "customers"), "customer_id"
            ),
            non_negative("tpv_total"),
            non_negative("total_exposure"),
            custom_sql("rating_coerente_com_score", "(customer_score >= 850) = (rating = 'AAA')"),
            custom_sql(
                "churn_coerente_com_recencia",
                "is_churn_risk OR days_since_last_transaction IS NULL "
                "OR days_since_last_transaction <= 60",
            ),
        ],
        ("gold", "merchant_performance"): [
            # O grao da tabela: uma linha por lojista por mes.
            composite_unique(["merchant_id", "event_month"]),
            non_negative("tpv"),
            non_negative("mdr_revenue"),
            custom_sql("aprovadas_nao_excedem_total", "txn_approved <= txn_count"),
        ],
        ("gold", "credit_risk_portfolio"): [
            composite_unique(["vintage", "product", "customer_risk_band"]),
            non_negative("expected_loss"),
            custom_sql("npl_nao_excede_exposicao", "npl_balance <= ead + 0.01"),
        ],
        ("gold", "financial_kpis_daily"): [
            non_negative("revenue_total"),
            non_negative("loss_total"),
            custom_sql(
                "resultado_fecha",
                "abs(net_result - (revenue_total - loss_total + recovery_amount)) < 0.05",
            ),
            custom_sql("aprovadas_nao_excedem_total", "txn_approved <= txn_count"),
        ],
        ("feature_store", "transaction_features"): [
            # A regra que protege o modelo: features do mes anterior, nunca do
            # proprio mes. E o mesmo invariante coberto pelos testes, aqui
            # aplicado tambem em producao a cada execucao.
            custom_sql(
                "sem_vazamento_temporal", "feature_month < date_format(event_date, 'yyyy-MM')"
            ),
            custom_sql("rotulo_binario", "is_fraud IN (0, 1)"),
            non_negative("tf_amount"),
        ],
    }


def run_advanced_checks(raise_on_error: bool = True) -> list[dict]:
    """Executa as suites estendidas e grava tudo em `quality.dq_results`."""
    from src.common.config import get_settings
    from src.common.delta_io import table_exists, write_delta
    from src.common.logging_utils import log_stage
    from src.common.spark_session import get_spark

    cfg = get_settings()
    spark = get_spark("quality_advanced_checks")
    todos: list[dict] = []

    with log_stage(log, "quality.advanced_checks"):
        for (camada, tabela), suite in build_suites(cfg).items():
            path = cfg.table_path(camada, tabela)
            if not table_exists(spark, path):
                log.warning("Tabela nao materializada, pulando: %s.%s", camada, tabela)
                continue

            df = spark.read.format("delta").load(path).cache()
            resultados = evaluate(spark, df, f"{camada}.{tabela}", suite)
            df.unpersist()
            todos.extend(resultados)

        if todos:
            write_delta(
                spark.createDataFrame(todos),
                cfg.table_path("quality", "dq_results"),
                mode="append",
                partition_by=["dataset"],
                merge_schema=True,
                comment="expectativas estendidas",
            )

        falhas = [r for r in todos if not r["passed"] and r["severity"] == SEVERITY_ERROR]
        log.info(
            "Expectativas estendidas: %s checagens, %s falhas criticas", len(todos), len(falhas)
        )

        if falhas and raise_on_error:
            from src.common.data_quality import DataQualityError

            nomes = ", ".join(f"{f['dataset']}::{f['expectation']}" for f in falhas)
            raise DataQualityError(f"Expectativas estendidas falharam em: {nomes}")

    return todos
