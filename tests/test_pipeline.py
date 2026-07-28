"""
Testes automatizados do lakehouse.

Escopo: as regras que, se quebrarem, produzem dado errado em silencio. Nao
testamos "o Spark funciona" — testamos as decisoes do projeto:

* as transformacoes de limpeza fazem o que prometem;
* o score de fraude soma os pesos corretos;
* o DAG nao tem ciclo nem dependencia orfa;
* a feature store nao vaza informacao do futuro (o teste mais importante).

Execucao:
    pytest tests/ -v

Os testes que precisam de Spark sao pulados automaticamente se o pipeline ainda
nao tiver sido executado (as tabelas nao existem).
"""

from __future__ import annotations

import pytest

from src.common.config import get_settings


# ===========================================================================
# Testes de configuracao e DAG (rapidos, sem Spark)
# ===========================================================================

def test_configuracao_carrega_e_tem_as_chaves_esperadas():
    cfg = get_settings()
    assert cfg.catalog
    for layer in ["raw", "bronze", "silver", "gold", "feature_store"]:
        assert layer in cfg.raw["paths"], f"camada `{layer}` ausente na configuracao"
    assert 0 < float(cfg.data_generation["fraud_rate"]) < 0.1


def test_caminho_de_tabela_respeita_a_camada():
    cfg = get_settings()
    path = cfg.table_path("gold", "customer_360")
    assert path.endswith("gold/customer_360")


def test_dag_nao_tem_ciclo_e_toda_dependencia_existe():
    from orchestration.run_pipeline import PIPELINE, STAGES_BY_NAME, topological_order

    nomes = {stage.name for stage in PIPELINE}
    for stage in PIPELINE:
        for dependencia in stage.depends_on:
            assert dependencia in nomes, \
                f"etapa `{stage.name}` depende de `{dependencia}`, que nao existe"

    ordem = topological_order(list(PIPELINE))
    assert len(ordem) == len(PIPELINE)

    # Toda dependencia precisa aparecer antes da etapa que a consome.
    posicao = {stage.name: i for i, stage in enumerate(ordem)}
    for stage in PIPELINE:
        for dependencia in stage.depends_on:
            assert posicao[dependencia] < posicao[stage.name], \
                f"`{dependencia}` deveria vir antes de `{stage.name}`"

    assert set(STAGES_BY_NAME) == nomes


def test_pesos_das_regras_de_fraude_sao_coerentes():
    from src.gold.gold_transaction_fraud_signals import RULE_WEIGHTS

    assert all(peso > 0 for peso in RULE_WEIGHTS.values())
    # A regra mais forte sozinha nao pode disparar bloqueio (score >= 70):
    # bloquear alguem por um unico sinal geraria falso positivo demais.
    assert max(RULE_WEIGHTS.values()) < 70
    # E o conjunto todo precisa conseguir chegar ao limiar critico.
    assert sum(RULE_WEIGHTS.values()) >= 70


def test_catalogo_declara_todas_as_camadas():
    import yaml

    cfg = get_settings()
    with open(cfg.project_root / "config" / "catalog.yaml", encoding="utf-8") as handle:
        catalogo = yaml.safe_load(handle)

    camadas = {entrada["layer"] for entrada in catalogo["tables"]}
    assert {"bronze", "silver", "gold", "feature_store"} <= camadas

    for entrada in catalogo["tables"]:
        assert entrada["domain"] in catalogo["domains"], \
            f"dominio desconhecido em {entrada['name']}"
        assert entrada["classification"] in catalogo["classifications"]


# ===========================================================================
# Testes que precisam de Spark
# ===========================================================================

@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")  # noqa: F841
    from src.common.spark_session import get_spark

    return get_spark("pytest")


def _requer_tabela(spark, camada: str, tabela: str):
    """Pula o teste se a tabela ainda nao foi materializada."""
    from src.common.delta_io import table_exists

    caminho = get_settings().table_path(camada, tabela)
    if not table_exists(spark, caminho):
        pytest.skip(f"`{camada}.{tabela}` nao existe - rode `make pipeline` primeiro")
    return spark.read.format("delta").load(caminho)


def test_normalizacao_de_texto_limpa_espacos_caixa_e_tabulacao(spark):
    from pyspark.sql import Row

    from src.common.transforms import normalize_text

    df = spark.createDataFrame([Row(canal="  POS "), Row(canal="Pos\t"), Row(canal="pos")])
    resultado = [linha[0] for linha in df.select(normalize_text("canal")).collect()]
    assert resultado == ["pos", "pos", "pos"]


def test_cast_seguro_devolve_nulo_em_vez_de_quebrar(spark):
    from pyspark.sql import Row

    from src.common.transforms import safe_double, safe_timestamp

    df = spark.createDataFrame([
        Row(valor="12.50", data="2024-07-01 10:00:00"),
        Row(valor="", data="31/02/2024 99:99"),
        Row(valor="nao_e_numero", data=""),
    ])
    valores = [linha[0] for linha in df.select(safe_double("valor")).collect()]
    datas = [linha[0] for linha in df.select(safe_timestamp("data")).collect()]

    assert valores == [12.5, None, None]
    assert datas[0] is not None and datas[1] is None and datas[2] is None


def test_deduplicacao_mantem_o_registro_mais_recente(spark):
    from pyspark.sql import Row
    from pyspark.sql import functions as F

    from src.common.transforms import deduplicate

    df = spark.createDataFrame([
        Row(id="A", versao=1, valor=10.0),
        Row(id="A", versao=3, valor=30.0),
        Row(id="A", versao=2, valor=20.0),
        Row(id="B", versao=1, valor=99.0),
    ])
    resultado = {linha["id"]: linha["valor"]
                 for linha in deduplicate(df, ["id"], [F.col("versao").desc()]).collect()}
    assert resultado == {"A": 30.0, "B": 99.0}


def test_mascaramento_de_pii_nao_expoe_o_dado_original(spark):
    from pyspark.sql import Row

    from src.common.transforms import hash_pii, mask_document, mask_email

    df = spark.createDataFrame([Row(cpf="123.456.789-01", email="ana.silva@exemplo.com.br")])
    linha = df.select(
        mask_document("cpf").alias("cpf"),
        mask_email("email").alias("email"),
        hash_pii("cpf").alias("hash"),
    ).collect()[0]

    assert linha["cpf"] == "***.456.789-**"
    assert linha["email"] == "a***@exemplo.com.br"
    assert len(linha["hash"]) == 64 and "123" not in linha["hash"]


def test_silver_transactions_nao_tem_duplicata_nem_valor_invalido(spark):
    from pyspark.sql import functions as F

    df = _requer_tabela(spark, "silver", "transactions")
    total = df.count()

    assert total > 0
    assert df.select("transaction_id").distinct().count() == total, "ha transaction_id duplicado"
    assert df.where(F.col("amount") <= 0).count() == 0, "ha valor nao positivo"
    assert df.where(F.col("event_ts").isNull()).count() == 0, "ha timestamp nulo"
    assert df.where(F.col("customer_key").isNull()).count() == 0, "ha chave orfa"


def test_gold_customer_360_tem_uma_linha_por_cliente_e_score_valido(spark):
    from pyspark.sql import functions as F

    df = _requer_tabela(spark, "gold", "customer_360")
    total = df.count()

    assert df.select("customer_id").distinct().count() == total
    assert df.where(~F.col("customer_score").between(0, 1000)).count() == 0
    assert df.where(F.col("rating").isNull()).count() == 0


def test_score_de_fraude_bate_com_a_soma_dos_pesos_das_regras(spark):
    from pyspark.sql import functions as F

    from src.gold.gold_transaction_fraud_signals import RULE_WEIGHTS

    df = _requer_tabela(spark, "gold", "transaction_fraud_signals")

    esperado = F.lit(0)
    for regra, peso in RULE_WEIGHTS.items():
        esperado = esperado + F.when(F.col(regra), F.lit(peso)).otherwise(F.lit(0))

    divergencias = df.where(
        F.least(esperado, F.lit(100)) != F.col("fraud_score_rule")
    ).count()
    assert divergencias == 0, "o score gravado nao corresponde as regras disparadas"


def test_feature_store_nao_vaza_informacao_do_futuro(spark):
    """O teste mais importante do projeto.

    Cada transacao deve ser enriquecida com o snapshot do cliente do mes
    ANTERIOR. Se `feature_month` for igual ou posterior ao mes do evento, o
    modelo estaria treinando com informacao que ainda nao existia — e a metrica
    offline viraria ficcao.
    """
    from pyspark.sql import functions as F

    df = _requer_tabela(spark, "feature_store", "transaction_features")

    vazamentos = df.where(
        F.col("feature_month") >= F.date_format(F.col("event_date"), "yyyy-MM")
    ).count()
    assert vazamentos == 0, f"{vazamentos} transacoes usam features do proprio mes ou do futuro"


def test_kpis_diarios_formam_serie_continua_sem_buracos(spark):
    from pyspark.sql import functions as F

    df = _requer_tabela(spark, "gold", "financial_kpis_daily")
    limites = df.agg(F.min("event_date").alias("inicio"),
                     F.max("event_date").alias("fim")).collect()[0]

    dias_esperados = (limites["fim"] - limites["inicio"]).days + 1
    assert df.count() == dias_esperados, "ha dias faltando na serie temporal"
    assert df.select("event_date").distinct().count() == dias_esperados


def test_quarentena_registra_o_motivo_da_rejeicao(spark):
    from src.common.delta_io import table_exists

    caminho = str(get_settings().layer_path("silver") / "_quarantine" / "transactions")
    if not table_exists(spark, caminho):
        pytest.skip("quarentena vazia nesta execucao")

    df = spark.read.format("delta").load(caminho)
    assert "_reject_reason" in df.columns
    motivos = {linha["_reject_reason"] for linha in df.select("_reject_reason").distinct().collect()}
    assert motivos, "quarentena sem motivo preenchido"
