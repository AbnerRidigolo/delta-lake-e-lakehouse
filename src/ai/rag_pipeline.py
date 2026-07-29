"""
Pipeline de RAG (Retrieval-Augmented Generation) sobre a camada Gold.

A ideia
-------
Transformar as tabelas Gold em uma **base de conhecimento consultavel em
linguagem natural**: "como esta a inadimplencia da safra de marco?", "quais
lojistas estao em alerta?", "o antifraude melhorou?".

Arquitetura implementada (roda 100% offline)
--------------------------------------------

    Gold (Delta)
        |  1. Chunking semantico: cada linha/agrupamento vira um paragrafo
        v     descritivo COM os numeros embutidos (documento "auto-contido")
    Documentos
        |  2. Embeddings locais: TF-IDF + SVD (LSA). Sem chave de API, sem
        v     download de modelo - o projeto roda em qualquer maquina.
    Vector store (tabela Delta `vector_store.gold_knowledge`)
        |  3. Recuperacao: similaridade do cosseno, top-k
        v
    Prompt montado com o contexto recuperado
        |  4. Geracao: MockLLM (deterministico, offline) ou Anthropic
        v
    Resposta com **citacao da tabela de origem**

Decisoes que valem a pena notar
-------------------------------
* **O vetor store e uma tabela Delta.** Nao e "gambiarra": versionamento, time
  travel e ACID valem tanto para embeddings quanto para dados. Reindexar vira
  um commit, e da para voltar ao indice anterior se a qualidade cair.
* **Cada chunk carrega a tabela de origem e o grao.** A resposta cita a fonte -
  requisito basico em contexto financeiro, onde "confie em mim" nao existe.
* **A camada de LLM e um adaptador.** Trocar `MockLLM` por `AnthropicLLM` nao
  muda nada no resto do pipeline; e so configuracao.

Em producao, o que mudaria: embeddings de um modelo dedicado (Voyage, OpenAI,
BGE), um vetor store com indice ANN (pgvector, Qdrant, Databricks Vector
Search) e reranking. A estrutura do codigo continuaria a mesma.

Execucao:
    python -m src.ai.rag_pipeline --build                       # indexa a Gold
    python -m src.ai.rag_pipeline --ask "como esta a inadimplencia?"
    python -m src.ai.rag_pipeline --demo                        # bateria de perguntas
"""

from __future__ import annotations

import argparse
import json
import os
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

KNOWLEDGE_TABLE = "gold_knowledge"
EMBEDDING_MODEL_FILE = "rag_embedder.joblib"


# ===========================================================================
# 1) Construcao dos documentos a partir da camada Gold
# ===========================================================================


@dataclass
class Document:
    """Um pedaco de conhecimento recuperavel."""

    doc_id: str
    source_table: str
    grain: str
    title: str
    content: str
    metadata: str  # JSON com os numeros crus, para a resposta poder ser precisa


def _fmt(value, digits: int = 2) -> str:
    if value is None:
        return "n/d"
    try:
        return f"{float(value):,.{digits}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    except (TypeError, ValueError):
        return str(value)


def build_documents(spark: SparkSession) -> list[Document]:
    """Converte as tabelas Gold em paragrafos descritivos auto-contidos.

    A regra de ouro do chunking: cada documento precisa fazer sentido **sozinho**,
    porque e assim que ele sera recuperado. Por isso repetimos o contexto (mes,
    produto, nome do lojista) dentro de cada texto, em vez de assumir que quem
    le tem a tabela ao lado.
    """
    cfg = get_settings()
    documents: list[Document] = []

    # ------------------------------------------------------ visao mensal ----
    kpis = spark.read.format("delta").load(cfg.table_path("gold", "financial_kpis_daily"))
    monthly = (
        kpis.groupBy("year_month")
        .agg(
            F.round(F.sum("tpv"), 2).alias("tpv"),
            F.round(F.sum("revenue_mdr"), 2).alias("receita_mdr"),
            F.round(F.sum("revenue_interest"), 2).alias("receita_juros"),
            F.round(F.sum("revenue_total"), 2).alias("receita_total"),
            F.round(F.sum("loss_total"), 2).alias("perdas"),
            F.round(F.sum("recovery_amount"), 2).alias("recuperacao"),
            F.round(F.sum("net_result"), 2).alias("resultado"),
            F.sum("txn_count").alias("transacoes"),
            F.round(F.avg("approval_rate"), 4).alias("taxa_aprovacao"),
            F.round(F.avg("chargeback_rate"), 5).alias("taxa_chargeback"),
            F.round(F.avg("take_rate"), 5).alias("take_rate"),
        )
        .orderBy("year_month")
        .collect()
    )

    for row in monthly:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"kpi::{data['year_month']}",
                source_table="gold.financial_kpis_daily",
                grain="mes",
                title=f"Resultado financeiro de {data['year_month']}",
                content=(
                    f"No mes de {data['year_month']}, a fintech NeoPag processou "
                    f"{int(data['transacoes'])} transacoes com TPV (volume transacionado) de "
                    f"R$ {_fmt(data['tpv'])}. A receita total foi de R$ {_fmt(data['receita_total'])}, "
                    f"sendo R$ {_fmt(data['receita_mdr'])} de MDR (taxa de adquirencia cobrada dos "
                    f"lojistas) e R$ {_fmt(data['receita_juros'])} de juros da carteira de credito. "
                    f"As perdas totais (chargeback, fraude e write-off) somaram "
                    f"R$ {_fmt(data['perdas'])} e a recuperacao de credito trouxe de volta "
                    f"R$ {_fmt(data['recuperacao'])}. O resultado liquido do mes foi de "
                    f"R$ {_fmt(data['resultado'])}. A taxa de aprovacao media foi de "
                    f"{_fmt(data['taxa_aprovacao'] * 100)}%, a taxa de chargeback de "
                    f"{_fmt(data['taxa_chargeback'] * 100, 3)}% e o take rate de "
                    f"{_fmt(data['take_rate'] * 100, 3)}%."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # ------------------------------------------------- carteira de credito ---
    portfolio = spark.read.format("delta").load(cfg.table_path("gold", "credit_risk_portfolio"))
    by_product = (
        portfolio.groupBy("product")
        .agg(
            F.sum("contracts").alias("contratos"),
            F.round(F.sum("ead"), 2).alias("ead"),
            F.round(F.sum("npl_balance"), 2).alias("saldo_npl"),
            F.round(F.sum("provision_amount"), 2).alias("provisao"),
            F.round(F.sum("expected_loss"), 2).alias("perda_esperada"),
            F.round(F.avg("pd_observed"), 4).alias("pd_media"),
            F.round(F.avg("lgd_assumption"), 4).alias("lgd"),
        )
        .collect()
    )

    for row in by_product:
        data = row.asDict()
        npl_ratio = (data["saldo_npl"] or 0) / max(data["ead"] or 1, 1)
        documents.append(
            Document(
                doc_id=f"credito::{data['product']}",
                source_table="gold.credit_risk_portfolio",
                grain="produto de credito",
                title=f"Carteira de credito - produto {data['product']}",
                content=(
                    f"O produto de credito '{data['product']}' tem {int(data['contratos'])} contratos "
                    f"e exposicao (EAD) de R$ {_fmt(data['ead'])}. O saldo inadimplente acima de 90 "
                    f"dias (NPL) e de R$ {_fmt(data['saldo_npl'])}, o que representa "
                    f"{_fmt(npl_ratio * 100)}% da carteira do produto. A provisao constituida e de "
                    f"R$ {_fmt(data['provisao'])} e a perda esperada calculada por EAD x PD x LGD e "
                    f"de R$ {_fmt(data['perda_esperada'])}. A probabilidade de default observada "
                    f"(PD) media e de {_fmt(data['pd_media'] * 100)}% e a perda dado o default (LGD) "
                    f"assumida e de {_fmt(data['lgd'] * 100)}%."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # Safras com alerta: o que a area de risco realmente procura.
    alerts = (
        portfolio.where(F.col("portfolio_alert") != "dentro_do_esperado")
        .orderBy(F.desc("expected_loss"))
        .limit(15)
        .collect()
    )
    for row in alerts:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"safra::{data['vintage']}::{data['product']}::{data['customer_risk_band']}",
                source_table="gold.credit_risk_portfolio",
                grain="safra x produto x faixa de risco",
                title=f"Alerta na safra {data['vintage']} - {data['product']}",
                content=(
                    f"A safra de {data['vintage']} do produto '{data['product']}' na faixa de risco "
                    f"{data['customer_risk_band']} esta classificada como '{data['portfolio_alert']}'. "
                    f"Sao {int(data['contracts'])} contratos com EAD de R$ {_fmt(data['ead'])}, "
                    f"PD observada de {_fmt(data['pd_observed'] * 100)}%, perda esperada de "
                    f"R$ {_fmt(data['expected_loss'])} e indice de cobertura de "
                    f"{_fmt(data['coverage_ratio'])}x. O atraso medio e de "
                    f"{_fmt(data['avg_days_past_due'], 0)} dias."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # ------------------------------------------------------------ clientes ---
    customers = spark.read.format("delta").load(cfg.table_path("gold", "customer_360"))
    by_segment = (
        customers.groupBy("segment", "value_risk_segment")
        .agg(
            F.count("*").alias("clientes"),
            F.round(F.avg("customer_score"), 0).alias("score_medio"),
            F.round(F.sum("tpv_90d"), 2).alias("tpv_90d"),
            F.round(F.avg("limit_utilization"), 4).alias("utilizacao_limite"),
            F.sum(F.col("is_churn_risk").cast("int")).alias("em_churn"),
        )
        .collect()
    )

    for row in by_segment:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"cliente::{data['segment']}::{data['value_risk_segment']}",
                source_table="gold.customer_360",
                grain="segmento x perfil de valor/risco",
                title=f"Clientes {data['segment']} no perfil {data['value_risk_segment']}",
                content=(
                    f"O segmento '{data['segment']}' no perfil de valor e risco "
                    f"'{data['value_risk_segment']}' reune {int(data['clientes'])} clientes, com "
                    f"score medio de {_fmt(data['score_medio'], 0)} pontos (escala 0 a 1000) e TPV "
                    f"nos ultimos 90 dias de R$ {_fmt(data['tpv_90d'])}. A utilizacao media de limite "
                    f"e de {_fmt(data['utilizacao_limite'] * 100)}% e "
                    f"{int(data['em_churn'])} clientes estao em risco de churn (inatividade)."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # -------------------------------------------------------------- fraude ---
    signals = spark.read.format("delta").load(cfg.table_path("gold", "transaction_fraud_signals"))
    by_channel = (
        signals.groupBy("channel")
        .agg(
            F.count("*").alias("transacoes"),
            F.sum("is_fraud").alias("fraudes"),
            F.round(F.avg("is_fraud"), 5).alias("taxa_fraude"),
            F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0.0)), 2).alias(
                "valor_fraude"
            ),
            F.round(F.avg("fraud_score_rule"), 1).alias("score_medio"),
        )
        .collect()
    )

    for row in by_channel:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"fraude::canal::{data['channel']}",
                source_table="gold.transaction_fraud_signals",
                grain="canal de pagamento",
                title=f"Fraude no canal {data['channel']}",
                content=(
                    f"O canal de pagamento '{data['channel']}' registrou "
                    f"{int(data['transacoes'])} transacoes, das quais {int(data['fraudes'])} foram "
                    f"fraudulentas - uma taxa de fraude de {_fmt(data['taxa_fraude'] * 100, 3)}%, "
                    f"totalizando R$ {_fmt(data['valor_fraude'])} em valor fraudado. O score medio "
                    f"atribuido pelo motor de regras neste canal e de {_fmt(data['score_medio'], 1)} "
                    f"pontos (escala 0 a 100)."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    by_pattern = (
        signals.where(F.col("is_fraud") == 1)
        .groupBy("fraud_type")
        .agg(
            F.count("*").alias("ocorrencias"),
            F.round(F.sum("amount"), 2).alias("valor"),
            F.round(F.avg("amount"), 2).alias("ticket_medio"),
            F.round(F.avg("fraud_score_rule"), 1).alias("score_medio"),
            F.round(F.avg(F.col("is_new_device").cast("int")), 3).alias("share_device_novo"),
        )
        .collect()
    )

    for row in by_pattern:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"fraude::padrao::{data['fraud_type']}",
                source_table="gold.transaction_fraud_signals",
                grain="padrao de fraude",
                title=f"Padrao de fraude: {data['fraud_type']}",
                content=(
                    f"O padrao de fraude '{data['fraud_type']}' teve {int(data['ocorrencias'])} "
                    f"ocorrencias no periodo, somando R$ {_fmt(data['valor'])} com ticket medio de "
                    f"R$ {_fmt(data['ticket_medio'])}. O motor de regras atribuiu score medio de "
                    f"{_fmt(data['score_medio'], 1)} pontos a essas transacoes e "
                    f"{_fmt(data['share_device_novo'] * 100, 1)}% delas partiram de um dispositivo "
                    f"nunca usado antes pelo cliente."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # ------------------------------------------------------------ lojistas ---
    merchants = spark.read.format("delta").load(cfg.table_path("gold", "merchant_performance"))
    by_category = (
        merchants.groupBy("category")
        .agg(
            F.countDistinct("merchant_id").alias("lojistas"),
            F.round(F.sum("tpv"), 2).alias("tpv"),
            F.round(F.sum("mdr_revenue"), 2).alias("receita"),
            F.round(F.avg("chargeback_rate"), 5).alias("taxa_chargeback"),
            F.round(F.avg("fraud_rate"), 5).alias("taxa_fraude"),
        )
        .collect()
    )

    for row in by_category:
        data = row.asDict()
        documents.append(
            Document(
                doc_id=f"lojista::categoria::{data['category']}",
                source_table="gold.merchant_performance",
                grain="categoria de estabelecimento",
                title=f"Estabelecimentos da categoria {data['category']}",
                content=(
                    f"A categoria de estabelecimentos '{data['category']}' reune "
                    f"{int(data['lojistas'])} lojistas, com TPV de R$ {_fmt(data['tpv'])} e receita "
                    f"de MDR de R$ {_fmt(data['receita'])}. A taxa media de chargeback da categoria "
                    f"e de {_fmt(data['taxa_chargeback'] * 100, 3)}% e a taxa de fraude e de "
                    f"{_fmt(data['taxa_fraude'] * 100, 3)}%."
                ),
                metadata=json.dumps(data, default=str, ensure_ascii=False),
            )
        )

    # ---------------------------------------------- documentos de RANKING ----
    documents.extend(
        build_ranking_documents(by_channel, by_category, by_product, portfolio, monthly)
    )

    log.info("Documentos construidos: %s", len(documents))
    return documents


def build_ranking_documents(
    by_channel, by_category, by_product, portfolio, monthly
) -> list[Document]:
    """Cria documentos de **ranking** e de **panorama consolidado**.

    Por que isso e necessario
    -------------------------
    Perguntas de negocio quase sempre vem em forma de superlativo: "qual canal
    tem MAIS fraude?", "qual a PIOR safra?". A recuperacao por similaridade nao
    entende superlativo - ela casa o vocabulario da pergunta ("canal de
    pagamento", "fraude") e pode devolver justamente o canal com fraude ZERO,
    porque o texto dele contem as mesmas palavras.

    A correcao nao e trocar o modelo de embedding: e dar ao indice um documento
    que **ja responde** a pergunta agregada. Precomputar rankings e visoes
    consolidadas na indexacao e uma das tecnicas mais efetivas de RAG sobre
    dados estruturados - e custa uma agregacao a mais, nao um modelo maior.
    """
    documents: list[Document] = []

    # --- ranking de canais por taxa de fraude --------------------------------
    canais = sorted(
        [r.asDict() for r in by_channel], key=lambda r: r["taxa_fraude"] or 0, reverse=True
    )
    if canais:
        linhas = "; ".join(
            f"{i}o lugar: {c['channel']} com {_fmt((c['taxa_fraude'] or 0) * 100, 3)}% "
            f"de fraude ({int(c['fraudes'])} casos, R$ {_fmt(c['valor_fraude'])})"
            for i, c in enumerate(canais, start=1)
        )
        documents.append(
            Document(
                doc_id="ranking::canais_fraude",
                source_table="gold.transaction_fraud_signals",
                grain="ranking",
                title="Ranking dos canais de pagamento por taxa de fraude",
                content=(
                    f"Ranking dos canais de pagamento com MAIS fraude, do maior para o menor. "
                    f"O canal com a maior taxa de fraude, o mais arriscado e o que concentra o "
                    f"maior numero de fraudes e o '{canais[0]['channel']}', com "
                    f"{_fmt((canais[0]['taxa_fraude'] or 0) * 100, 3)}% das transacoes fraudulentas. "
                    f"O canal mais seguro, com a menor taxa de fraude, e o "
                    f"'{canais[-1]['channel']}'. Ranking completo: {linhas}."
                ),
                metadata=json.dumps(canais, default=str, ensure_ascii=False),
            )
        )

    # --- ranking de categorias de lojista por chargeback ---------------------
    categorias = sorted(
        [r.asDict() for r in by_category], key=lambda r: r["taxa_chargeback"] or 0, reverse=True
    )
    if categorias:
        linhas = "; ".join(
            f"{c['category']}: {_fmt((c['taxa_chargeback'] or 0) * 100, 3)}% de chargeback e "
            f"{_fmt((c['taxa_fraude'] or 0) * 100, 3)}% de fraude"
            for c in categorias[:8]
        )
        documents.append(
            Document(
                doc_id="ranking::categorias_chargeback",
                source_table="gold.merchant_performance",
                grain="ranking",
                title="Ranking das categorias de estabelecimento por taxa de chargeback",
                content=(
                    f"Ranking das categorias de estabelecimento com MAIOR taxa de chargeback e "
                    f"maior risco. A categoria com a pior taxa de chargeback e "
                    f"'{categorias[0]['category']}', com "
                    f"{_fmt((categorias[0]['taxa_chargeback'] or 0) * 100, 3)}%. A categoria com o "
                    f"menor risco de chargeback e '{categorias[-1]['category']}'. "
                    f"Ranking: {linhas}."
                ),
                metadata=json.dumps(categorias, default=str, ensure_ascii=False),
            )
        )

    # --- ranking de produtos de credito por inadimplencia --------------------
    produtos = sorted(
        [r.asDict() for r in by_product],
        key=lambda r: (r["saldo_npl"] or 0) / max(r["ead"] or 1, 1),
        reverse=True,
    )
    if produtos:
        linhas = "; ".join(
            f"{p['product']}: NPL de "
            f"{_fmt(((p['saldo_npl'] or 0) / max(p['ead'] or 1, 1)) * 100)}% sobre EAD de "
            f"R$ {_fmt(p['ead'])}"
            for p in produtos
        )
        documents.append(
            Document(
                doc_id="ranking::produtos_inadimplencia",
                source_table="gold.credit_risk_portfolio",
                grain="ranking",
                title="Ranking dos produtos de credito por inadimplencia",
                content=(
                    f"Ranking dos produtos de credito com MAIOR inadimplencia (NPL 90+). O produto "
                    f"mais inadimplente, com a pior carteira e o maior risco de credito, e "
                    f"'{produtos[0]['product']}'. O produto com melhor comportamento de pagamento e "
                    f"'{produtos[-1]['product']}'. Ranking: {linhas}."
                ),
                metadata=json.dumps(produtos, default=str, ensure_ascii=False),
            )
        )

    # --- piores safras -------------------------------------------------------
    piores = [
        r.asDict()
        for r in portfolio.where(F.col("contracts") >= 5)
        .orderBy(F.desc("pd_observed"))
        .limit(8)
        .collect()
    ]
    if piores:
        linhas = "; ".join(
            f"safra {p['vintage']} do produto {p['product']} na faixa {p['customer_risk_band']} "
            f"com PD de {_fmt((p['pd_observed'] or 0) * 100)}%"
            for p in piores
        )
        documents.append(
            Document(
                doc_id="ranking::piores_safras",
                source_table="gold.credit_risk_portfolio",
                grain="ranking",
                title="Piores safras de originacao de credito",
                content=(
                    f"Ranking das PIORES safras de originacao de credito, com a maior probabilidade "
                    f"de default (PD) observada. A safra mais problematica e critica e a de "
                    f"{piores[0]['vintage']} do produto '{piores[0]['product']}', com PD de "
                    f"{_fmt((piores[0]['pd_observed'] or 0) * 100)}%. Ranking completo: {linhas}."
                ),
                metadata=json.dumps(piores, default=str, ensure_ascii=False),
            )
        )

    # --- panorama consolidado do periodo -------------------------------------
    if monthly:
        totais = {
            "tpv": sum(r["tpv"] or 0 for r in monthly),
            "receita": sum(r["receita_total"] or 0 for r in monthly),
            "perdas": sum(r["perdas"] or 0 for r in monthly),
            "resultado": sum(r["resultado"] or 0 for r in monthly),
            "transacoes": sum(int(r["transacoes"] or 0) for r in monthly),
        }
        melhor = max(monthly, key=lambda r: r["resultado"] or 0)
        pior = min(monthly, key=lambda r: r["resultado"] or 0)
        documents.append(
            Document(
                doc_id="panorama::consolidado",
                source_table="gold.financial_kpis_daily",
                grain="periodo completo",
                title="Panorama financeiro consolidado do periodo",
                content=(
                    f"Panorama consolidado de todo o periodo analisado ({monthly[0]['year_month']} a "
                    f"{monthly[-1]['year_month']}): a fintech NeoPag processou no total "
                    f"{totais['transacoes']} transacoes, com TPV acumulado de R$ {_fmt(totais['tpv'])}, "
                    f"receita total de R$ {_fmt(totais['receita'])}, perdas totais de "
                    f"R$ {_fmt(totais['perdas'])} e resultado liquido acumulado de "
                    f"R$ {_fmt(totais['resultado'])}. O melhor mes foi {melhor['year_month']} "
                    f"(resultado de R$ {_fmt(melhor['resultado'])}) e o pior foi {pior['year_month']} "
                    f"(resultado de R$ {_fmt(pior['resultado'])})."
                ),
                metadata=json.dumps(totais, default=str, ensure_ascii=False),
            )
        )

    return documents


# ===========================================================================
# 2) Embeddings locais
# ===========================================================================


class LocalEmbedder:
    """Embeddings sem dependencia externa: TF-IDF seguido de SVD (LSA).

    Nao compete com um modelo neural de embeddings, mas resolve o problema
    pedagogico e pratico deste projeto: vetores densos, deterministicos, que
    capturam similaridade lexica e alguma similaridade semantica, rodando em
    qualquer maquina sem GPU nem chave de API.

    Trocar por um provider real e substituir esta classe - a interface
    (`fit_transform`, `transform`) e a mesma de qualquer client de embeddings.
    """

    def __init__(self, dimension: int = 256) -> None:
        self.dimension = dimension
        self.vectorizer = None
        self.reducer = None

    def fit_transform(self, texts: Sequence[str]) -> np.ndarray:
        from sklearn.decomposition import TruncatedSVD
        from sklearn.feature_extraction.text import TfidfVectorizer

        self.vectorizer = TfidfVectorizer(
            lowercase=True,
            # 1-2 gramas capturam termos compostos ("taxa de fraude", "perda esperada")
            ngram_range=(1, 2),
            min_df=1,
            max_features=20_000,
            strip_accents="unicode",
        )
        matrix = self.vectorizer.fit_transform(texts)

        # O SVD nao pode ter mais componentes do que features/documentos.
        n_components = min(self.dimension, matrix.shape[1] - 1, len(texts) - 1)
        n_components = max(n_components, 2)
        self.reducer = TruncatedSVD(n_components=n_components, random_state=42)
        vectors = self.reducer.fit_transform(matrix)
        return _l2_normalize(vectors)

    def transform(self, texts: Sequence[str]) -> np.ndarray:
        if self.vectorizer is None or self.reducer is None:
            raise RuntimeError("Embedder nao treinado. Rode `build_index()` primeiro.")
        return _l2_normalize(self.reducer.transform(self.vectorizer.transform(texts)))


def _l2_normalize(matrix: np.ndarray) -> np.ndarray:
    """Normaliza os vetores para que o produto interno seja o cosseno."""
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / np.maximum(norms, 1e-12)


# ===========================================================================
# 3) Vector store em Delta
# ===========================================================================


def build_index() -> dict[str, int]:
    """Indexa a camada Gold: documentos + embeddings -> tabela Delta."""
    cfg = get_settings()
    spark = get_spark("rag_build_index")

    with log_stage(log, "ai.rag_pipeline::build_index"):
        documents = build_documents(spark)
        if not documents:
            raise RuntimeError("Nenhum documento gerado - a camada Gold esta vazia?")

        embedder = LocalEmbedder(dimension=int(cfg.ai["embedding_dim"]))
        vectors = embedder.fit_transform([d.content for d in documents])

        rows = [
            {
                "doc_id": doc.doc_id,
                "source_table": doc.source_table,
                "grain": doc.grain,
                "title": doc.title,
                "content": doc.content,
                "metadata": doc.metadata,
                "embedding": [float(x) for x in vector],
                "indexed_at": datetime.now(timezone.utc).replace(tzinfo=None),
            }
            for doc, vector in zip(documents, vectors)
        ]

        path = cfg.table_path("vector_store", KNOWLEDGE_TABLE)
        write_delta(
            spark.createDataFrame(rows),
            path,
            mode="overwrite",
            overwrite_schema=True,
            comment=f"indice RAG da camada Gold ({len(rows)} chunks)",
        )

        # O modelo de embedding precisa ser persistido: a consulta tem que usar
        # exatamente o mesmo espaco vetorial da indexacao.
        import joblib

        artifacts = cfg.layer_path("artifacts")
        artifacts.mkdir(parents=True, exist_ok=True)
        joblib.dump(embedder, artifacts / EMBEDDING_MODEL_FILE)

        log.info(
            "Indice RAG criado: %s chunks, dimensao %s -> %s", len(rows), vectors.shape[1], path
        )
        return {"chunks": len(rows), "dimensao": int(vectors.shape[1])}


# ===========================================================================
# 4) Recuperacao
# ===========================================================================


@dataclass
class RetrievedChunk:
    doc_id: str
    source_table: str
    title: str
    content: str
    score: float


# Termos que revelam intencao de SUPERLATIVO ou de visao consolidada.
# Uma pergunta como "qual canal tem MAIS fraude?" nao e respondida pelo chunk de
# um canal qualquer - e respondida pelo documento de ranking.
SUPERLATIVE_TERMS = {
    "mais",
    "maior",
    "maiores",
    "pior",
    "piores",
    "melhor",
    "melhores",
    "top",
    "ranking",
    "principal",
    "principais",
    "concentra",
    "lidera",
    "menor",
    "menores",
}
AGGREGATE_TERMS = {
    "total",
    "totais",
    "consolidado",
    "geral",
    "acumulado",
    "periodo",
    "resumo",
    "panorama",
    "faturou",
    "faturamento",
}
# Peso do boost aplicado aos documentos de ranking/panorama quando a intencao
# da pergunta e superlativa. Calibrado para reordenar sem anular o sinal vetorial.
INTENT_BOOST = 0.35


def detect_intent(question: str) -> str:
    """Classifica a intencao da pergunta: `ranking`, `panorama` ou `especifica`."""
    tokens = set(question.lower().replace("?", " ").replace(",", " ").split())
    if tokens & SUPERLATIVE_TERMS:
        return "ranking"
    if tokens & AGGREGATE_TERMS:
        return "panorama"
    return "especifica"


def retrieve(question: str, top_k: int | None = None) -> list[RetrievedChunk]:
    """Busca os chunks mais relevantes com **recuperacao hibrida**.

    Score final = similaridade do cosseno + boost por intencao.

    Por que hibrido e nao so vetorial
    ---------------------------------
    A busca vetorial pura falha de forma previsivel em superlativos. Perguntando
    "qual canal tem MAIS fraude?", os chunks de TODOS os canais tem
    praticamente a mesma similaridade - eles compartilham o vocabulario da
    pergunta - e o topo acaba sendo decidido por ruido. Nao raro volta justamente
    o canal com fraude ZERO.

    A correcao aplicada aqui tem duas partes:
      1. na indexacao, precomputar documentos de ranking (ver `build_ranking_documents`);
      2. na recuperacao, detectar a intencao da pergunta e priorizar esses documentos.

    E o mesmo principio dos sistemas de busca hibrida em producao (BM25 + vetor +
    reranking): o sinal semantico sozinho nao carrega intencao de agregacao.
    """
    cfg = get_settings()
    spark = get_spark("rag_retrieve")
    top_k = top_k or int(cfg.ai["top_k"])

    import joblib

    embedder_path = cfg.layer_path("artifacts") / EMBEDDING_MODEL_FILE
    if not embedder_path.exists():
        raise RuntimeError("Indice nao encontrado. Rode `python -m src.ai.rag_pipeline --build`.")
    embedder: LocalEmbedder = joblib.load(embedder_path)

    index = (
        spark.read.format("delta").load(cfg.table_path("vector_store", KNOWLEDGE_TABLE)).toPandas()
    )

    matrix = np.vstack(index["embedding"].apply(np.array).to_numpy())
    query_vector = embedder.transform([question])[0]

    # Vetores ja normalizados -> produto interno == cosseno.
    scores = matrix @ query_vector

    intent = detect_intent(question)
    if intent in ("ranking", "panorama"):
        alvo = "ranking" if intent == "ranking" else "periodo completo"
        boost = (index["grain"] == alvo).to_numpy().astype(float) * INTENT_BOOST
        # O panorama tambem ajuda em perguntas de ranking, e vice-versa.
        boost += (index["grain"].isin(["ranking", "periodo completo"])).to_numpy().astype(float) * (
            INTENT_BOOST / 3
        )
        scores = scores + boost
        log.info(
            "Intencao detectada: %s (boost aplicado a %s documentos)",
            intent,
            int((boost > 0).sum()),
        )

    best = np.argsort(-scores)[:top_k]

    return [
        RetrievedChunk(
            doc_id=index.iloc[i]["doc_id"],
            source_table=index.iloc[i]["source_table"],
            title=index.iloc[i]["title"],
            content=index.iloc[i]["content"],
            score=round(float(scores[i]), 4),
        )
        for i in best
    ]


# ===========================================================================
# 5) Camada de geracao (adaptadores de LLM)
# ===========================================================================


class BaseLLM:
    """Interface comum dos geradores de resposta."""

    def generate(self, question: str, context: list[RetrievedChunk]) -> str:
        raise NotImplementedError


class MockLLM(BaseLLM):
    """Gerador offline e deterministico.

    Nao inventa nada: monta a resposta a partir do contexto recuperado, sempre
    citando a tabela de origem. Serve para (1) rodar o projeto sem chave de API
    e (2) como caso de teste - se a recuperacao piorar, a resposta piora junto,
    sem o ruido de um modelo probabilistico no meio.
    """

    def generate(self, question: str, context: list[RetrievedChunk]) -> str:
        if not context:
            return "Nao encontrei informacao suficiente na camada Gold para responder."

        parts = [
            f"**Pergunta:** {question}",
            "",
            "**Resposta baseada nos dados da camada Gold:**",
            "",
        ]
        for i, chunk in enumerate(context, start=1):
            parts.append(f"{i}. {chunk.content}")
            parts.append(f"   _(fonte: `{chunk.source_table}` - relevancia {chunk.score})_")
            parts.append("")
        parts.append("---")
        parts.append(
            f"_Resposta gerada por recuperacao sobre {len(context)} trechos indexados. "
            "Nenhum numero foi gerado pelo modelo: todos vem das tabelas Delta._"
        )
        return "\n".join(parts)


class AnthropicLLM(BaseLLM):
    """Adaptador para a API da Anthropic (usado quando ha chave configurada).

    O prompt e construido com duas travas que importam em contexto financeiro:
    (1) responder **apenas** com base no contexto recuperado e (2) dizer
    explicitamente quando o contexto nao contem a resposta. Alucinar um numero
    de inadimplencia e pior do que nao responder.
    """

    SYSTEM_PROMPT = textwrap.dedent("""
        Voce e um analista de dados senior de uma fintech brasileira. Responda em
        portugues do Brasil, de forma objetiva e executiva.

        Regras obrigatorias:
        1. Use EXCLUSIVAMENTE os dados do contexto fornecido. Nunca estime,
           complete ou infira numeros que nao estejam ali.
        2. Cite a tabela de origem de cada numero que usar.
        3. Se o contexto nao contiver a resposta, diga isso claramente e sugira
           qual tabela da camada Gold deveria ser consultada.
        4. Traga a leitura de negocio, nao so o numero: o que o dado significa e
           qual acao ele sugere.
    """).strip()

    def __init__(self, model: str, api_key: str) -> None:
        self.model = model
        self.api_key = api_key

    def generate(self, question: str, context: list[RetrievedChunk]) -> str:
        from anthropic import Anthropic

        client = Anthropic(api_key=self.api_key)
        context_block = "\n\n".join(
            f"[Fonte: {c.source_table} | {c.title}]\n{c.content}" for c in context
        )
        message = client.messages.create(
            model=self.model,
            max_tokens=1200,
            system=self.SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": f"Contexto recuperado do lakehouse:\n\n{context_block}\n\n"
                    f"Pergunta: {question}",
                }
            ],
        )
        return message.content[0].text


def get_llm() -> BaseLLM:
    """Escolhe o gerador conforme a configuracao e a disponibilidade de chave."""
    cfg = get_settings()
    provider = str(cfg.ai.get("llm_provider", "mock")).lower()
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if provider == "anthropic" and api_key:
        log.info("Usando LLM Anthropic (%s)", cfg.ai["llm_model"])
        return AnthropicLLM(str(cfg.ai["llm_model"]), api_key)
    if provider == "anthropic":
        log.warning(
            "llm_provider=anthropic, mas ANTHROPIC_API_KEY nao esta definida. "
            "Caindo para o gerador offline."
        )
    return MockLLM()


# ===========================================================================
# 6) Interface de pergunta e resposta
# ===========================================================================


def ask(question: str, top_k: int | None = None) -> dict[str, object]:
    """Executa o ciclo completo: recuperar -> montar contexto -> gerar."""
    chunks = retrieve(question, top_k)
    answer = get_llm().generate(question, chunks)
    return {
        "pergunta": question,
        "resposta": answer,
        "fontes": [
            {"doc_id": c.doc_id, "tabela": c.source_table, "relevancia": c.score} for c in chunks
        ],
    }


DEMO_QUESTIONS = [
    "Como esta a inadimplencia da carteira de credito?",
    "Qual canal de pagamento concentra mais fraude?",
    "Quanto a empresa faturou e qual foi o resultado liquido?",
    "Quais categorias de estabelecimento tem maior taxa de chargeback?",
    "Quantos clientes estao em risco de churn?",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline RAG sobre a camada Gold.")
    parser.add_argument("--build", action="store_true", help="(re)constroi o indice vetorial")
    parser.add_argument("--ask", default=None, help="pergunta em linguagem natural")
    parser.add_argument(
        "--demo", action="store_true", help="roda a bateria de perguntas de exemplo"
    )
    parser.add_argument("--top-k", type=int, default=None, help="numero de trechos recuperados")
    args = parser.parse_args()

    if args.build:
        build_index()
    if args.ask:
        result = ask(args.ask, args.top_k)
        print("\n" + str(result["resposta"]) + "\n")
    if args.demo:
        for question in DEMO_QUESTIONS:
            result = ask(question, args.top_k)
            print("=" * 78)
            print(result["resposta"])
            print()
    if not any([args.build, args.ask, args.demo]):
        parser.print_help()


if __name__ == "__main__":
    main()
