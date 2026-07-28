"""
Gerador automatico de insights analiticos sobre a camada Gold.

O que faz
---------
Le as tabelas Gold, extrai um conjunto de **fatos numericos verificaveis** e
escreve um relatorio executivo em Markdown (`reports/`), com:

* fechamento do periodo (receita, perdas, recuperacao, resultado, take rate);
* comparacao mes contra mes com leitura de tendencia;
* deteccao de anomalias na serie diaria (z-score robusto por mediana/MAD);
* diagnostico de credito por safra e de carteira de lojistas;
* desempenho do antifraude (regras + modelo).

Por que a geracao e baseada em fatos, e nao em texto livre
----------------------------------------------------------
O modulo separa **extracao de fatos** (Spark, deterministico, auditavel) de
**redacao** (template ou LLM). Essa separacao e o que torna o resultado
confiavel: o numero sempre vem de uma query, nunca da "memoria" de um modelo.
E tambem o que alimenta o RAG - o JSON de fatos vira contexto recuperavel.

Se houver uma chave de API configurada (`ai.llm_provider: anthropic`), o mesmo
conjunto de fatos e enviado a um LLM para redacao mais fluida; sem chave, o
relatorio e montado por template. O conteudo numerico e identico nos dois casos.

Execucao:
    python -m src.ai.insight_generator
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)


# ===========================================================================
# 1) Extracao de fatos (a parte deterministica)
# ===========================================================================

def _money(value: Any) -> str:
    """Formata valores no padrao brasileiro (R$ 1.234.567,89)."""
    try:
        formatted = f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "n/d"
    return "R$ " + formatted.replace(",", "_").replace(".", ",").replace("_", ".")


def _int(value: Any) -> str:
    """Formata inteiros com separador de milhar brasileiro (1.234.567)."""
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "n/d"


def _pct(value: Any, digits: int = 2) -> str:
    try:
        return f"{float(value) * 100:.{digits}f}%".replace(".", ",")
    except (TypeError, ValueError):
        return "n/d"


def extract_financial_facts(kpis: DataFrame) -> Dict[str, Any]:
    """Fechamento do periodo e comparacao mes a mes."""
    totals = kpis.agg(
        F.min("event_date").alias("inicio"),
        F.max("event_date").alias("fim"),
        F.sum("tpv").alias("tpv"),
        F.sum("revenue_mdr").alias("receita_mdr"),
        F.sum("revenue_interest").alias("receita_juros"),
        F.sum("revenue_total").alias("receita_total"),
        F.sum("loss_chargeback").alias("perda_chargeback"),
        F.sum("loss_fraud").alias("perda_fraude"),
        F.sum("loss_write_off").alias("perda_write_off"),
        F.sum("recovery_amount").alias("recuperacao"),
        F.sum("net_result").alias("resultado"),
        F.sum("txn_count").alias("transacoes"),
        F.avg("approval_rate").alias("taxa_aprovacao"),
        F.avg("chargeback_rate").alias("taxa_chargeback"),
    ).collect()[0].asDict()

    totals["take_rate"] = (totals["receita_mdr"] or 0) / max(totals["tpv"] or 1, 1)
    totals["margem_liquida"] = (totals["resultado"] or 0) / max(totals["receita_total"] or 1, 1)

    monthly = (
        kpis.groupBy("year_month").agg(
            F.round(F.sum("tpv"), 2).alias("tpv"),
            F.round(F.sum("revenue_total"), 2).alias("receita"),
            F.round(F.sum("loss_total"), 2).alias("perdas"),
            F.round(F.sum("net_result"), 2).alias("resultado"),
            F.round(F.avg("approval_rate"), 4).alias("taxa_aprovacao"),
            F.round(F.avg("fraud_rate_value"), 5).alias("taxa_fraude_valor"),
        ).orderBy("year_month").collect()
    )
    months = [row.asDict() for row in monthly]

    comparison = {}
    if len(months) >= 2:
        current, previous = months[-1], months[-2]
        comparison = {
            "mes_atual": current["year_month"],
            "mes_anterior": previous["year_month"],
            "variacao_receita": (current["receita"] - previous["receita"])
                                / max(abs(previous["receita"]), 1e-9),
            "variacao_tpv": (current["tpv"] - previous["tpv"]) / max(abs(previous["tpv"]), 1e-9),
            "variacao_resultado": (current["resultado"] - previous["resultado"])
                                  / max(abs(previous["resultado"]), 1e-9),
        }

    return {"totais": totals, "serie_mensal": months, "comparacao_mensal": comparison}


def detect_anomalies(kpis: DataFrame, column: str = "tpv", threshold: float = 3.5) -> List[Dict]:
    """Deteccao de anomalias por z-score robusto (mediana + MAD).

    Usamos mediana e desvio absoluto mediano em vez de media e desvio padrao
    porque a propria anomalia contamina a media - o metodo classico esconde
    justamente o que deveria encontrar.
    """
    pdf = kpis.select("event_date", column).toPandas()
    if pdf.empty:
        return []

    median = pdf[column].median()
    mad = (pdf[column] - median).abs().median()
    if mad == 0:
        return []

    # 0.6745 converte o MAD para a escala do desvio padrao de uma normal.
    pdf["z_robusto"] = 0.6745 * (pdf[column] - median) / mad
    outliers = pdf[pdf["z_robusto"].abs() >= threshold].sort_values("z_robusto", key=abs,
                                                                   ascending=False)
    return [
        {
            "data": str(row["event_date"]),
            "metrica": column,
            "valor": round(float(row[column]), 2),
            "mediana_do_periodo": round(float(median), 2),
            "z_robusto": round(float(row["z_robusto"]), 2),
            "direcao": "acima do normal" if row["z_robusto"] > 0 else "abaixo do normal",
        }
        for _, row in outliers.head(5).iterrows()
    ]


def extract_credit_facts(portfolio: DataFrame) -> Dict[str, Any]:
    """Posicao da carteira de credito e safras problematicas."""
    totals = portfolio.agg(
        F.sum("ead").alias("ead"),
        F.sum("npl_balance").alias("saldo_npl"),
        F.sum("provision_amount").alias("provisao"),
        F.sum("expected_loss").alias("perda_esperada"),
        F.sum("contracts").alias("contratos"),
        F.sum("recovery_amount").alias("recuperacao"),
    ).collect()[0].asDict()
    totals["npl_ratio"] = (totals["saldo_npl"] or 0) / max(totals["ead"] or 1, 1)
    totals["cobertura"] = (totals["provisao"] or 0) / max(totals["saldo_npl"] or 1, 1)

    worst = (
        portfolio.where(F.col("contracts") >= 5)
        .orderBy(F.desc("pd_observed"))
        .select("vintage", "product", "customer_risk_band", "contracts", "ead",
                "pd_observed", "expected_loss", "portfolio_alert")
        .limit(5).collect()
    )
    by_product = (
        portfolio.groupBy("product").agg(
            F.round(F.sum("ead"), 2).alias("ead"),
            F.round(F.sum("expected_loss"), 2).alias("perda_esperada"),
            F.round(F.sum("npl_balance") / F.greatest(F.sum("ead"), F.lit(1.0)), 4).alias("npl_ratio"),
        ).orderBy(F.desc("ead")).collect()
    )

    return {
        "totais": totals,
        "piores_safras": [row.asDict() for row in worst],
        "por_produto": [row.asDict() for row in by_product],
    }


def extract_customer_facts(customers: DataFrame) -> Dict[str, Any]:
    """Base de clientes: churn, rating e concentracao de valor."""
    totals = customers.agg(
        F.count("*").alias("clientes"),
        F.sum(F.col("is_churn_risk").cast("int")).alias("em_risco_de_churn"),
        F.round(F.avg("customer_score"), 1).alias("score_medio"),
        F.sum("has_npl").alias("clientes_inadimplentes"),
        F.round(F.sum("tpv_90d"), 2).alias("tpv_90d"),
        F.round(F.avg("limit_utilization"), 4).alias("utilizacao_media_de_limite"),
    ).collect()[0].asDict()
    totals["taxa_de_churn"] = (totals["em_risco_de_churn"] or 0) / max(totals["clientes"] or 1, 1)

    by_segment = (
        customers.groupBy("value_risk_segment").agg(
            F.count("*").alias("clientes"),
            F.round(F.sum("tpv_90d"), 2).alias("tpv_90d"),
            F.round(F.avg("customer_score"), 0).alias("score_medio"),
        ).orderBy(F.desc("clientes")).collect()
    )
    by_rating = (
        customers.groupBy("rating").agg(F.count("*").alias("clientes"))
        .orderBy("rating").collect()
    )

    # Concentracao: quanto do volume esta nos 10% maiores clientes (Pareto).
    pdf = customers.select("tpv_90d").toPandas().sort_values("tpv_90d", ascending=False)
    top_decile = int(len(pdf) * 0.1) or 1
    concentration = pdf.head(top_decile)["tpv_90d"].sum() / max(pdf["tpv_90d"].sum(), 1e-9)

    return {
        "totais": totals,
        "por_segmento_de_valor": [row.asDict() for row in by_segment],
        "por_rating": [row.asDict() for row in by_rating],
        "concentracao_top_decil": float(concentration),
    }


def extract_fraud_facts(signals: DataFrame, artifacts_dir: Path) -> Dict[str, Any]:
    """Desempenho do antifraude: volume, perdas, regras e modelo."""
    totals = signals.agg(
        F.count("*").alias("transacoes"),
        F.sum("is_fraud").alias("fraudes"),
        F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0.0)), 2)
         .alias("valor_em_fraude"),
        F.sum(F.when(F.col("fraud_score_rule") >= 45, 1).otherwise(0)).alias("alertas"),
        F.sum(F.when((F.col("fraud_score_rule") >= 45) & (F.col("is_fraud") == 1), 1)
              .otherwise(0)).alias("alertas_corretos"),
    ).collect()[0].asDict()

    totals["taxa_de_fraude"] = (totals["fraudes"] or 0) / max(totals["transacoes"] or 1, 1)
    totals["precisao_regras"] = (totals["alertas_corretos"] or 0) / max(totals["alertas"] or 1, 1)
    totals["recall_regras"] = (totals["alertas_corretos"] or 0) / max(totals["fraudes"] or 1, 1)

    by_pattern = (
        signals.where(F.col("is_fraud") == 1).groupBy("fraud_type").agg(
            F.count("*").alias("ocorrencias"),
            F.round(F.sum("amount"), 2).alias("valor"),
            F.round(F.avg("fraud_score_rule"), 1).alias("score_medio_regras"),
        ).orderBy(F.desc("valor")).collect()
    )
    by_channel = (
        signals.groupBy("channel").agg(
            F.count("*").alias("transacoes"),
            F.round(F.avg("is_fraud"), 5).alias("taxa_de_fraude"),
        ).orderBy(F.desc("taxa_de_fraude")).collect()
    )

    # Metricas do modelo, se ele ja tiver sido treinado.
    model_metrics = {}
    metrics_file = artifacts_dir / "fraud_model_metrics.json"
    if metrics_file.exists():
        payload = json.loads(metrics_file.read_text(encoding="utf-8"))
        model_metrics = {
            "algoritmo": payload.get("algoritmo"),
            "pr_auc": payload.get("metricas_modelo", {}).get("pr_auc"),
            "roc_auc": payload.get("metricas_modelo", {}).get("roc_auc"),
            "ponto_de_operacao": payload.get("metricas_modelo", {}).get("ponto_de_operacao"),
            "baseline_regras": payload.get("baseline_regras"),
            "ganho_sobre_baseline": payload.get("ganho_sobre_baseline"),
            "top_features": payload.get("importancia_features", [])[:5],
        }

    return {
        "totais": totals,
        "por_padrao": [row.asDict() for row in by_pattern],
        "por_canal": [row.asDict() for row in by_channel],
        "modelo": model_metrics,
    }


def extract_merchant_facts(merchants: DataFrame) -> Dict[str, Any]:
    """Carteira de lojistas: maiores volumes e alertas de risco."""
    top = (
        merchants.groupBy("merchant_id", "merchant_name", "category").agg(
            F.round(F.sum("tpv"), 2).alias("tpv"),
            F.round(F.sum("mdr_revenue"), 2).alias("receita"),
            F.round(F.avg("chargeback_rate"), 5).alias("taxa_chargeback"),
        ).orderBy(F.desc("tpv")).limit(5).collect()
    )
    risky = (
        merchants.where(F.col("merchant_health").isin("descredenciar", "monitoramento_bandeira"))
        .groupBy("merchant_id", "merchant_name", "category", "merchant_health").agg(
            F.round(F.sum("tpv"), 2).alias("tpv"),
            F.round(F.avg("chargeback_rate"), 5).alias("taxa_chargeback"),
            F.round(F.avg("fraud_rate"), 5).alias("taxa_fraude"),
        ).orderBy(F.desc("taxa_chargeback")).limit(5).collect()
    )
    health = (
        merchants.groupBy("merchant_health").agg(F.countDistinct("merchant_id").alias("lojistas"))
        .orderBy(F.desc("lojistas")).collect()
    )
    return {
        "maiores_por_tpv": [row.asDict() for row in top],
        "em_alerta": [row.asDict() for row in risky],
        "distribuicao_de_saude": [row.asDict() for row in health],
    }


def collect_facts(spark: SparkSession) -> Dict[str, Any]:
    """Roda todas as extracoes e devolve o dicionario completo de fatos."""
    cfg = get_settings()
    gold = lambda name: spark.read.format("delta").load(cfg.table_path("gold", name))  # noqa: E731

    kpis = gold("financial_kpis_daily")
    facts = {
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "financeiro": extract_financial_facts(kpis),
        "anomalias_tpv": detect_anomalies(kpis, "tpv"),
        "anomalias_resultado": detect_anomalies(kpis, "net_result"),
        "credito": extract_credit_facts(gold("credit_risk_portfolio")),
        "clientes": extract_customer_facts(gold("customer_360")),
        "fraude": extract_fraud_facts(gold("transaction_fraud_signals"),
                                      cfg.layer_path("artifacts")),
        "lojistas": extract_merchant_facts(gold("merchant_performance")),
    }
    return facts


# ===========================================================================
# 2) Redacao do relatorio (template deterministico)
# ===========================================================================

def render_markdown(facts: Dict[str, Any]) -> str:
    """Monta o relatorio executivo em Markdown a partir dos fatos."""
    fin = facts["financeiro"]["totais"]
    comp = facts["financeiro"].get("comparacao_mensal", {})
    cred = facts["credito"]["totais"]
    cust = facts["clientes"]["totais"]
    fraud = facts["fraude"]["totais"]
    model = facts["fraude"].get("modelo", {})

    def trend(value: Any) -> str:
        try:
            value = float(value)
        except (TypeError, ValueError):
            return "estavel"
        if value > 0.05:
            return f"alta de {_pct(value, 1)}"
        if value < -0.05:
            return f"queda de {_pct(abs(value), 1)}"
        return f"estabilidade ({_pct(value, 1)})"

    lines: List[str] = []
    add = lines.append

    add(f"# Relatorio analitico automatico - NeoPag")
    add("")
    add(f"*Gerado automaticamente pelo pipeline em {facts['gerado_em']} "
        f"a partir das tabelas da camada Gold.*")
    add("")
    add(f"**Periodo analisado:** {fin['inicio']} a {fin['fim']}")
    add("")

    # ---------------------------------------------------------------- resumo
    add("## 1. Resumo executivo")
    add("")
    add(f"No periodo, a NeoPag processou **{_int(fin['transacoes'])}** transacoes, "
        f"movimentando um TPV de **{_money(fin['tpv'])}**. A receita total foi de "
        f"**{_money(fin['receita_total'])}** ({_money(fin['receita_mdr'])} de MDR e "
        f"{_money(fin['receita_juros'])} de juros da carteira de credito), com perdas de "
        f"**{_money((fin['perda_chargeback'] or 0) + (fin['perda_fraude'] or 0) + (fin['perda_write_off'] or 0))}** "
        f"e recuperacao de {_money(fin['recuperacao'])}. O resultado liquido somou "
        f"**{_money(fin['resultado'])}**, uma margem de {_pct(fin['margem_liquida'], 1)}.")
    add("")
    add(f"O take rate medio ficou em **{_pct(fin['take_rate'], 3)}** e a taxa de aprovacao "
        f"em **{_pct(fin['taxa_aprovacao'], 1)}**.")
    add("")
    if comp:
        add(f"Na comparacao de **{comp['mes_atual']}** contra **{comp['mes_anterior']}**: "
            f"receita com {trend(comp['variacao_receita'])}, TPV com "
            f"{trend(comp['variacao_tpv'])} e resultado com {trend(comp['variacao_resultado'])}.")
        add("")

    # ------------------------------------------------------------- financeiro
    add("## 2. Evolucao mensal")
    add("")
    add("| Mes | TPV | Receita | Perdas | Resultado | Aprovacao |")
    add("|---|---|---|---|---|---|")
    for row in facts["financeiro"]["serie_mensal"]:
        add(f"| {row['year_month']} | {_money(row['tpv'])} | {_money(row['receita'])} | "
            f"{_money(row['perdas'])} | {_money(row['resultado'])} | "
            f"{_pct(row['taxa_aprovacao'], 1)} |")
    add("")

    # -------------------------------------------------------------- anomalias
    anomalies = facts["anomalias_tpv"] + facts["anomalias_resultado"]
    add("## 3. Anomalias detectadas")
    add("")
    if anomalies:
        add("Deteccao por z-score robusto (mediana + MAD), limiar 3,5:")
        add("")
        for item in anomalies[:6]:
            add(f"- **{item['data']}**: `{item['metrica']}` = {_money(item['valor'])}, "
                f"{item['direcao']} (mediana do periodo: {_money(item['mediana_do_periodo'])}, "
                f"z = {item['z_robusto']}).")
    else:
        add("Nenhum dia fora do padrao estatistico esperado no periodo.")
    add("")

    # ---------------------------------------------------------------- credito
    add("## 4. Risco de credito")
    add("")
    add(f"A carteira soma **{_money(cred['ead'])}** de exposicao (EAD) em "
        f"{_int(cred['contratos'])} contratos. O saldo inadimplente (NPL 90+) e de "
        f"{_money(cred['saldo_npl'])}, equivalente a **{_pct(cred['npl_ratio'])}** da carteira. "
        f"A provisao constituida e de {_money(cred['provisao'])}, com indice de cobertura de "
        f"**{float(cred['cobertura'] or 0):.2f}x**. A perda esperada (EAD x PD x LGD) e de "
        f"**{_money(cred['perda_esperada'])}**.")
    add("")
    if facts["credito"]["piores_safras"]:
        add("**Safras que exigem atencao:**")
        add("")
        add("| Safra | Produto | Faixa | Contratos | EAD | PD observada | Alerta |")
        add("|---|---|---|---|---|---|---|")
        for row in facts["credito"]["piores_safras"]:
            add(f"| {row['vintage']} | {row['product']} | {row['customer_risk_band']} | "
                f"{row['contracts']} | {_money(row['ead'])} | {_pct(row['pd_observed'], 1)} | "
                f"{row['portfolio_alert']} |")
        add("")

    # --------------------------------------------------------------- clientes
    add("## 5. Base de clientes")
    add("")
    add(f"Sao **{_int(cust['clientes'])}** clientes ativos na base, com score medio de "
        f"**{cust['score_medio']}**. Estao em risco de churn "
        f"**{_int(cust['em_risco_de_churn'])}** clientes "
        f"(**{_pct(cust['taxa_de_churn'], 1)}** da base) e "
        f"{_int(cust['clientes_inadimplentes'])} apresentam inadimplencia acima de 90 dias. "
        f"Os 10% maiores clientes concentram **{_pct(facts['clientes']['concentracao_top_decil'], 1)}** "
        f"do volume dos ultimos 90 dias.")
    add("")
    add("| Segmento de valor/risco | Clientes | TPV 90d | Score medio |")
    add("|---|---|---|---|")
    for row in facts["clientes"]["por_segmento_de_valor"]:
        add(f"| {row['value_risk_segment']} | {row['clientes']} | {_money(row['tpv_90d'])} | "
            f"{row['score_medio']} |")
    add("")

    # ----------------------------------------------------------------- fraude
    add("## 6. Antifraude")
    add("")
    add(f"Foram identificadas **{_int(fraud['fraudes'])}** transacoes fraudulentas "
        f"({_pct(fraud['taxa_de_fraude'], 3)} do volume), somando "
        f"**{_money(fraud['valor_em_fraude'])}**. O motor de regras gerou "
        f"{_int(fraud['alertas'])} alertas, com precisao de "
        f"**{_pct(fraud['precisao_regras'], 1)}** e recall de "
        f"**{_pct(fraud['recall_regras'], 1)}**.")
    add("")
    if model:
        op = model.get("ponto_de_operacao") or {}
        add(f"O modelo de ML (`{model.get('algoritmo')}`) atingiu **PR-AUC de "
            f"{model.get('pr_auc')}** e ROC-AUC de {model.get('roc_auc')}. No ponto de operacao "
            f"otimizado por custo (limiar {op.get('limiar')}), entrega precisao de "
            f"{_pct(op.get('precisao'), 1)} e recall de {_pct(op.get('recall'), 1)}, "
            f"com ganho financeiro de {_money(model.get('ganho_sobre_baseline'))} sobre o "
            f"motor de regras.")
        add("")
        if model.get("top_features"):
            add("Principais features do modelo: "
                + ", ".join(f"`{f['feature']}`" for f in model["top_features"]) + ".")
            add("")
    if facts["fraude"]["por_padrao"]:
        add("| Padrao de fraude | Ocorrencias | Valor | Score medio das regras |")
        add("|---|---|---|---|")
        for row in facts["fraude"]["por_padrao"]:
            add(f"| {row['fraud_type'] or 'nao classificado'} | {row['ocorrencias']} | "
                f"{_money(row['valor'])} | {row['score_medio_regras']} |")
        add("")

    # --------------------------------------------------------------- lojistas
    add("## 7. Carteira de estabelecimentos")
    add("")
    if facts["lojistas"]["em_alerta"]:
        add("**Lojistas em alerta de risco:**")
        add("")
        add("| Estabelecimento | Categoria | Situacao | TPV | Chargeback | Fraude |")
        add("|---|---|---|---|---|---|")
        for row in facts["lojistas"]["em_alerta"]:
            add(f"| {row['merchant_name']} | {row['category']} | {row['merchant_health']} | "
                f"{_money(row['tpv'])} | {_pct(row['taxa_chargeback'], 2)} | "
                f"{_pct(row['taxa_fraude'], 2)} |")
        add("")
    add("**Maiores por volume:**")
    add("")
    for row in facts["lojistas"]["maiores_por_tpv"]:
        add(f"- {row['merchant_name']} ({row['category']}): {_money(row['tpv'])} de TPV, "
            f"{_money(row['receita'])} de receita.")
    add("")

    # --------------------------------------------------------- recomendacoes
    add("## 8. Recomendacoes automaticas")
    add("")
    for recommendation in build_recommendations(facts):
        add(f"- {recommendation}")
    add("")
    add("---")
    add("")
    add("*Todos os numeros deste relatorio sao extraidos diretamente das tabelas Delta da "
        "camada Gold. A redacao e gerada por template deterministico; os fatos podem ser "
        "auditados pelas queries em `src/ai/insight_generator.py`.*")

    return "\n".join(lines)


def build_recommendations(facts: Dict[str, Any]) -> List[str]:
    """Regras de negocio que transformam os fatos em acoes sugeridas."""
    recommendations: List[str] = []

    cred = facts["credito"]["totais"]
    cust = facts["clientes"]["totais"]
    fraud = facts["fraude"]["totais"]
    fin = facts["financeiro"]["totais"]

    if (cred.get("cobertura") or 0) < 1.0:
        recommendations.append(
            f"**Credito:** o indice de cobertura esta em {float(cred['cobertura']):.2f}x - "
            "abaixo de 1x. Revisar os percentuais de provisao por faixa de atraso antes do "
            "proximo fechamento contabil.")
    if (cred.get("npl_ratio") or 0) > 0.05:
        recommendations.append(
            f"**Credito:** NPL de {_pct(cred['npl_ratio'])} acima do patamar confortavel de 5%. "
            "Priorizar acao de cobranca nas safras sinalizadas como criticas.")
    if (cust.get("taxa_de_churn") or 0) > 0.10:
        recommendations.append(
            f"**Retencao:** {_pct(cust['taxa_de_churn'], 1)} da base esta inativa. Ativar "
            "campanha para o segmento `em_risco_de_churn`, que ja vem segmentado na "
            "`gold.customer_360`.")
    if (fraud.get("recall_regras") or 0) < 0.5:
        recommendations.append(
            f"**Antifraude:** o motor de regras captura apenas {_pct(fraud['recall_regras'], 1)} "
            "das fraudes. Promover o modelo de ML a decisor primario, mantendo as regras como "
            "camada de seguranca e explicabilidade.")
    if (fin.get("take_rate") or 0) < 0.015:
        recommendations.append(
            f"**Receita:** take rate de {_pct(fin['take_rate'], 3)}. Avaliar o mix de canais - "
            "PIX cresce em volume mas nao remunera como o cartao.")

    risky_merchants = facts["lojistas"]["em_alerta"]
    if risky_merchants:
        recommendations.append(
            f"**Adquirencia:** {len(risky_merchants)} estabelecimentos acima do limiar de "
            "chargeback das bandeiras. Iniciar plano de acao ou descredenciamento.")

    if not recommendations:
        recommendations.append("Nenhum indicador fora dos limites definidos. Manter monitoramento.")
    return recommendations


# ===========================================================================
# 3) Execucao
# ===========================================================================

def run() -> Dict[str, Any]:
    cfg = get_settings()
    spark = get_spark("ai_insight_generator")

    with log_stage(log, "ai.insight_generator"):
        facts = collect_facts(spark)
        report = render_markdown(facts)

        reports_dir = cfg.layer_path("reports")
        reports_dir.mkdir(parents=True, exist_ok=True)

        today = date.today().isoformat()
        report_path = reports_dir / f"relatorio_analitico_{today}.md"
        facts_path = reports_dir / f"fatos_{today}.json"

        report_path.write_text(report, encoding="utf-8")
        facts_path.write_text(json.dumps(facts, indent=2, ensure_ascii=False, default=str),
                              encoding="utf-8")

        log.info("Relatorio gerado: %s (%s linhas)", report_path, len(report.splitlines()))
        log.info("Fatos estruturados: %s", facts_path)
        return {"relatorio": str(report_path), "fatos": str(facts_path),
                "recomendacoes": len(build_recommendations(facts))}


if __name__ == "__main__":
    run()
