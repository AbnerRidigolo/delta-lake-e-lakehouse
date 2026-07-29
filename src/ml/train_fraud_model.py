"""
Modelo de deteccao de fraude treinado sobre a feature store.

Decisoes de modelagem (e o porque de cada uma)
----------------------------------------------
* **Split temporal, nao aleatorio.** Fraude evolui: o padrao de dezembro nao e
  o de janeiro. Um split aleatorio deixa transacoes do futuro no treino e
  produz uma metrica bonita que nao se sustenta em producao. Aqui treinamos no
  passado e testamos no futuro - como o modelo sera usado de fato.

* **Metrica principal: PR-AUC, nao ROC-AUC.** Com ~1% de fraude, ROC-AUC fica
  alta ate para modelo ruim. A curva precisao-recall e o que reflete a dor real
  da operacao (quantos alertas falsos por fraude capturada).

* **Baseline explicito.** O modelo e comparado com o motor de regras da Gold no
  MESMO conjunto de teste. Modelo que nao bate a regra nao vai para producao -
  e um resultado legitimo, nao um fracasso.

* **Ponto de operacao escolhido por custo.** Em vez de usar o limiar 0.5
  (arbitrario), escolhemos o limiar que maximiza o resultado financeiro dado o
  custo de uma revisao manual e o custo de uma fraude nao capturada.

Saidas
------
    artifacts/fraud_model.joblib          - pipeline treinado (pronto para servir)
    artifacts/fraud_model_metrics.json    - metricas, limiares e importancias
    feature_store.fraud_model_scores      - scores por transacao (tabela Delta)

Execucao:
    python -m src.ml.train_fraud_model
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from src.common.config import get_settings
from src.common.delta_io import register_table, write_delta
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark
from src.ml.feature_store import build_training_set

log = get_logger(__name__)

TARGET = "is_fraud"
SCORES_TABLE = "fraud_model_scores"

# Custos usados para escolher o ponto de operacao (valores tipicos de mercado,
# ajustaveis pela operacao).
COST_MANUAL_REVIEW = 12.0  # R$ por transacao enviada para analise humana
FRAUD_RECOVERY_RATE = 0.85  # fracao do valor salva quando a fraude e barrada


# ===========================================================================
# Preparacao do dataset
# ===========================================================================


def load_dataset() -> pd.DataFrame:
    """Materializa o training set da feature store em pandas."""
    spark = get_spark("train_fraud_model")
    training = build_training_set(spark)

    pdf = training.toPandas()
    log.info(
        "Dataset carregado: %s linhas x %s colunas | fraudes: %s (%.2f%%)",
        len(pdf),
        pdf.shape[1],
        int(pdf[TARGET].sum()),
        100 * pdf[TARGET].mean(),
    )
    return pdf


def split_features(pdf: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Separa as colunas de feature em numericas e categoricas.

    Convencao do projeto: features de transacao tem prefixo `tf_`, features de
    cliente tem prefixo `cf_`. Qualquer outra coluna e chave, rotulo ou
    metadado - e fica de fora por construcao, o que elimina uma classe inteira
    de vazamento acidental.
    """
    feature_columns = [c for c in pdf.columns if c.startswith(("tf_", "cf_"))]
    numeric = [c for c in feature_columns if pd.api.types.is_numeric_dtype(pdf[c])]
    categorical = [c for c in feature_columns if c not in numeric]
    log.info("Features: %s numericas, %s categoricas", len(numeric), len(categorical))
    return numeric, categorical


def temporal_split(
    pdf: pd.DataFrame, train_ratio: float = 0.75
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Divide o dataset no tempo: passado para treino, futuro para teste."""
    pdf = pdf.sort_values("event_ts").reset_index(drop=True)
    cut_index = int(len(pdf) * train_ratio)
    cut_date = pdf.loc[cut_index, "event_ts"]
    train = pdf[pdf["event_ts"] < cut_date]
    test = pdf[pdf["event_ts"] >= cut_date]
    log.info(
        "Split temporal em %s | treino=%s (%.2f%% fraude) | teste=%s (%.2f%% fraude)",
        cut_date,
        len(train),
        100 * train[TARGET].mean(),
        len(test),
        100 * test[TARGET].mean(),
    )
    return train, test


# ===========================================================================
# Modelo
# ===========================================================================


def build_pipeline(numeric: list[str], categorical: list[str], scale_pos_weight: float):
    """Monta o pipeline de pre-processamento + classificador.

    Usa XGBoost quando disponivel (padrao de mercado para tabular) e cai para o
    `HistGradientBoostingClassifier` do scikit-learn caso contrario - assim o
    projeto roda em qualquer ambiente sem dependencia extra.
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder

    preprocessor = ColumnTransformer(
        transformers=[
            # Arvores nao precisam de escala, mas precisam de valor preenchido.
            ("num", SimpleImputer(strategy="median"), numeric),
            (
                "cat",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="constant", fill_value="desconhecido")),
                        (
                            "onehot",
                            OneHotEncoder(
                                handle_unknown="ignore", min_frequency=20, sparse_output=False
                            ),
                        ),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )

    try:
        from xgboost import XGBClassifier

        model = XGBClassifier(
            n_estimators=400,
            max_depth=5,
            learning_rate=0.08,
            subsample=0.9,
            colsample_bytree=0.8,
            reg_lambda=1.5,
            # Compensa o desbalanceamento sem jogar dado fora (nada de undersampling).
            scale_pos_weight=scale_pos_weight,
            eval_metric="aucpr",
            n_jobs=-1,
            random_state=42,
            tree_method="hist",
        )
        algorithm = "XGBClassifier"
    except ImportError:
        from sklearn.ensemble import HistGradientBoostingClassifier

        model = HistGradientBoostingClassifier(
            max_iter=400,
            max_depth=6,
            learning_rate=0.08,
            l2_regularization=1.0,
            class_weight="balanced",
            random_state=42,
        )
        algorithm = "HistGradientBoostingClassifier"

    log.info("Algoritmo selecionado: %s", algorithm)
    return Pipeline([("prep", preprocessor), ("model", model)]), algorithm


# ===========================================================================
# Avaliacao
# ===========================================================================


def evaluate(y_true: np.ndarray, y_score: np.ndarray, amounts: np.ndarray) -> dict:
    """Metricas estatisticas + a leitura financeira do modelo."""
    from sklearn.metrics import average_precision_score, roc_auc_score

    metrics = {
        "roc_auc": round(float(roc_auc_score(y_true, y_score)), 4),
        "pr_auc": round(float(average_precision_score(y_true, y_score)), 4),
        "prevalencia_fraude": round(float(y_true.mean()), 5),
    }

    # Varredura de limiares: e assim que a area de risco escolhe o ponto de corte.
    sweep = []
    for threshold in np.arange(0.05, 0.96, 0.05):
        flagged = y_score >= threshold
        n_flagged = int(flagged.sum())
        if n_flagged == 0:
            continue
        tp = int((flagged & (y_true == 1)).sum())
        precision = tp / n_flagged
        recall = tp / max(int(y_true.sum()), 1)
        # Beneficio = valor de fraude barrado; custo = analistas revisando alertas.
        value_saved = float(amounts[flagged & (y_true == 1)].sum()) * FRAUD_RECOVERY_RATE
        review_cost = n_flagged * COST_MANUAL_REVIEW
        sweep.append(
            {
                "limiar": round(float(threshold), 2),
                "sinalizadas": n_flagged,
                "taxa_de_revisao": round(n_flagged / len(y_true), 4),
                "precisao": round(precision, 4),
                "recall": round(recall, 4),
                "f1": round(2 * precision * recall / max(precision + recall, 1e-9), 4),
                "valor_salvo": round(value_saved, 2),
                "custo_revisao": round(review_cost, 2),
                "resultado_liquido": round(value_saved - review_cost, 2),
            }
        )

    metrics["varredura_de_limiares"] = sweep
    if sweep:
        best = max(sweep, key=lambda row: row["resultado_liquido"])
        metrics["ponto_de_operacao"] = best
        log.info(
            "Melhor ponto de operacao: limiar=%s | precisao=%.3f | recall=%.3f | "
            "resultado liquido=R$ %s",
            best["limiar"],
            best["precisao"],
            best["recall"],
            best["resultado_liquido"],
        )
    return metrics


def evaluate_rule_baseline(test: pd.DataFrame, threshold: int = 45) -> dict:
    """Mesma avaliacao aplicada ao motor de regras, para comparacao justa."""
    flagged = test["fraud_score_rule"] >= threshold
    n_flagged = int(flagged.sum())
    tp = int((flagged & (test[TARGET] == 1)).sum())
    value_saved = (
        float(test.loc[flagged & (test[TARGET] == 1), "tf_amount"].sum()) * FRAUD_RECOVERY_RATE
    )
    return {
        "limiar": threshold,
        "sinalizadas": n_flagged,
        "taxa_de_revisao": round(n_flagged / max(len(test), 1), 4),
        "precisao": round(tp / n_flagged, 4) if n_flagged else 0.0,
        "recall": round(tp / max(int(test[TARGET].sum()), 1), 4),
        "resultado_liquido": round(value_saved - n_flagged * COST_MANUAL_REVIEW, 2),
    }


def extract_feature_importance(
    pipeline, X_test: pd.DataFrame, y_test: pd.Series, top_n: int = 20
) -> list[dict]:
    """Extrai a importancia das features, ja com os nomes pos one-hot.

    Usa a importancia nativa quando o algoritmo expoe uma (caso do XGBoost) e
    cai para **permutation importance** caso contrario (caso do
    HistGradientBoosting). A permutacao e mais cara, mas mede o que realmente
    interessa: quanto a metrica piora quando aquela feature vira ruido.
    """
    model = pipeline.named_steps["model"]
    native = getattr(model, "feature_importances_", None)

    if native is not None:
        names = pipeline.named_steps["prep"].get_feature_names_out()
        ranking = sorted(zip(names, native), key=lambda item: item[1], reverse=True)
        return [
            {"feature": str(name), "importancia": round(float(value), 5), "metodo": "nativa"}
            for name, value in ranking[:top_n]
            if value > 0
        ]

    try:
        from sklearn.inspection import permutation_importance

        # A permutacao reavalia o modelo (n_features x n_repeats) vezes. Em uma
        # amostra estratificada de ate 20 mil linhas o ranking ja e estavel e o
        # calculo cai de minutos para segundos.
        if len(X_test) > 20_000:
            sample = X_test.sample(n=20_000, random_state=42)
            X_sample, y_sample = sample, y_test.loc[sample.index]
        else:
            X_sample, y_sample = X_test, y_test

        result = permutation_importance(
            pipeline,
            X_sample,
            y_sample,
            n_repeats=3,
            random_state=42,
            scoring="average_precision",
            n_jobs=-1,
        )
        ranking = sorted(
            zip(X_sample.columns, result.importances_mean), key=lambda item: item[1], reverse=True
        )
        return [
            {"feature": str(name), "importancia": round(float(value), 5), "metodo": "permutacao"}
            for name, value in ranking[:top_n]
            if value > 0
        ]
    except Exception as exc:  # pragma: no cover - depende do ambiente
        log.warning("Nao foi possivel calcular a importancia das features: %s", exc)
        return []


# ===========================================================================
# Execucao
# ===========================================================================


def run() -> dict:
    cfg = get_settings()
    artifacts = cfg.layer_path("artifacts")
    artifacts.mkdir(parents=True, exist_ok=True)

    with log_stage(log, "ml.train_fraud_model"):
        pdf = load_dataset()
        numeric, categorical = split_features(pdf)
        train, test = temporal_split(pdf)

        positives = max(int(train[TARGET].sum()), 1)
        scale_pos_weight = (len(train) - positives) / positives
        pipeline, algorithm = build_pipeline(numeric, categorical, scale_pos_weight)

        feature_columns = numeric + categorical
        pipeline.fit(train[feature_columns], train[TARGET])

        y_score = pipeline.predict_proba(test[feature_columns])[:, 1]
        model_metrics = evaluate(test[TARGET].to_numpy(), y_score, test["tf_amount"].to_numpy())
        baseline_metrics = evaluate_rule_baseline(test)

        operating_point = model_metrics.get("ponto_de_operacao", {})
        uplift = round(
            operating_point.get("resultado_liquido", 0.0) - baseline_metrics["resultado_liquido"], 2
        )
        log.info(
            "Modelo x regras -> recall %.3f vs %.3f | ganho financeiro R$ %s",
            operating_point.get("recall", 0.0),
            baseline_metrics["recall"],
            uplift,
        )

        report = {
            "treinado_em": datetime.now(timezone.utc).isoformat(),
            "algoritmo": algorithm,
            "linhas_treino": len(train),
            "linhas_teste": len(test),
            "features_numericas": numeric,
            "features_categoricas": categorical,
            "metricas_modelo": model_metrics,
            "baseline_regras": baseline_metrics,
            "ganho_sobre_baseline": uplift,
            "importancia_features": extract_feature_importance(
                pipeline, test[feature_columns], test[TARGET]
            ),
        }

        # --- Persistencia dos artefatos --------------------------------------
        import joblib

        model_path = artifacts / "fraud_model.joblib"
        joblib.dump({"pipeline": pipeline, "features": feature_columns}, model_path)
        metrics_path = artifacts / "fraud_model_metrics.json"
        metrics_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        log.info("Modelo salvo em %s | metricas em %s", model_path, metrics_path)

        # --- Scores de volta para o lakehouse --------------------------------
        # Fechar o ciclo importa: o score precisa estar disponivel para o BI e
        # para o monitoramento de drift, nao so dentro de um notebook.
        spark = get_spark("train_fraud_model")
        threshold = float(operating_point.get("limiar", 0.5))
        scores = test[
            ["transaction_id", "customer_id", "event_date", "tf_amount", "fraud_score_rule", TARGET]
        ].copy()
        scores["model_score"] = y_score
        scores["model_flag"] = (scores["model_score"] >= threshold).astype(int)
        scores["model_version"] = f"{algorithm}-{datetime.now(timezone.utc):%Y%m%d}"
        scores = scores.rename(columns={"tf_amount": "amount"})

        scores_df = spark.createDataFrame(scores)
        path = cfg.table_path("feature_store", SCORES_TABLE)
        write_delta(
            scores_df,
            path,
            mode="overwrite",
            overwrite_schema=True,
            comment=f"scores do modelo de fraude ({algorithm})",
        )
        register_table(spark, cfg.full_table_name("feature_store", SCORES_TABLE), path)

        return report


if __name__ == "__main__":
    run()
