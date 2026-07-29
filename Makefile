# ===========================================================================
# Atalhos do projeto. Rode `make help` para ver tudo.
# ===========================================================================

PYTHON ?= python
VENV   ?= .venv
PY     := $(VENV)/bin/python
export PYTHONPATH := .

.DEFAULT_GOAL := help
.PHONY: help setup data bronze silver gold ml ai quality contracts pipeline ci \
        maintenance demo rag report clean clean-data test lint format

help:  ## Mostra esta ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup:  ## Cria o ambiente virtual e instala as dependencias
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip setuptools wheel
	$(PY) -m pip install -r requirements.txt
	@echo "Ambiente pronto. Rode: make pipeline"

data:  ## Gera os dados sinteticos da fintech na camada raw
	$(PY) -m src.generators.generate_synthetic_data

bronze:  ## Ingestao Bronze (batch + streaming)
	$(PY) -m src.bronze.bronze_ingestion
	$(PY) -m src.bronze.bronze_transactions_stream

silver:  ## Camada Silver completa
	$(PY) -m src.silver.silver_customers
	$(PY) -m src.silver.silver_merchants
	$(PY) -m src.silver.silver_transactions
	$(PY) -m src.silver.silver_credit_contracts

gold:  ## Camada Gold completa
	$(PY) -m src.gold.gold_customer_360
	$(PY) -m src.gold.gold_transaction_fraud_signals
	$(PY) -m src.gold.gold_credit_risk_portfolio
	$(PY) -m src.gold.gold_financial_kpis_daily
	$(PY) -m src.gold.gold_merchant_performance

ml:  ## Feature store + treino do modelo de fraude
	$(PY) -m src.ml.feature_store
	$(PY) -m src.ml.train_fraud_model

ai:  ## Relatorio analitico automatico + indice RAG
	$(PY) -m src.ai.insight_generator
	$(PY) -m src.ai.rag_pipeline --build

quality:  ## Portao de qualidade completo (constraints, contratos, conciliacao, drift, scorecard)
	$(PY) -m src.quality.constraints
	$(PY) -m src.quality.schema_contract
	$(PY) -m src.quality.expectations
	$(PY) -m src.quality.reconciliation
	$(PY) -m src.quality.drift
	$(PY) -m src.quality.scorecard

contracts:  ## Congela o schema atual das tabelas como novo contrato (gera diff no Git)
	$(PY) -m src.quality.schema_contract --update

pipeline:  ## Executa o pipeline inteiro (recomendado)
	$(PY) -m orchestration.run_pipeline

ci:  ## Roda o pipeline como o CI roda: perfil reduzido, ~4 min
	LAKEHOUSE_PROFILE=ci LAKEHOUSE_ROOT=data-ci \
		$(PY) -m orchestration.run_pipeline --skip delta_maintenance
	LAKEHOUSE_PROFILE=ci LAKEHOUSE_ROOT=data-ci $(PY) -m pytest tests/ -q

maintenance:  ## OPTIMIZE + Z-ORDER nas tabelas Delta
	$(PY) -m src.maintenance.delta_maintenance

demo:  ## Demonstra time travel, schema evolution, MERGE, RESTORE e constraints
	$(PY) -m src.maintenance.delta_features_demo

rag:  ## Bateria de perguntas em linguagem natural sobre a camada Gold
	$(PY) -m src.ai.rag_pipeline --demo

report:  ## Regera apenas o relatorio analitico
	$(PY) -m src.ai.insight_generator

test:  ## Roda os testes automatizados
	$(PY) -m pytest tests/ -v

lint:  ## Lint e formatacao (o mesmo que o CI roda)
	$(PY) -m ruff check src orchestration tests
	$(PY) -m ruff format --check src orchestration tests
	$(PY) -m compileall -q src orchestration tests && echo "Sintaxe OK"

format:  ## Corrige lint e formatacao automaticamente
	$(PY) -m ruff check --fix src orchestration tests
	$(PY) -m ruff format src orchestration tests

clean:  ## Remove caches do Python
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache

clean-data:  ## Apaga TODAS as camadas de dados (raw, bronze, silver, gold...)
	rm -rf data data-ci artifacts reports docs/generated
	@echo "Camadas de dados removidas. Rode `make pipeline` para recriar."
