-- =====================================================================
-- Setup do Unity Catalog equivalente ao catalogo declarativo do projeto.
-- Gerado por src/governance/apply_catalog_metadata.py
-- =====================================================================

CREATE CATALOG IF NOT EXISTS fintech
  MANAGED LOCATION 's3://neopag-lakehouse/fintech';

CREATE SCHEMA IF NOT EXISTS fintech.bronze;
CREATE SCHEMA IF NOT EXISTS fintech.silver;
CREATE SCHEMA IF NOT EXISTS fintech.gold;
CREATE SCHEMA IF NOT EXISTS fintech.feature_store;

-- fintech.bronze.customers
COMMENT ON TABLE fintech.bronze.customers IS 'Cadastro bruto de clientes ingerido do CSV operacional, sem tratamento.';
ALTER TABLE fintech.bronze.customers SET TAGS ('classificacao' = 'PII', 'dominio' = 'customer', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.bronze.customers TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.bronze.customers TO `dpo`;
ALTER TABLE fintech.bronze.customers ALTER COLUMN full_name SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.bronze.customers ALTER COLUMN cpf SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.bronze.customers ALTER COLUMN email SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.bronze.customers ALTER COLUMN phone SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.bronze.customers ALTER COLUMN birth_date SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');

-- fintech.bronze.merchants
COMMENT ON TABLE fintech.bronze.merchants IS 'Cadastro bruto de estabelecimentos credenciados.';
ALTER TABLE fintech.bronze.merchants SET TAGS ('classificacao' = 'INTERNAL', 'dominio' = 'payments', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.bronze.merchants TO `analistas`;
GRANT SELECT ON TABLE fintech.bronze.merchants TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.bronze.merchants TO `ciencia_dados`;

-- fintech.bronze.transactions
COMMENT ON TABLE fintech.bronze.transactions IS 'Transacoes brutas (batch + streaming), particionadas por data de ingestao.';
ALTER TABLE fintech.bronze.transactions SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'payments', 'sla_horas' = '1');
GRANT SELECT ON TABLE fintech.bronze.transactions TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.bronze.transactions TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.bronze.transactions TO `risco`;
ALTER TABLE fintech.bronze.transactions ALTER COLUMN ip_address SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.bronze.transactions ALTER COLUMN device_id SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');

-- fintech.bronze.credit_contracts
COMMENT ON TABLE fintech.bronze.credit_contracts IS 'Contratos de credito brutos (emprestimo pessoal, cartao, consignado, BNPL).';
ALTER TABLE fintech.bronze.credit_contracts SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'credit', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.bronze.credit_contracts TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.bronze.credit_contracts TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.bronze.credit_contracts TO `risco`;

-- fintech.silver.customers
COMMENT ON TABLE fintech.silver.customers IS 'Clientes deduplicados, normalizados e validados. CPF e e-mail mascarados.';
ALTER TABLE fintech.silver.customers SET TAGS ('classificacao' = 'PII', 'dominio' = 'customer', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.silver.customers TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.silver.customers TO `dpo`;
ALTER TABLE fintech.silver.customers ALTER COLUMN full_name SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.silver.customers ALTER COLUMN cpf_masked SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
ALTER TABLE fintech.silver.customers ALTER COLUMN email_masked SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');

-- fintech.silver.merchants
COMMENT ON TABLE fintech.silver.merchants IS 'Estabelecimentos normalizados e enriquecidos com categoria de MCC.';
ALTER TABLE fintech.silver.merchants SET TAGS ('classificacao' = 'INTERNAL', 'dominio' = 'payments', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.silver.merchants TO `analistas`;
GRANT SELECT ON TABLE fintech.silver.merchants TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.silver.merchants TO `ciencia_dados`;

-- fintech.silver.transactions
COMMENT ON TABLE fintech.silver.transactions IS 'Transacoes limpas, tipadas, deduplicadas e enriquecidas com cliente e merchant.';
ALTER TABLE fintech.silver.transactions SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'payments', 'sla_horas' = '1');
GRANT SELECT ON TABLE fintech.silver.transactions TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.silver.transactions TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.silver.transactions TO `risco`;

-- fintech.silver.credit_contracts
COMMENT ON TABLE fintech.silver.credit_contracts IS 'Contratos de credito tipados, com faixa de atraso e provisao calculada.';
ALTER TABLE fintech.silver.credit_contracts SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'credit', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.silver.credit_contracts TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.silver.credit_contracts TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.silver.credit_contracts TO `risco`;

-- fintech.gold.customer_360
COMMENT ON TABLE fintech.gold.customer_360 IS 'Visao unica do cliente: RFM, exposicao de credito, inadimplencia, churn e score agregado.';
ALTER TABLE fintech.gold.customer_360 SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'customer', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.gold.customer_360 TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.gold.customer_360 TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.gold.customer_360 TO `risco`;

-- fintech.gold.transaction_fraud_signals
COMMENT ON TABLE fintech.gold.transaction_fraud_signals IS 'Transacoes com sinais de fraude por regra (velocity, valor atipico, madrugada, device novo).';
ALTER TABLE fintech.gold.transaction_fraud_signals SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'payments', 'sla_horas' = '1');
GRANT SELECT ON TABLE fintech.gold.transaction_fraud_signals TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.gold.transaction_fraud_signals TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.gold.transaction_fraud_signals TO `risco`;

-- fintech.gold.credit_risk_portfolio
COMMENT ON TABLE fintech.gold.credit_risk_portfolio IS 'Carteira de credito por safra/produto com EAD, PD proxy, LGD e perda esperada.';
ALTER TABLE fintech.gold.credit_risk_portfolio SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'credit', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.gold.credit_risk_portfolio TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.gold.credit_risk_portfolio TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.gold.credit_risk_portfolio TO `risco`;

-- fintech.gold.financial_kpis_daily
COMMENT ON TABLE fintech.gold.financial_kpis_daily IS 'KPIs financeiros diarios: receita (MDR + juros), perdas, recuperacao, chargeback e aprovacao.';
ALTER TABLE fintech.gold.financial_kpis_daily SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'finance', 'sla_horas' = '6');
GRANT SELECT ON TABLE fintech.gold.financial_kpis_daily TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.gold.financial_kpis_daily TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.gold.financial_kpis_daily TO `risco`;

-- fintech.gold.merchant_performance
COMMENT ON TABLE fintech.gold.merchant_performance IS 'Desempenho e risco por estabelecimento: TPV, ticket medio, taxa de chargeback e de fraude.';
ALTER TABLE fintech.gold.merchant_performance SET TAGS ('classificacao' = 'INTERNAL', 'dominio' = 'payments', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.gold.merchant_performance TO `analistas`;
GRANT SELECT ON TABLE fintech.gold.merchant_performance TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.gold.merchant_performance TO `ciencia_dados`;

-- fintech.feature_store.customer_features
COMMENT ON TABLE fintech.feature_store.customer_features IS 'Features de cliente com timestamp de referencia para treino point-in-time.';
ALTER TABLE fintech.feature_store.customer_features SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'ml', 'sla_horas' = '24');
GRANT SELECT ON TABLE fintech.feature_store.customer_features TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.feature_store.customer_features TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.feature_store.customer_features TO `risco`;

-- fintech.feature_store.transaction_features
COMMENT ON TABLE fintech.feature_store.transaction_features IS 'Features transacionais para o modelo de deteccao de fraude.';
ALTER TABLE fintech.feature_store.transaction_features SET TAGS ('classificacao' = 'CONFIDENTIAL', 'dominio' = 'ml', 'sla_horas' = '1');
GRANT SELECT ON TABLE fintech.feature_store.transaction_features TO `engenharia_dados`;
GRANT SELECT ON TABLE fintech.feature_store.transaction_features TO `ciencia_dados`;
GRANT SELECT ON TABLE fintech.feature_store.transaction_features TO `risco`;

-- Mascaramento dinamico de PII: quem nao esta no grupo ve o dado mascarado.
CREATE OR REPLACE FUNCTION mask_pii(valor STRING)
  RETURN CASE WHEN is_account_group_member('dpo') THEN valor ELSE '***' END;

ALTER TABLE fintech.silver.customers ALTER COLUMN full_name SET MASK mask_pii;
