# Dicionario de dados

Descricao das principais colunas de cada tabela do lakehouse. As colunas
tecnicas (prefixo `_`) sao comuns a todas as tabelas e estao no fim do documento.

> O catalogo com dominio, owner, classificacao e SLA de cada tabela e gerado
> automaticamente em [`docs/generated/data_catalog.md`](generated/data_catalog.md)
> a partir de `config/catalog.yaml`.

---

## Camada RAW

### `data/raw/customers/customers.csv`
Cadastro de clientes extraido do core banking / CRM.

| Coluna | Tipo logico | Descricao |
|---|---|---|
| `customer_id` | texto | Chave primaria (`CUST-000001`) |
| `full_name`, `cpf`, `email`, `phone` | texto | **PII** — mascarados na Silver |
| `birth_date` | data | Data de nascimento |
| `city`, `state`, `region`, `country` | texto | Endereco de residencia |
| `signup_date` | data | Data de abertura da conta (define a safra de aquisicao) |
| `acquisition_channel` | texto | Canal de aquisicao (app organico, indicacao, parceria...) |
| `segment` | texto | `varejo`, `premium`, `private`, `pj_micro` |
| `monthly_income` | decimal | Renda mensal declarada |
| `credit_limit` | decimal | Limite de credito aprovado |
| `risk_band` | texto | Faixa de risco do onboarding: `A` (melhor) a `E` (pior) |
| `status` | texto | `active`, `inactive`, `blocked` |

### `data/raw/transactions/transactions_YYYY-MM.csv`
Historico transacional (batch, um arquivo por mes).

| Coluna | Tipo logico | Descricao |
|---|---|---|
| `transaction_id` | texto | Chave primaria (`TXN-000000001`) |
| `event_ts` | timestamp | Momento da transacao |
| `customer_id`, `merchant_id` | texto | Chaves estrangeiras |
| `mcc` | texto | Merchant Category Code (4 digitos) |
| `amount` | decimal | Valor em BRL |
| `channel` | texto | `pos`, `ecommerce`, `pix`, `wallet`, `boleto`, `ted`, `atm` |
| `payment_method` | texto | `credit_card`, `debit_card`, `pix`, `boleto`, `ted` |
| `installments` | inteiro | Numero de parcelas |
| `device_id`, `ip_address` | texto | Identificadores tecnicos usados no antifraude |
| `geo_city`, `geo_state` | texto | Localizacao da transacao |
| `status` | texto | `approved`, `declined`, `reversed`, `chargeback` |
| `is_fraud` | inteiro | Rotulo (0/1) — confirmado a posteriori pela area de risco |
| `fraud_type` | texto | `card_testing`, `account_takeover`, `merchant_collusion`, `cnp_high_ticket` |

### `data/raw/streaming/transactions/events_batch_NNN.json`
Mesmo schema acima, em JSONL. Representa os ultimos 7 dias chegando em
micro-lotes, como um topico Kafka despejado no storage.

### `data/raw/merchants/merchants.csv`
Cadastro de estabelecimentos credenciados: `merchant_id`, `mcc`, `category`,
`avg_ticket`, `risk_score`, `risk_flag`, `mdr_rate` (taxa de adquirencia),
`is_active`.

### `data/raw/credit_contracts/credit_contracts.csv`
Carteira de credito: `contract_id`, `product`, `principal_amount`,
`interest_rate_month`, `term_months`, `origination_date`, `installments_paid`,
`outstanding_balance`, `days_past_due`, `status`, `recovery_amount`.

---

## Camada SILVER

### `silver.customers`
Alem das colunas normalizadas da raw:

| Coluna | Descricao |
|---|---|
| `customer_key` | SHA-256 do CPF com salt — permite join sem expor o documento |
| `cpf_masked`, `email_masked` | Versoes mascaradas (LGPD) |
| `age`, `age_band` | Idade e faixa etaria |
| `tenure_days`, `tenure_band` | Tempo de relacionamento |
| `signup_cohort` | Safra de aquisicao (`YYYY-MM`) |
| `limit_to_income_ratio` | Limite dividido pela renda — proxy de alavancagem |
| `income_band` | Faixa de renda |

### `silver.transactions`
A tabela de fatos central. Alem das colunas tipadas:

| Coluna | Descricao |
|---|---|
| `event_date`, `event_hour`, `event_dow`, `event_month` | Decomposicao temporal |
| `is_night`, `is_weekend` | Flags de horario atipico |
| `is_approved` | Status igual a `approved` |
| `installment_amount` | Valor de cada parcela |
| `mdr_revenue` | Receita de adquirencia (`amount x mdr_rate`), so em aprovadas |
| `chargeback_loss` | Valor devolvido ao portador em caso de chargeback |
| `amount_band` | Faixa de valor (`ate_50`, `50_200`, `200_1k`, `1k_5k`, `5k+`) |
| `is_card_not_present` | Canal sem presenca do cartao (`ecommerce`, `wallet`) |
| `is_out_of_home_state` | Compra fora do estado de residencia |
| `amount_vs_merchant_avg` | Quantas vezes o valor excede o ticket medio do lojista |
| `amount_to_income_ratio` | Quanto a compra representa da renda mensal |
| `customer_*`, `merchant_*` | Atributos herdados das dimensoes |

### `silver.credit_contracts`

| Coluna | Descricao |
|---|---|
| `delinquency_bucket` | `current`, `dpd_1_15`, `dpd_16_30`, `dpd_31_60`, `dpd_61_90`, `dpd_91_180`, `dpd_180_plus` |
| `provision_rate` / `provision_amount` | Percentual e valor provisionados (proxy de PDD/ECL) |
| `is_npl` | Atraso >= 90 dias (default) |
| `is_written_off` | Contrato levado a prejuizo (>= 180 dias) |
| `vintage` | Safra de originacao (`YYYY-MM`) |
| `months_on_book` | Meses desde a originacao |
| `monthly_installment` | Parcela pela Tabela Price |
| `debt_to_income` | Comprometimento de renda |
| `exposure_at_default` | EAD — saldo exposto |

### `silver._quarantine.<tabela>`
Registros rejeitados, com `_reject_reason` e `_quarantine_date`. Motivos
possiveis: chave ausente, timestamp invalido, valor nao numerico ou nao
positivo, categoria fora do dominio, chave estrangeira orfa.

---

## Camada GOLD

### `gold.customer_360` — uma linha por cliente

| Grupo | Colunas |
|---|---|
| RFM | `last_transaction_date`, `days_since_last_transaction`, `txn_count_total`, `txn_count_90d`, `tpv_total`, `tpv_90d`, `tpv_30d`, `avg_ticket`, `median_ticket` |
| Qualidade | `approval_rate`, `chargeback_count`, `confirmed_fraud_count`, `mdr_revenue_generated` |
| Diversidade | `distinct_merchants`, `distinct_devices`, `distinct_states`, `active_days` |
| Mix | `pix_share`, `card_not_present_share`, `night_share`, `weekend_share` |
| Credito | `credit_contracts_count`, `credit_outstanding_total`, `credit_provision_total`, `max_days_past_due`, `has_npl`, `has_written_off`, `max_debt_to_income`, `monthly_debt_service` |
| Derivados | `total_exposure`, `limit_utilization`, `spend_trend_ratio`, `is_delinquent` |
| Churn | `is_churn_risk`, `churn_reason` (`ativo`, `inativo_recente`, `inativo_longo`, `nunca transacionou`) |
| Score | `customer_score` (0–1000), `rating` (`AAA`..`D`), `value_risk_segment` |

**Como o `customer_score` e calculado** — parte de 1000 e desconta:
faixa de risco (0 a 360), dias de atraso (ate 180), NPL (200), write-off (150),
comprometimento de renda acima de 40% (80), fraude confirmada (90), chargebacks
(ate 100), inatividade (60). Soma bonus por tempo de casa (ate 60), engajamento
(ate 40) e taxa de aprovacao (ate 40). Resultado limitado a [0, 1000].

### `gold.transaction_fraud_signals` — uma linha por transacao

| Grupo | Colunas |
|---|---|
| Velocity | `txn_count_10min`, `txn_count_24h`, `amount_sum_24h`, `seconds_since_prev_txn` |
| Dispersao | `distinct_devices_24h`, `distinct_states_24h`, `is_new_device` |
| Desvio | `amount_zscore_customer`, `amount_vs_merchant_avg`, `amount_to_income_ratio` |
| Regras | `rule_velocity_burst`, `rule_new_device`, `rule_high_amount`, `rule_device_dispersion`, `rule_amount_outlier_merchant`, `rule_cnp_high_ticket`, `rule_income_incompatible`, `rule_risky_merchant`, `rule_geo_jump`, `rule_night_activity` |
| Decisao | `fraud_score_rule` (0–100), `triggered_rules`, `risk_level`, `recommended_action` |

**Pesos das regras:** velocity 22, dispositivo novo 18, valor alto 15, dispersao
de dispositivos 15, outlier no lojista 12, CNP de alto ticket 12, incompativel
com a renda 10, lojista arriscado 10, salto geografico 10, madrugada 8.

**Acoes:** score >= 70 -> `bloquear`; >= 45 -> `revisar_manualmente`;
>= 20 -> `autenticar_2fa`; abaixo -> `aprovar`.

### `gold.credit_risk_portfolio` — safra x produto x faixa de risco

`contracts`, `customers`, `principal_originated`, `ead`, `npl_contracts`,
`npl_balance`, `npl_ratio`, `provision_amount`, `coverage_ratio`,
`pd_observed`, `lgd_assumption`, `lgd_observed`, `expected_loss`,
`expected_loss_rate`, `written_off_balance`, `recovery_amount`,
`net_recovery_rate`, `portfolio_alert`.

**Perda esperada** = `EAD x PD x LGD`. **LGD por produto:** consignado 25%,
capital de giro PJ 55%, emprestimo pessoal 65%, BNPL 80%, rotativo 85%.

### `gold.financial_kpis_daily` — uma linha por dia

| Grupo | Colunas |
|---|---|
| Volume | `txn_count`, `txn_approved`, `txn_declined`, `txn_chargeback`, `tpv`, `avg_ticket`, `active_customers`, `active_merchants` |
| Receita | `revenue_mdr`, `revenue_interest`, `revenue_total` |
| Perdas | `loss_chargeback`, `loss_fraud`, `loss_write_off`, `loss_total` |
| Recuperacao | `recovery_amount` |
| Resultado | `net_result`, `net_margin` |
| Taxas | `approval_rate`, `chargeback_rate`, `fraud_rate_volume`, `fraud_rate_value`, `take_rate` |
| Credito | `credit_portfolio_balance`, `active_contracts`, `write_off_contracts` |

### `gold.merchant_performance` — lojista x mes

`tpv`, `txn_count`, `avg_ticket`, `mdr_revenue`, `unique_customers`,
`approval_rate`, `chargeback_rate`, `fraud_rate`, `txn_per_customer`,
`revenue_per_customer`, `tpv_growth`, `tpv_ma3`, `merchant_health`,
`is_under_monitoring`.

**`merchant_health`:** `descredenciar` (chargeback >= 3%),
`monitoramento_bandeira` (>= 1% ou fraude > 2%), `estrategico` (TPV alto e
chargeback baixo), `risco_de_evasao` (queda de TPV > 50%), `saudavel`.

---

## Feature Store

### `feature_store.customer_features` — cliente x mes
Prefixo `cf_`. Snapshot **acumulado ate o fim daquele mes**: `cf_txn_count_lifetime`,
`cf_tpv_lifetime`, `cf_chargebacks_lifetime`, `cf_frauds_lifetime`,
`cf_active_months`, janelas de 3 meses (`cf_*_3m`), taxas derivadas e atributos
cadastrais (`cf_segment`, `cf_risk_band`, `cf_monthly_income`, ...).

### `feature_store.transaction_features` — transacao
Prefixo `tf_`. Features disponiveis no instante da transacao, mais o rotulo
`is_fraud` e a coluna `feature_month` (mes **anterior**, chave do join
point-in-time).

### `feature_store.fraud_model_scores`
`transaction_id`, `model_score`, `model_flag`, `fraud_score_rule`, `is_fraud`,
`model_version` — fecha o ciclo devolvendo a predicao ao lakehouse.

---

## Colunas tecnicas (todas as tabelas)

| Coluna | Descricao |
|---|---|
| `_ingested_at` | Timestamp da ingestao |
| `_source_system` | Sistema de origem (`payments_ledger`, `payments_stream`, ...) |
| `_source_file` | Arquivo exato de onde a linha veio |
| `_pipeline_run_id` | Identificador da execucao do pipeline |
| `_ingestion_date` | Data da carga (particao da Bronze) |
| `_corrupt_record` | Linha malformada preservada na leitura permissiva |
| `_ingestion_lag_seconds` | Latencia entre o evento e a ingestao (streaming) |
| `_silver_processed_at` / `_gold_processed_at` | Timestamp do processamento |

## Tabela de qualidade

`data/quality/dq_results` — resultado de cada expectativa executada:
`run_id`, `checked_at`, `dataset`, `expectation`, `kind`, `column`, `severity`,
`total_rows`, `violations`, `violation_ratio`, `threshold`, `passed`, `detail`.
