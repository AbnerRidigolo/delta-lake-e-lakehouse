# Catalogo de dados - Lakehouse NeoPag

*Documento gerado automaticamente por `src/governance/apply_catalog_metadata.py` a partir de `config/catalog.yaml`. Nao edite manualmente.*

## Classificacao de dados

| Classificacao | Significado | Quem acessa |
|---|---|---|
| `PUBLIC` | Pode ser exposto em dashboards e relatorios externos. | analistas, engenharia_dados, ciencia_dados, negocio |
| `INTERNAL` | Uso interno, sem dado pessoal identificavel. | analistas, engenharia_dados, ciencia_dados |
| `CONFIDENTIAL` | Dado de negocio sensivel (financeiro, risco). | engenharia_dados, ciencia_dados, risco |
| `PII` | Dado pessoal - LGPD. Requer mascaramento e acesso restrito. | engenharia_dados, dpo |

## Dominios e responsaveis

| Dominio | Owner (produto de dados) | Steward (governanca) |
|---|---|---|
| `customer` | squad-customer-360 | data-governance |
| `payments` | squad-payments | data-governance |
| `credit` | squad-credit-risk | risk-office |
| `finance` | squad-fpna | controladoria |
| `ml` | squad-ml-platform | data-governance |

## Camada BRONZE

| Tabela | Nome no Unity Catalog | Dominio | Classificacao | SLA | Descricao |
|---|---|---|---|---|---|
| `bronze.customers` | `fintech.bronze.customers` | customer | `PII` | 24h | Cadastro bruto de clientes ingerido do CSV operacional, sem tratamento. |
| `bronze.merchants` | `fintech.bronze.merchants` | payments | `INTERNAL` | 24h | Cadastro bruto de estabelecimentos credenciados. |
| `bronze.transactions` | `fintech.bronze.transactions` | payments | `CONFIDENTIAL` | 1h | Transacoes brutas (batch + streaming), particionadas por data de ingestao. |
| `bronze.credit_contracts` | `fintech.bronze.credit_contracts` | credit | `CONFIDENTIAL` | 24h | Contratos de credito brutos (emprestimo pessoal, cartao, consignado, BNPL). |

## Camada SILVER

| Tabela | Nome no Unity Catalog | Dominio | Classificacao | SLA | Descricao |
|---|---|---|---|---|---|
| `silver.customers` | `fintech.silver.customers` | customer | `PII` | 24h | Clientes deduplicados, normalizados e validados. CPF e e-mail mascarados. |
| `silver.merchants` | `fintech.silver.merchants` | payments | `INTERNAL` | 24h | Estabelecimentos normalizados e enriquecidos com categoria de MCC. |
| `silver.transactions` | `fintech.silver.transactions` | payments | `CONFIDENTIAL` | 1h | Transacoes limpas, tipadas, deduplicadas e enriquecidas com cliente e merchant. |
| `silver.credit_contracts` | `fintech.silver.credit_contracts` | credit | `CONFIDENTIAL` | 24h | Contratos de credito tipados, com faixa de atraso e provisao calculada. |

## Camada GOLD

| Tabela | Nome no Unity Catalog | Dominio | Classificacao | SLA | Descricao |
|---|---|---|---|---|---|
| `gold.customer_360` | `fintech.gold.customer_360` | customer | `CONFIDENTIAL` | 24h | Visao unica do cliente: RFM, exposicao de credito, inadimplencia, churn e score agregado. |
| `gold.transaction_fraud_signals` | `fintech.gold.transaction_fraud_signals` | payments | `CONFIDENTIAL` | 1h | Transacoes com sinais de fraude por regra (velocity, valor atipico, madrugada, device novo). |
| `gold.credit_risk_portfolio` | `fintech.gold.credit_risk_portfolio` | credit | `CONFIDENTIAL` | 24h | Carteira de credito por safra/produto com EAD, PD proxy, LGD e perda esperada. |
| `gold.financial_kpis_daily` | `fintech.gold.financial_kpis_daily` | finance | `CONFIDENTIAL` | 6h | KPIs financeiros diarios: receita (MDR + juros), perdas, recuperacao, chargeback e aprovacao. |
| `gold.merchant_performance` | `fintech.gold.merchant_performance` | payments | `INTERNAL` | 24h | Desempenho e risco por estabelecimento: TPV, ticket medio, taxa de chargeback e de fraude. |

## Camada FEATURE_STORE

| Tabela | Nome no Unity Catalog | Dominio | Classificacao | SLA | Descricao |
|---|---|---|---|---|---|
| `feature_store.customer_features` | `fintech.feature_store.customer_features` | ml | `CONFIDENTIAL` | 24h | Features de cliente com timestamp de referencia para treino point-in-time. |
| `feature_store.transaction_features` | `fintech.feature_store.transaction_features` | ml | `CONFIDENTIAL` | 1h | Features transacionais para o modelo de deteccao de fraude. |

## Dados pessoais (LGPD)

Colunas classificadas como dado pessoal e o tratamento aplicado:

| Tabela | Colunas | Tratamento |
|---|---|---|
| `bronze.customers` | full_name, cpf, email, phone, birth_date | acesso restrito (dado bruto preservado para auditoria) |
| `bronze.transactions` | ip_address, device_id | acesso restrito (dado bruto preservado para auditoria) |
| `silver.customers` | full_name, cpf_masked, email_masked | mascaramento + hash com salt |

*Tabelas materializadas e anotadas nesta execucao: 15.*
