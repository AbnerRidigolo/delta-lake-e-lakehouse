# ✅ Scorecard de qualidade de dados

**Nota geral: 99.1/100** (minimo exigido: 90.0) · perfil `ci` · run `537c162ee319`

## Nota por camada

| Camada | Tabelas | Checagens | Falhas (erro) | Falhas (aviso) | Nota |
|---|---:|---:|---:|---:|---:|
| `bronze` | 4 | 12 | 0 | 3 | **91.7** |
| `feature_store` | 1 | 8 | 0 | 0 | **100.0** |
| `gold` | 5 | 48 | 0 | 0 | **100.0** |
| `silver` | 4 | 46 | 0 | 0 | **100.0** |

## Detalhe por tabela

| | Tabela | Checagens | Erro | Aviso | Nota |
|---|---|---:|---:|---:|---:|
| ⚠️ | `bronze.credit_contracts` | 3 | 0 | 1 | 88.9 |
| ⚠️ | `bronze.customers` | 3 | 0 | 1 | 88.9 |
| ✅ | `bronze.merchants` | 3 | 0 | 0 | 100.0 |
| ⚠️ | `bronze.transactions` | 3 | 0 | 1 | 88.9 |
| ✅ | `feature_store.transaction_features` | 8 | 0 | 0 | 100.0 |
| ✅ | `gold.credit_risk_portfolio` | 9 | 0 | 0 | 100.0 |
| ✅ | `gold.customer_360` | 12 | 0 | 0 | 100.0 |
| ✅ | `gold.financial_kpis_daily` | 11 | 0 | 0 | 100.0 |
| ✅ | `gold.merchant_performance` | 10 | 0 | 0 | 100.0 |
| ✅ | `gold.transaction_fraud_signals` | 6 | 0 | 0 | 100.0 |
| ✅ | `silver.credit_contracts` | 13 | 0 | 0 | 100.0 |
| ✅ | `silver.customers` | 9 | 0 | 0 | 100.0 |
| ✅ | `silver.merchants` | 6 | 0 | 0 | 100.0 |
| ✅ | `silver.transactions` | 18 | 0 | 0 | 100.0 |

## Conciliacao entre camadas

✅ 9/9 identidades fecharam.

---

*Gerado por `src/quality/scorecard.py` a partir das tabelas `quality.dq_results`, `quality.reconciliation_results` e `quality.drift_results`.*
