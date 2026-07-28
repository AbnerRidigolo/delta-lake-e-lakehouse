"""
Contratos de schema das fontes brutas.

Decisao de arquitetura: **na Bronze, tudo entra como STRING.**

Por que? Porque a origem e um CSV/JSON operacional que pode trazer
`"amount": ""`, `"event_ts": "31/02/2024"` ou `"installments": "N/A"`. Se
deixarmos o Spark inferir tipos:

* linhas invalidas viram `null` silenciosamente e perdemos a evidencia do erro;
* a inferencia varre o arquivo inteiro antes de ler (caro e nao-deterministico);
* uma mudanca de tipo na origem quebra o job em producao sem aviso.

Tipando tudo como string preservamos o dado exatamente como chegou (Bronze e a
"fonte da verdade imutavel") e empurramos a casting/validacao para a Silver,
onde os registros rejeitados sao contabilizados em vez de sumirem.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType


def _string_schema(*columns: str) -> StructType:
    """Monta um StructType com todas as colunas como StringType (nullable)."""
    return StructType([StructField(name, StringType(), True) for name in columns])


CUSTOMERS_SCHEMA = _string_schema(
    "customer_id", "full_name", "cpf", "email", "phone", "birth_date", "city", "state",
    "region", "country", "signup_date", "acquisition_channel", "segment", "monthly_income",
    "credit_limit", "risk_band", "status", "source_extracted_at",
)

MERCHANTS_SCHEMA = _string_schema(
    "merchant_id", "merchant_name", "legal_name", "mcc", "category", "city", "state",
    "region", "country", "onboarding_date", "avg_ticket", "risk_score", "risk_flag",
    "mdr_rate", "is_active", "source_extracted_at",
)

TRANSACTIONS_SCHEMA = _string_schema(
    "transaction_id", "event_ts", "customer_id", "merchant_id", "mcc", "amount", "currency",
    "channel", "payment_method", "installments", "device_id", "ip_address", "geo_city",
    "geo_state", "status", "is_fraud", "fraud_type", "source_extracted_at",
)

CREDIT_CONTRACTS_SCHEMA = _string_schema(
    "contract_id", "customer_id", "product", "principal_amount", "interest_rate_month",
    "term_months", "origination_date", "installments_paid", "outstanding_balance",
    "days_past_due", "status", "recovery_amount", "collateral", "source_extracted_at",
)

# Mapa nome-da-tabela -> schema, usado pelo job generico de ingestao.
BRONZE_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "merchants": MERCHANTS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
    "credit_contracts": CREDIT_CONTRACTS_SCHEMA,
}
