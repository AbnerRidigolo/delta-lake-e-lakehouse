"""
CHECK constraints do Delta: qualidade aplicada pelo storage, nao pelo job.

Diferenca em relacao as expectativas
------------------------------------
Uma expectativa e uma **medicao**: roda depois da escrita e diz que o dado esta
ruim. Uma constraint e uma **garantia**: o Delta recusa o commit. O dado
invalido nunca chega a existir na tabela.

Isso importa porque nem toda escrita passa pelo pipeline. Um notebook de
analista, um backfill manual, um job de outro time — todos escrevem na mesma
tabela. A expectativa nao protege contra eles; a constraint sim, porque vive no
protocolo da tabela.

Estrategia adotada aqui
-----------------------
* **Silver/Gold** recebem constraints: sao tabelas de contrato, com invariantes
  de negocio que valem sempre (valor positivo, score na faixa, taxa entre 0 e 1).
* **Bronze nao recebe nenhuma**, de proposito. A Bronze existe justamente para
  aceitar o dado como ele veio. Constraint na Bronze faria o pipeline rejeitar
  na ingestao o registro que a Silver deveria capturar e explicar.

As constraints sao idempotentes: aplicar de novo nao duplica, e uma constraint
que ja falharia contra os dados existentes e reportada em vez de derrubar tudo.

Execucao:
    python -m src.quality.constraints
    python -m src.quality.constraints --drop   # remove todas (util em dev)
"""

from __future__ import annotations

import argparse

from src.common.config import get_settings
from src.common.delta_io import table_exists
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

# (camada, tabela) -> [(nome_da_constraint, expressao SQL)]
# As expressoes toleram NULL de proposito: `coluna > 0` e verdadeiro-vazio para
# NULL no SQL, e a obrigatoriedade de preenchimento e responsabilidade das
# expectativas `not_null`. Misturar as duas coisas em uma constraint torna a
# mensagem de erro ambigua.
CONSTRAINTS: dict[tuple[str, str], list[tuple[str, str]]] = {
    ("silver", "transactions"): [
        ("valor_positivo", "amount > 0"),
        ("parcelas_validas", "installments >= 1 AND installments <= 48"),
        ("moeda_suportada", "currency = 'BRL'"),
        ("status_no_dominio", "status IN ('approved','declined','reversed','chargeback')"),
        ("mdr_nao_negativo", "mdr_revenue >= 0"),
    ],
    ("silver", "customers"): [
        ("renda_positiva", "monthly_income > 0"),
        ("limite_nao_negativo", "credit_limit >= 0"),
        ("faixa_de_risco_valida", "risk_band IN ('A','B','C','D','E')"),
        ("idade_plausivel", "age IS NULL OR (age >= 16 AND age <= 110)"),
    ],
    ("silver", "credit_contracts"): [
        ("principal_positivo", "principal_amount > 0"),
        ("prazo_positivo", "term_months > 0"),
        ("atraso_nao_negativo", "days_past_due >= 0"),
        ("provisao_entre_0_e_1", "provision_rate >= 0 AND provision_rate <= 1"),
        ("saldo_nao_negativo", "outstanding_balance >= 0"),
    ],
    ("silver", "merchants"): [
        ("mdr_na_faixa_contratual", "mdr_rate >= 0 AND mdr_rate <= 0.15"),
        ("ticket_medio_positivo", "avg_ticket > 0"),
    ],
    ("gold", "customer_360"): [
        ("score_na_faixa", "customer_score >= 0 AND customer_score <= 1000"),
        ("rating_no_dominio", "rating IN ('AAA','AA','A','B','C','D')"),
        (
            "taxa_de_aprovacao_valida",
            "approval_rate IS NULL OR (approval_rate >= 0 AND approval_rate <= 1)",
        ),
    ],
    ("gold", "transaction_fraud_signals"): [
        ("score_de_fraude_na_faixa", "fraud_score_rule >= 0 AND fraud_score_rule <= 100"),
        ("nivel_de_risco_no_dominio", "risk_level IN ('baixo','medio','alto','critico')"),
        (
            "acao_no_dominio",
            "recommended_action IN ('aprovar','autenticar_2fa','revisar_manualmente','bloquear')",
        ),
    ],
    ("gold", "credit_risk_portfolio"): [
        ("pd_entre_0_e_1", "pd_observed >= 0 AND pd_observed <= 1"),
        ("lgd_entre_0_e_1", "lgd_assumption >= 0 AND lgd_assumption <= 1"),
        ("perda_esperada_nao_negativa", "expected_loss >= 0"),
    ],
    ("gold", "financial_kpis_daily"): [
        ("taxa_de_aprovacao_valida", "approval_rate >= 0 AND approval_rate <= 1"),
        ("chargeback_entre_0_e_1", "chargeback_rate >= 0 AND chargeback_rate <= 1"),
        ("tpv_nao_negativo", "tpv >= 0"),
    ],
}


def existing_constraints(spark, path: str) -> list[str]:
    """Lista as constraints ja registradas nas propriedades da tabela."""
    propriedades = spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect()
    prefixo = "delta.constraints."
    return [
        linha["key"][len(prefixo) :] for linha in propriedades if linha["key"].startswith(prefixo)
    ]


def apply_constraints(drop: bool = False) -> dict[str, list[str]]:
    """Aplica (ou remove) as CHECK constraints declaradas acima."""
    cfg = get_settings()
    spark = get_spark("quality_constraints")
    relatorio: dict[str, list[str]] = {}

    with log_stage(log, "quality.constraints"):
        for (camada, tabela), regras in CONSTRAINTS.items():
            path = cfg.table_path(camada, tabela)
            nome_completo = f"{camada}.{tabela}"

            if not table_exists(spark, path):
                log.warning("Tabela nao materializada, pulando: %s", nome_completo)
                continue

            ja_existem = set(existing_constraints(spark, path))
            aplicadas: list[str] = []

            for nome, expressao in regras:
                if drop:
                    if nome in ja_existem:
                        spark.sql(f"ALTER TABLE delta.`{path}` DROP CONSTRAINT {nome}")
                        aplicadas.append(f"-{nome}")
                    continue

                if nome in ja_existem:
                    continue  # idempotente

                try:
                    spark.sql(
                        f"ALTER TABLE delta.`{path}` ADD CONSTRAINT {nome} CHECK ({expressao})"
                    )
                    aplicadas.append(nome)
                except Exception as exc:
                    # Constraint que ja seria violada pelos dados atuais: nao
                    # derrubamos o pipeline, mas isso e um achado de qualidade.
                    log.error(
                        "Constraint `%s` REJEITADA em %s: os dados atuais ja a violam. "
                        "Expressao: %s | %s",
                        nome,
                        nome_completo,
                        expressao,
                        str(exc).splitlines()[0][:160],
                    )

            if aplicadas:
                log.info("%-34s | %s", nome_completo, ", ".join(aplicadas))
            relatorio[nome_completo] = aplicadas

        total = sum(len(v) for v in relatorio.values())
        acao = "removidas" if drop else "aplicadas"
        log.info("Constraints %s: %s em %s tabelas", acao, total, len(relatorio))

    return relatorio


def run() -> dict[str, list[str]]:
    return apply_constraints(drop=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="CHECK constraints das tabelas Delta.")
    parser.add_argument(
        "--drop", action="store_true", help="remove as constraints em vez de aplicar"
    )
    args = parser.parse_args()
    apply_constraints(drop=args.drop)


if __name__ == "__main__":
    main()
