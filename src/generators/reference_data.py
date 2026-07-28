"""
Dados de referencia usados pelo gerador sintetico.

Separado do gerador para manter o arquivo principal legivel e permitir que as
listas sejam reutilizadas (por exemplo, no dicionario de dados e nos testes).
Nada aqui e dado real: nomes, cidades e estabelecimentos sao ficticios ou
genericos.
"""

from __future__ import annotations

# --- Pessoas (nomes comuns no Brasil, combinados aleatoriamente) ------------
FIRST_NAMES = [
    "Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
    "Isabela", "Joao", "Karina", "Lucas", "Mariana", "Nicolas", "Olivia", "Pedro",
    "Quezia", "Rafael", "Sofia", "Thiago", "Ursula", "Vitor", "Wesley", "Yasmin",
    "Beatriz", "Caio", "Debora", "Emerson", "Fernanda", "Gustavo", "Helena", "Igor",
    "Julia", "Kevin", "Larissa", "Matheus", "Natalia", "Otavio", "Paula", "Renato",
]

LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves",
    "Pereira", "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida",
    "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha", "Dias", "Nunes",
    "Moreira", "Cardoso", "Teixeira", "Correia", "Cavalcanti", "Monteiro", "Freitas",
]

# --- Geografia: (cidade, UF, regiao, peso populacional relativo) ------------
CITIES = [
    ("Sao Paulo", "SP", "Sudeste", 22.0),
    ("Rio de Janeiro", "RJ", "Sudeste", 12.0),
    ("Belo Horizonte", "MG", "Sudeste", 7.0),
    ("Campinas", "SP", "Sudeste", 4.0),
    ("Curitiba", "PR", "Sul", 5.0),
    ("Porto Alegre", "RS", "Sul", 5.0),
    ("Florianopolis", "SC", "Sul", 3.0),
    ("Salvador", "BA", "Nordeste", 6.0),
    ("Recife", "PE", "Nordeste", 5.0),
    ("Fortaleza", "CE", "Nordeste", 5.0),
    ("Natal", "RN", "Nordeste", 2.0),
    ("Brasilia", "DF", "Centro-Oeste", 6.0),
    ("Goiania", "GO", "Centro-Oeste", 3.0),
    ("Campo Grande", "MS", "Centro-Oeste", 2.0),
    ("Manaus", "AM", "Norte", 4.0),
    ("Belem", "PA", "Norte", 3.0),
]

# --- Segmentos comerciais da fintech ---------------------------------------
# (segmento, peso, renda mensal minima, renda mensal maxima)
SEGMENTS = [
    ("varejo", 0.62, 1_800.0, 8_000.0),
    ("premium", 0.22, 8_000.0, 25_000.0),
    ("private", 0.05, 25_000.0, 120_000.0),
    ("pj_micro", 0.11, 5_000.0, 60_000.0),
]

ACQUISITION_CHANNELS = [
    ("app_organico", 0.34),
    ("app_pago", 0.22),
    ("indicacao", 0.18),
    ("parceria_varejo", 0.14),
    ("web", 0.09),
    ("agencia_parceira", 0.03),
]

# Faixa de risco inicial atribuida no onboarding (equivalente a um bureau score)
RISK_BANDS = [("A", 0.18), ("B", 0.27), ("C", 0.29), ("D", 0.17), ("E", 0.09)]

# --- Estabelecimentos: MCC (Merchant Category Code) -------------------------
# (mcc, categoria, peso de ocorrencia, ticket medio, risco intrinseco de fraude)
MCC_CATALOG = [
    ("5411", "supermercado", 0.16, 180.0, 0.6),
    ("5812", "restaurante", 0.14, 95.0, 0.8),
    ("5541", "posto_combustivel", 0.09, 210.0, 1.1),
    ("5912", "farmacia", 0.08, 85.0, 0.5),
    ("5999", "varejo_diverso", 0.10, 160.0, 1.0),
    ("4121", "mobilidade_urbana", 0.08, 32.0, 0.7),
    ("5732", "eletronicos", 0.06, 1450.0, 2.6),
    ("7995", "apostas_online", 0.03, 320.0, 4.2),
    ("4816", "servicos_digitais", 0.07, 60.0, 2.1),
    ("4722", "agencia_viagem", 0.04, 2100.0, 2.9),
    ("5691", "vestuario", 0.07, 240.0, 1.2),
    ("8011", "saude", 0.04, 380.0, 0.4),
    ("6051", "cripto_cambio", 0.02, 1800.0, 5.0),
    ("5944", "joalheria", 0.02, 2600.0, 3.4),
]

MERCHANT_NAME_PREFIX = [
    "Loja", "Casa", "Grupo", "Rede", "Comercio", "Distribuidora", "Central",
    "Ponto", "Espaco", "Mercado",
]
MERCHANT_NAME_SUFFIX = [
    "Aurora", "Atlantico", "Bandeirante", "Cristal", "Delta", "Estrela", "Horizonte",
    "Ipe", "Jacaranda", "Litoral", "Monte", "Nova Era", "Oceano", "Primavera",
    "Quartzo", "Real", "Sol", "Tropical", "Uniao", "Vertice",
]

# --- Canais e formas de pagamento ------------------------------------------
# (canal, peso, formas de pagamento possiveis)
CHANNELS = [
    ("pos", 0.30, ["credit_card", "debit_card"]),
    ("ecommerce", 0.24, ["credit_card", "debit_card", "pix"]),
    ("pix", 0.26, ["pix"]),
    ("wallet", 0.09, ["credit_card", "pix"]),
    ("boleto", 0.05, ["boleto"]),
    ("ted", 0.03, ["ted"]),
    ("atm", 0.03, ["debit_card"]),
]

TRANSACTION_STATUS = ["approved", "declined", "reversed", "chargeback"]

# --- Produtos de credito ----------------------------------------------------
# (produto, peso, principal min, principal max, juros a.m. min, juros a.m. max, prazos)
CREDIT_PRODUCTS = [
    ("cartao_rotativo", 0.34, 500.0, 15_000.0, 0.089, 0.145, [1, 3, 6, 12]),
    ("emprestimo_pessoal", 0.28, 1_000.0, 60_000.0, 0.031, 0.079, [6, 12, 18, 24, 36]),
    ("consignado", 0.16, 3_000.0, 90_000.0, 0.014, 0.029, [24, 36, 48, 60]),
    ("bnpl", 0.14, 150.0, 4_000.0, 0.0, 0.049, [2, 3, 4, 6]),
    ("capital_giro_pj", 0.08, 10_000.0, 250_000.0, 0.019, 0.045, [12, 18, 24]),
]

# Modos de fraude simulados. Cada um gera um padrao diferente nos dados, para
# que o modelo de ML tenha o que aprender e as regras da Gold facam sentido.
FRAUD_PATTERNS = ["card_testing", "account_takeover", "merchant_collusion", "cnp_high_ticket"]
