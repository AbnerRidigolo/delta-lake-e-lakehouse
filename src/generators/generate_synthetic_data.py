"""
Gerador de dados sinteticos da fintech ficticia **NeoPag**.

O que este script produz (camada `data/raw/`)
---------------------------------------------
    data/raw/customers/customers.csv                 -> cadastro de clientes
    data/raw/merchants/merchants.csv                 -> estabelecimentos credenciados
    data/raw/credit_contracts/credit_contracts.csv   -> contratos de credito
    data/raw/transactions/transactions_YYYY-MM.csv   -> historico transacional (BATCH)
    data/raw/streaming/transactions/batch_XXX.json   -> ultimos dias em JSONL (STREAMING)

Por que os dados sao "realistas" e nao aleatorios
-------------------------------------------------
Um dataset puramente aleatorio nao ensina nada a um modelo e deixa a camada
Gold sem historia para contar. Aqui a geracao e **guiada por comportamento**:

* cada cliente tem um perfil (renda, propensao a gasto, cidade, dispositivos,
  canais preferidos) e as transacoes sao amostradas desse perfil;
* a fraude segue **quatro padroes distintos** (card testing, account takeover,
  conluio com estabelecimento e CNP de alto ticket), cada um com assinatura
  propria em valor, horario, canal, dispositivo e velocidade;
* a inadimplencia dos contratos e correlacionada com faixa de risco,
  comprometimento de renda e safra;
* uma fracao dos registros e **corrompida de proposito** (nulos, duplicatas,
  datas invalidas, categorias com caixa/espacos inconsistentes, chaves orfas)
  para que a camada Silver tenha trabalho de verdade a fazer.

Execucao
--------
    python -m src.generators.generate_synthetic_data
    python -m src.generators.generate_synthetic_data --transactions 50000 --seed 7

Nao depende de Spark: e Python puro (stdlib), roda em qualquer maquina.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import shutil
from bisect import bisect
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

from src.common.config import get_settings
from src.common.logging_utils import get_logger, log_stage
from src.generators import reference_data as ref

log = get_logger(__name__)

BRAND = "NeoPag"

# Dias de atraso a partir dos quais o contrato e levado a prejuizo (write-off).
# Precisa ser o mesmo valor usado em `gold_financial_kpis_daily.WRITE_OFF_LAG_DAYS`.
WRITE_OFF_DPD = 180

# --- Parametros de realismo comportamental ---------------------------------
# Existem para evitar que features triviais separem fraude de forma perfeita.
# Um dataset sintetico "limpo demais" produz um modelo com 100% de recall que
# nao significa absolutamente nada.
SESSION_RATE = 0.07       # chance de a compra virar uma sessao de varias compras
LONG_SESSION_RATE = 0.18  # dentro das sessoes, chance de ser uma sessao longa
RETRY_RATE = 0.45         # chance de retentativa apos uma recusa
NEW_DEVICE_RATE = 0.045   # chance de o cliente usar um aparelho novo

# Nem toda fraude e descoberta. Esta fracao das transacoes fraudulentas fica
# rotulada como legitima - o cliente nao contestou, a analise nao pegou. Rotulo
# ruidoso e a norma em antifraude, e um dataset sem esse ruido produz metricas
# que nenhum modelo real alcanca.
UNREPORTED_FRAUD_RATE = 0.08


# ===========================================================================
# Utilitarios de amostragem
# ===========================================================================

def weighted_pick(rng: random.Random, options: Sequence[Tuple[Any, float]]) -> Any:
    """Escolhe um item de [(valor, peso), ...]."""
    values = [o[0] for o in options]
    weights = [o[1] for o in options]
    return rng.choices(values, weights=weights, k=1)[0]


def lognormal_amount(rng: random.Random, median: float, sigma: float = 0.65) -> float:
    """Valor monetario com cauda longa a direita (como gasto real de cartao)."""
    value = rng.lognormvariate(math.log(max(median, 1.0)), sigma)
    return round(min(value, median * 40), 2)


def random_date(rng: random.Random, start: date, end: date) -> date:
    """Data uniforme em [start, end]. Se a janela for invalida, devolve `end`.

    O clamp evita que combinacoes de datas nas bordas (cliente que entrou na
    base perto do fim da janela, por exemplo) quebrem a geracao.
    """
    span = (end - start).days
    if span <= 0:
        return end
    return start + timedelta(days=rng.randint(0, span))


def business_hour(rng: random.Random) -> int:
    """Distribuicao horaria tipica de consumo (picos no almoco e a noite)."""
    hours = list(range(24))
    weights = [
        0.6, 0.4, 0.3, 0.2, 0.2, 0.4, 1.0, 2.2, 3.4, 4.0, 4.6, 5.6,
        6.4, 5.4, 4.8, 4.6, 4.8, 5.6, 6.6, 6.8, 5.8, 4.4, 2.8, 1.4,
    ]
    return rng.choices(hours, weights=weights, k=1)[0]


# ===========================================================================
# Perfis comportamentais
# ===========================================================================

@dataclass
class CustomerProfile:
    """Parametros que governam como um cliente transaciona."""

    customer_id: str
    segment: str
    income: float
    risk_band: str
    city: str
    state: str
    signup_date: date
    devices: List[str]
    spend_factor: float          # multiplicador do ticket medio do merchant
    activity: float              # peso relativo de frequencia transacional
    preferred_channels: List[Tuple[str, float]]
    churn_date: date | None      # a partir daqui o cliente para de transacionar
    fraud_victim: bool           # sera alvo de um episodio de fraude


# ===========================================================================
# 1) Clientes
# ===========================================================================

def build_customers(rng: random.Random, n: int, start: date, end: date
                    ) -> Tuple[List[Dict[str, Any]], List[CustomerProfile]]:
    """Gera o cadastro de clientes e os perfis comportamentais correspondentes."""
    rows: List[Dict[str, Any]] = []
    profiles: List[CustomerProfile] = []

    city_options = [((c, uf, reg), w) for c, uf, reg, w in ref.CITIES]
    segment_options = [(s, w) for s, w, _, _ in ref.SEGMENTS]
    segment_income = {s: (lo, hi) for s, _, lo, hi in ref.SEGMENTS}

    for i in range(1, n + 1):
        customer_id = f"CUST-{i:06d}"
        city, state, region = weighted_pick(rng, city_options)
        segment = weighted_pick(rng, segment_options)
        income_lo, income_hi = segment_income[segment]
        income = round(rng.uniform(income_lo, income_hi), 2)
        risk_band = weighted_pick(rng, ref.RISK_BANDS)

        # Clientes entram na base ao longo do tempo (safras de aquisicao).
        signup = random_date(rng, start - timedelta(days=540), end - timedelta(days=15))
        birth = date(rng.randint(1955, 2005), rng.randint(1, 12), rng.randint(1, 28))

        # ~14% da base "morre" em algum momento da janela (churn comportamental).
        churn_date = None
        if rng.random() < 0.14:
            churn_date = random_date(rng, max(signup, start) + timedelta(days=30), end)

        # Clientes de maior risco/segmento premium sao alvos mais provaveis.
        fraud_propensity = {"A": 0.006, "B": 0.010, "C": 0.016, "D": 0.024, "E": 0.034}[risk_band]
        if segment in ("premium", "private"):
            fraud_propensity *= 1.6

        profile = CustomerProfile(
            customer_id=customer_id,
            segment=segment,
            income=income,
            risk_band=risk_band,
            city=city,
            state=state,
            signup_date=signup,
            devices=[f"DEV-{rng.getrandbits(40):010x}" for _ in range(rng.randint(1, 3))],
            spend_factor=round(rng.lognormvariate(0.0, 0.45), 3),
            activity=max(0.15, rng.gauss(1.0, 0.45)),
            preferred_channels=_channel_preferences(rng, segment),
            churn_date=churn_date,
            fraud_victim=rng.random() < fraud_propensity * 6,
        )
        profiles.append(profile)

        first, last = rng.choice(ref.FIRST_NAMES), rng.choice(ref.LAST_NAMES)
        rows.append({
            "customer_id": customer_id,
            "full_name": f"{first} {last}",
            # CPF sintetico: digitos aleatorios apenas no formato brasileiro.
            "cpf": f"{rng.randint(0, 999):03d}.{rng.randint(0, 999):03d}."
                   f"{rng.randint(0, 999):03d}-{rng.randint(0, 99):02d}",
            "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 9999)}@exemplo.com.br",
            "phone": f"+55{rng.choice(['11', '21', '31', '41', '51', '71', '81'])}"
                     f"9{rng.randint(10_000_000, 99_999_999)}",
            "birth_date": birth.isoformat(),
            "city": city,
            "state": state,
            "region": region,
            "country": "BR",
            "signup_date": signup.isoformat(),
            "acquisition_channel": weighted_pick(rng, ref.ACQUISITION_CHANNELS),
            "segment": segment,
            "monthly_income": f"{income:.2f}",
            "credit_limit": f"{round(income * rng.uniform(0.4, 2.8), 2):.2f}",
            "risk_band": risk_band,
            "status": "active" if churn_date is None else "inactive",
            "source_extracted_at": datetime.combine(end, datetime.min.time()).isoformat(),
        })

    return rows, profiles


def _channel_preferences(rng: random.Random, segment: str) -> List[Tuple[str, float]]:
    """Ajusta os pesos de canal conforme o segmento do cliente."""
    prefs = []
    for channel, weight, _methods in ref.CHANNELS:
        adjusted = weight * rng.uniform(0.6, 1.4)
        if segment == "pj_micro" and channel in ("ted", "boleto", "pix"):
            adjusted *= 2.2
        if segment in ("premium", "private") and channel in ("ecommerce", "pos"):
            adjusted *= 1.4
        prefs.append((channel, adjusted))
    return prefs


# ===========================================================================
# 2) Estabelecimentos (merchants)
# ===========================================================================

def build_merchants(rng: random.Random, n: int, start: date, end: date) -> List[Dict[str, Any]]:
    """Gera o cadastro de estabelecimentos com MCC, ticket medio e risco."""
    rows: List[Dict[str, Any]] = []
    mcc_options = [((mcc, cat, ticket, risk), w) for mcc, cat, w, ticket, risk in ref.MCC_CATALOG]
    city_options = [((c, uf, reg), w) for c, uf, reg, w in ref.CITIES]

    for i in range(1, n + 1):
        mcc, category, base_ticket, base_risk = weighted_pick(rng, mcc_options)
        city, state, region = weighted_pick(rng, city_options)
        avg_ticket = round(base_ticket * rng.uniform(0.6, 1.7), 2)

        # Alguns poucos estabelecimentos sao "podres" (conluio / laranja).
        is_colluding = rng.random() < 0.02

        # O `risk_score` e o score que a ADQUIRENTE calcula sobre o lojista - uma
        # ESTIMATIVA a partir do historico, nao a verdade. Ele precisa ser
        # ruidoso nas duas direcoes, senao vira um oraculo:
        #   * so 60% dos lojistas em conluio ja foram detectados pelo score;
        #   * ~8% dos lojistas honestos carregam score alto sem motivo (falso
        #     positivo do modelo de risco da adquirente).
        # Sem esse ruido, o score sozinho identificaria a fraude e o modelo de ML
        # aprenderia a copiar o rotulo em vez de aprender o comportamento.
        score_noise = 0.0
        if is_colluding and rng.random() < 0.60:
            score_noise = rng.uniform(0.5, 2.5)
        elif not is_colluding and rng.random() < 0.08:
            score_noise = rng.uniform(1.0, 3.0)
        risk_score = round(min(base_risk * rng.uniform(0.7, 1.5) + score_noise, 10.0), 2)

        rows.append({
            "merchant_id": f"MERC-{i:05d}",
            "merchant_name": f"{rng.choice(ref.MERCHANT_NAME_PREFIX)} "
                             f"{rng.choice(ref.MERCHANT_NAME_SUFFIX)} {i:03d}",
            "legal_name": f"{rng.choice(ref.MERCHANT_NAME_SUFFIX).upper()} COMERCIO LTDA",
            "mcc": mcc,
            "category": category,
            "city": city,
            "state": state,
            "region": region,
            "country": "BR",
            "onboarding_date": random_date(rng, start - timedelta(days=900), end).isoformat(),
            "avg_ticket": f"{avg_ticket:.2f}",
            "risk_score": f"{risk_score:.2f}",
            "risk_flag": "high" if risk_score >= 4.0 else ("medium" if risk_score >= 2.0 else "low"),
            # MDR = taxa cobrada do lojista; e a principal receita de adquirencia.
            "mdr_rate": f"{round(rng.uniform(0.009, 0.039), 4):.4f}",
            "is_active": "true" if rng.random() > 0.06 else "false",
            "source_extracted_at": datetime.combine(end, datetime.min.time()).isoformat(),
            "_colluding": "true" if is_colluding else "false",
        })
    return rows


# ===========================================================================
# 3) Transacoes (o coracao do dataset)
# ===========================================================================

def build_transactions(
    rng: random.Random,
    profiles: List[CustomerProfile],
    merchants: List[Dict[str, Any]],
    n_transactions: int,
    fraud_rate: float,
    start: date,
    end: date,
) -> List[Dict[str, Any]]:
    """Gera o historico transacional legitimo + episodios de fraude."""
    merchant_index = {m["merchant_id"]: m for m in merchants}
    colluding = [m["merchant_id"] for m in merchants if m["_colluding"] == "true"] or \
                [merchants[0]["merchant_id"]]
    merchant_ids = [m["merchant_id"] for m in merchants]
    risky_merchants = [m["merchant_id"] for m in merchants
                       if m["category"] in ("eletronicos", "cripto_cambio", "servicos_digitais",
                                            "apostas_online", "agencia_viagem", "joalheria")]
    # A fraude PREFERE certas categorias, mas nao se restringe a elas. Se todo
    # evento fraudulento caisse em um subconjunto fechado de lojistas, a
    # categoria sozinha entregaria o rotulo.
    ecommerce_merchants = risky_merchants + rng.sample(
        merchant_ids, k=min(len(merchant_ids), max(len(risky_merchants) // 2, 5)))
    merchant_weights = [1.0 / (1.0 + float(m["avg_ticket"]) ** 0.35) for m in merchants]

    n_fraud = int(n_transactions * fraud_rate)
    n_legit = n_transactions - n_fraud
    # Parte das transacoes legitimas nasce de "sessoes" e retentativas (abaixo),
    # entao sorteamos menos eventos-base para o total final bater com o pedido.
    n_base = int(n_legit / (1 + SESSION_RATE * 1.5 + 0.02))

    rows: List[Dict[str, Any]] = []
    counter = 0

    # ---- 3.1 Transacoes legitimas -----------------------------------------
    # Sorteamos os clientes de uma vez (peso = atividade) para performance.
    chosen_customers = rng.choices(profiles, weights=[p.activity for p in profiles], k=n_base)
    chosen_merchants = rng.choices(merchant_ids, weights=merchant_weights, k=n_base)

    for profile, merchant_id in zip(chosen_customers, chosen_merchants):
        counter += 1
        merchant = merchant_index[merchant_id]

        # A transacao so pode existir entre o cadastro e o churn do cliente.
        window_start = max(start, profile.signup_date)
        window_end = profile.churn_date or end
        if window_end <= window_start:
            window_end = window_start + timedelta(days=1)
        event_day = random_date(rng, window_start, min(window_end, end))
        event_ts = datetime.combine(event_day, datetime.min.time()) + timedelta(
            hours=business_hour(rng), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
        )

        channel = weighted_pick(rng, profile.preferred_channels)
        methods = next(m for c, _w, m in ref.CHANNELS if c == channel)
        payment_method = rng.choice(methods)

        amount = lognormal_amount(rng, float(merchant["avg_ticket"]) * profile.spend_factor)
        # Parcelamento so existe em credito e cresce com o valor.
        installments = 1
        if payment_method == "credit_card" and amount > 300 and rng.random() < 0.35:
            installments = rng.choice([2, 3, 4, 6, 10, 12])

        # A maioria das compras acontece na cidade do cliente.
        if rng.random() < 0.88:
            geo_city, geo_state = profile.city, profile.state
        else:
            geo_city, geo_state, _ = weighted_pick(rng, [((c, uf, r), w) for c, uf, r, w in ref.CITIES])

        status = _legit_status(rng, profile, amount)

        # Cliente legitimo tambem troca de aparelho de vez em quando (celular
        # novo, navegador novo). Sem isso, "dispositivo desconhecido" viraria um
        # separador perfeito de fraude - e o modelo aprenderia uma mentira.
        if rng.random() < NEW_DEVICE_RATE:
            device = f"DEV-{rng.getrandbits(40):010x}"
            profile.devices.append(device)
        else:
            device = rng.choice(profile.devices)

        rows.append(_transaction_row(
            counter=counter, event_ts=event_ts, profile=profile, merchant=merchant,
            amount=amount, channel=channel, payment_method=payment_method,
            installments=installments, device=device,
            geo_city=geo_city, geo_state=geo_state, status=status,
            is_fraud=0, fraud_type="", rng=rng,
        ))

        # --- Comportamento legitimo de alta frequencia ----------------------
        # Duas situacoes muito comuns geram varias transacoes em poucos minutos
        # SEM que exista fraude: a "sessao de compra" (pessoa comprando itens em
        # sequencia) e a retentativa apos uma recusa. Modelar isso e essencial:
        # velocity precisa ser um sinal util, nao um atalho para o rotulo.
        extra_events = 0
        if status == "declined" and rng.random() < RETRY_RATE:
            extra_events = rng.randint(1, 2)          # retentativa apos recusa
        elif rng.random() < SESSION_RATE:
            # A cauda longa importa: existem sessoes legitimas com muitas compras
            # em minutos (marketplace com varios vendedores, recarga, assinatura
            # em lote). Sem essa cauda, "velocity alta" separaria fraude de forma
            # perfeita e o modelo aprenderia um artefato do gerador.
            extra_events = (rng.randint(4, 8) if rng.random() < LONG_SESSION_RATE
                            else rng.randint(1, 3))

        for k in range(extra_events):
            counter += 1
            follow_up_merchant = merchant if rng.random() < 0.7 else \
                merchant_index[rng.choice(merchant_ids)]
            follow_up_amount = (amount if status == "declined"
                                else lognormal_amount(rng, float(follow_up_merchant["avg_ticket"])
                                                      * profile.spend_factor))
            rows.append(_transaction_row(
                counter=counter,
                event_ts=event_ts + timedelta(minutes=rng.randint(1, 9) * (k + 1),
                                              seconds=rng.randint(0, 59)),
                profile=profile, merchant=follow_up_merchant, amount=follow_up_amount,
                channel=channel, payment_method=payment_method, installments=1,
                device=device, geo_city=geo_city, geo_state=geo_state,
                status=_legit_status(rng, profile, follow_up_amount),
                is_fraud=0, fraud_type="", rng=rng,
            ))

    # ---- 3.2 Episodios de fraude ------------------------------------------
    # Fraude raramente e um evento isolado: e uma sequencia. Por isso geramos
    # "episodios" com 3 a 12 transacoes cada, seguindo padroes conhecidos.
    victims = [p for p in profiles if p.fraud_victim] or profiles[:50]
    produced = 0
    while produced < n_fraud:
        victim = rng.choice(victims)
        pattern = weighted_pick(rng, [("card_testing", 0.30), ("account_takeover", 0.28),
                                      ("merchant_collusion", 0.20), ("cnp_high_ticket", 0.22)])
        episode = _fraud_episode(
            rng=rng, profile=victim, pattern=pattern, merchant_index=merchant_index,
            colluding=colluding, ecommerce_merchants=ecommerce_merchants or merchant_ids,
            merchant_ids=merchant_ids, start=start, end=end, start_counter=counter,
        )
        counter += len(episode)
        produced += len(episode)
        rows.extend(episode)

    rows.sort(key=lambda r: r["event_ts"])
    return rows


def _legit_status(rng: random.Random, profile: CustomerProfile, amount: float) -> str:
    """Status de uma transacao legitima (a maioria aprova)."""
    roll = rng.random()
    # Clientes de risco alto tem mais recusa (limite/saldo insuficiente).
    decline_rate = {"A": 0.020, "B": 0.032, "C": 0.048, "D": 0.070, "E": 0.098}[profile.risk_band]
    if amount > profile.income * 0.6:
        decline_rate *= 2.5
    if roll < decline_rate:
        return "declined"
    if roll < decline_rate + 0.004:
        return "reversed"          # estorno operacional legitimo
    if roll < decline_rate + 0.0045:
        return "chargeback"        # contestacao nao fraudulenta (produto nao entregue)
    return "approved"


def _fraud_episode(
    rng: random.Random,
    profile: CustomerProfile,
    pattern: str,
    merchant_index: Dict[str, Dict[str, Any]],
    colluding: List[str],
    ecommerce_merchants: List[str],
    merchant_ids: List[str],
    start: date,
    end: date,
    start_counter: int,
) -> List[Dict[str, Any]]:
    """Gera uma sequencia de transacoes fraudulentas com assinatura propria.

    * `card_testing`      - rajada de valores baixos em minutos, e-commerce, device novo.
    * `account_takeover`  - alto valor, madrugada, cidade e device diferentes.
    * `merchant_collusion`- valores medios/altos concentrados em lojista suspeito.
    * `cnp_high_ticket`   - compra unica de alto ticket sem presenca do cartao.
    """
    window_start = max(start, profile.signup_date)
    base_day = random_date(rng, window_start, end) if end > window_start else window_start
    rows: List[Dict[str, Any]] = []
    counter = start_counter

    # Fraudador quase sempre usa um dispositivo nunca visto antes.
    fraud_device = f"DEV-{rng.getrandbits(40):010x}"

    if pattern == "card_testing":
        size = rng.randint(5, 12)
        base_ts = datetime.combine(base_day, datetime.min.time()) + timedelta(
            hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
        for k in range(size):
            counter += 1
            merchant = merchant_index[rng.choice(ecommerce_merchants)]
            rows.append(_transaction_row(
                counter=counter,
                event_ts=base_ts + timedelta(seconds=rng.randint(20, 110) * (k + 1)),
                profile=profile, merchant=merchant,
                amount=round(rng.uniform(1.0, 24.9), 2),
                channel="ecommerce", payment_method="credit_card", installments=1,
                device=fraud_device, geo_city=profile.city, geo_state=profile.state,
                # Boa parte das tentativas de teste de cartao e recusada.
                status="declined" if rng.random() < 0.55 else "approved",
                is_fraud=1, fraud_type=pattern, rng=rng, foreign_ip=True,
            ))

    elif pattern == "account_takeover":
        size = rng.randint(2, 5)
        far_city, far_state, _ = weighted_pick(
            rng, [((c, uf, r), w) for c, uf, r, w in ref.CITIES if uf != profile.state])
        base_ts = datetime.combine(base_day, datetime.min.time()) + timedelta(
            hours=rng.randint(0, 4), minutes=rng.randint(0, 59))
        for k in range(size):
            counter += 1
            merchant = merchant_index[rng.choice(ecommerce_merchants)]
            rows.append(_transaction_row(
                counter=counter, event_ts=base_ts + timedelta(minutes=rng.randint(3, 40) * (k + 1)),
                profile=profile, merchant=merchant,
                amount=lognormal_amount(rng, max(profile.income * 0.45, 900.0), sigma=0.5),
                channel=rng.choice(["ecommerce", "wallet", "pix"]),
                payment_method=rng.choice(["credit_card", "pix"]),
                installments=1, device=fraud_device, geo_city=far_city, geo_state=far_state,
                status=rng.choices(["approved", "chargeback", "declined"], [0.45, 0.40, 0.15])[0],
                is_fraud=1, fraud_type=pattern, rng=rng, foreign_ip=True,
            ))

    elif pattern == "merchant_collusion":
        size = rng.randint(3, 7)
        merchant = merchant_index[rng.choice(colluding)]
        for k in range(size):
            counter += 1
            day = base_day + timedelta(days=rng.randint(0, 6))
            if day > end:
                day = end
            rows.append(_transaction_row(
                counter=counter,
                event_ts=datetime.combine(day, datetime.min.time())
                         + timedelta(hours=rng.randint(8, 23), minutes=rng.randint(0, 59)),
                profile=profile, merchant=merchant,
                amount=lognormal_amount(rng, float(merchant["avg_ticket"]) * 3.2, sigma=0.4),
                channel="pos", payment_method="credit_card",
                installments=rng.choice([1, 1, 6, 10, 12]),
                device=rng.choice(profile.devices), geo_city=merchant["city"],
                geo_state=merchant["state"],
                status=rng.choices(["approved", "chargeback"], [0.6, 0.4])[0],
                is_fraud=1, fraud_type=pattern, rng=rng,
            ))

    else:  # cnp_high_ticket - "card not present" de alto valor
        counter += 1
        merchant = merchant_index[rng.choice(ecommerce_merchants)]
        rows.append(_transaction_row(
            counter=counter,
            event_ts=datetime.combine(base_day, datetime.min.time())
                     + timedelta(hours=rng.choice([1, 2, 3, 4, 23]), minutes=rng.randint(0, 59)),
            profile=profile, merchant=merchant,
            amount=lognormal_amount(rng, 6800.0, sigma=0.45),
            channel="ecommerce", payment_method="credit_card",
            installments=rng.choice([1, 1, 2, 3]), device=fraud_device,
            geo_city=profile.city, geo_state=profile.state,
            status=rng.choices(["approved", "chargeback", "declined"], [0.4, 0.45, 0.15])[0],
            is_fraud=1, fraud_type="cnp_high_ticket", rng=rng, foreign_ip=True,
        ))

    return rows


def _transaction_row(
    counter: int, event_ts: datetime, profile: CustomerProfile, merchant: Dict[str, Any],
    amount: float, channel: str, payment_method: str, installments: int, device: str,
    geo_city: str, geo_state: str, status: str, is_fraud: int, fraud_type: str,
    rng: random.Random, foreign_ip: bool = False,
) -> Dict[str, Any]:
    """Monta o registro cru de transacao (todos os campos como texto, como num CSV real)."""
    # Fraude nao confirmada: a transacao aconteceu, mas nunca foi rotulada como
    # fraude (cliente nao contestou / analise nao identificou).
    if is_fraud == 1 and rng.random() < UNREPORTED_FRAUD_RATE:
        is_fraud, fraud_type = 0, ""

    if foreign_ip and rng.random() < 0.6:
        ip = f"{rng.randint(2, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"
    else:
        ip = f"{rng.choice([177, 187, 189, 191, 200, 201])}." \
             f"{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"

    return {
        "transaction_id": f"TXN-{counter:09d}",
        "event_ts": event_ts.isoformat(sep=" ", timespec="seconds"),
        "customer_id": profile.customer_id,
        "merchant_id": merchant["merchant_id"],
        "mcc": merchant["mcc"],
        "amount": f"{amount:.2f}",
        "currency": "BRL",
        "channel": channel,
        "payment_method": payment_method,
        "installments": str(installments),
        "device_id": device,
        "ip_address": ip,
        "geo_city": geo_city,
        "geo_state": geo_state,
        "status": status,
        "is_fraud": str(is_fraud),
        "fraud_type": fraud_type,
        "source_extracted_at": datetime.now().isoformat(timespec="seconds"),
    }


# ===========================================================================
# 4) Contratos de credito
# ===========================================================================

def build_credit_contracts(rng: random.Random, profiles: List[CustomerProfile],
                           n_contracts: int, start: date, end: date,
                           default_dpd: int) -> List[Dict[str, Any]]:
    """Gera a carteira de credito com inadimplencia correlacionada ao risco."""
    rows: List[Dict[str, Any]] = []
    product_options = [((p, lo, hi, jlo, jhi, terms), w)
                       for p, w, lo, hi, jlo, jhi, terms in ref.CREDIT_PRODUCTS]
    # Probabilidade base de atraso por faixa de risco (proxy de PD).
    pd_by_band = {"A": 0.018, "B": 0.041, "C": 0.087, "D": 0.163, "E": 0.271}

    eligible = [p for p in profiles if p.risk_band != "A" or rng.random() < 0.5]

    for i in range(1, n_contracts + 1):
        profile = rng.choice(eligible)
        product, lo, hi, jlo, jhi, terms = weighted_pick(rng, product_options)

        # O principal e limitado pela capacidade de pagamento do cliente.
        cap = profile.income * rng.uniform(1.5, 12.0)
        principal = round(min(rng.uniform(lo, hi), max(cap, lo)), 2)
        rate = round(rng.uniform(jlo, jhi), 5)
        term = rng.choice(terms)

        origination = random_date(rng, max(start - timedelta(days=365), profile.signup_date), end)
        months_elapsed = max(0, (end.year - origination.year) * 12 + end.month - origination.month)
        installments_due = min(term, months_elapsed)

        # Probabilidade de atraso cresce com risco, comprometimento de renda e prazo.
        commitment = (principal / max(term, 1)) / max(profile.income, 500.0)
        pd = pd_by_band[profile.risk_band] * (1 + 2.2 * min(commitment, 1.2)) * \
            (1.25 if product in ("cartao_rotativo", "bnpl") else 1.0)
        pd = min(pd, 0.85)

        if rng.random() < pd:
            days_past_due = int(min(rng.expovariate(1 / 70.0) + 1, 540))
            installments_paid = max(0, installments_due - max(1, days_past_due // 30))
        else:
            days_past_due = 0
            installments_paid = installments_due

        amortized = principal * (installments_paid / term) if term else 0.0
        outstanding = round(max(principal - amortized, 0.0), 2)

        if installments_paid >= term and days_past_due == 0:
            status, outstanding = "settled", 0.0
        elif days_past_due >= WRITE_OFF_DPD:
            # Politica de write-off: acima de 180 dias o contrato vai a prejuizo
            # e sai da carteira ativa, passando para cobranca/recuperacao.
            status = "written_off"
        elif days_past_due >= default_dpd:
            status = "default"
        else:
            status = "active"

        # Parte do que foi para prejuizo retorna via cobranca/recuperacao.
        recovery = round(outstanding * rng.uniform(0.05, 0.35), 2) if status == "written_off" else 0.0

        rows.append({
            "contract_id": f"CTR-{i:07d}",
            "customer_id": profile.customer_id,
            "product": product,
            "principal_amount": f"{principal:.2f}",
            "interest_rate_month": f"{rate:.5f}",
            "term_months": str(term),
            "origination_date": origination.isoformat(),
            "installments_paid": str(installments_paid),
            "outstanding_balance": f"{outstanding:.2f}",
            "days_past_due": str(days_past_due),
            "status": status,
            "recovery_amount": f"{recovery:.2f}",
            "collateral": "sim" if product == "consignado" else "nao",
            "source_extracted_at": datetime.combine(end, datetime.min.time()).isoformat(),
        })

    return rows


# ===========================================================================
# 5) Injecao de "sujeira" (para a camada Silver ter o que limpar)
# ===========================================================================

def inject_dirty_records(rng: random.Random, rows: List[Dict[str, Any]],
                         dirty_rate: float) -> List[Dict[str, Any]]:
    """Corrompe uma fracao dos registros, imitando problemas reais de origem.

    Problemas injetados:
      1. valor nulo / vazio / negativo;
      2. timestamp invalido;
      3. categoria com caixa e espacos inconsistentes ("  PoS ");
      4. chave estrangeira orfa (cliente inexistente);
      5. linhas duplicadas (reprocessamento da origem).
    """
    if dirty_rate <= 0 or not rows:
        return rows

    # Cada tabela tem colunas diferentes; descobrimos quais problemas fazem
    # sentido a partir das colunas realmente presentes no registro.
    columns = rows[0].keys()
    numeric_col = next((c for c in ("amount", "principal_amount", "monthly_income") if c in columns), None)
    date_col = next((c for c in ("event_ts", "origination_date", "signup_date") if c in columns), None)
    category_col = next((c for c in ("channel", "status", "segment") if c in columns), None)
    # Chave estrangeira so e "orfa" em tabelas-filhas (transacao, contrato).
    has_fk = "customer_id" in columns and ("transaction_id" in columns or "contract_id" in columns)

    problems = ["duplicate"]
    if numeric_col:
        problems.append("bad_number")
    if date_col:
        problems.append("bad_date")
    if category_col:
        problems.append("messy_category")
    if has_fk:
        problems.append("orphan_fk")

    n_dirty = int(len(rows) * dirty_rate)
    indices = rng.sample(range(len(rows)), k=min(n_dirty, len(rows)))
    duplicates: List[Dict[str, Any]] = []

    for idx in indices:
        row = rows[idx]
        problem = rng.choice(problems)

        if problem == "bad_number":
            row[numeric_col] = rng.choice(["", "null", f"-{row[numeric_col]}"])
        elif problem == "bad_date":
            row[date_col] = rng.choice(["", "0000-00-00 00:00:00", "31/02/2024 99:99"])
        elif problem == "messy_category":
            value = str(row[category_col])
            row[category_col] = rng.choice([f"  {value.upper()} ", value.title(), f"{value}\t"])
        elif problem == "orphan_fk":
            row["customer_id"] = f"CUST-{rng.randint(900_000, 999_999):06d}"
        else:
            duplicates.append(dict(row))

    rows.extend(duplicates)
    log.info("Sujeira injetada: %s registros corrompidos, %s duplicatas", len(indices), len(duplicates))
    return rows


# ===========================================================================
# 6) Escrita dos arquivos
# ===========================================================================

def write_csv(path: Path, rows: List[Dict[str, Any]], exclude: Sequence[str] = ()) -> None:
    """Escreve uma lista de dicionarios em CSV com cabecalho."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [c for c in rows[0].keys() if c not in exclude]
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("CSV gravado: %s (%s linhas)", path, len(rows))


def split_batch_and_stream(rows: List[Dict[str, Any]], end: date, stream_days: int
                           ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Separa o historico (batch) dos ultimos dias (que viram eventos de streaming)."""
    cutoff = (datetime.combine(end, datetime.min.time()) - timedelta(days=stream_days)).isoformat(sep=" ")
    batch, stream = [], []
    for row in rows:
        # Registros com timestamp corrompido vao para o batch (a Silver os rejeita).
        (stream if row["event_ts"] >= cutoff and len(row["event_ts"]) == 19 else batch).append(row)
    return batch, stream


def _month_bucket(event_ts: str) -> str:
    """Extrai `YYYY-MM` de um timestamp; registros corrompidos vao para `invalid`."""
    try:
        parsed = datetime.fromisoformat(event_ts)
    except (ValueError, TypeError):
        return "invalid"
    return f"{parsed.year:04d}-{parsed.month:02d}"


def write_transactions_monthly(base_dir: Path, rows: List[Dict[str, Any]]) -> None:
    """Grava as transacoes em arquivos mensais, como uma extracao real faria."""
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(_month_bucket(row["event_ts"]), []).append(row)
    for month, month_rows in sorted(buckets.items()):
        write_csv(base_dir / f"transactions_{month}.csv", month_rows)


def write_streaming_batches(base_dir: Path, rows: List[Dict[str, Any]], n_batches: int) -> None:
    """Grava micro-lotes em JSONL, imitando um topico Kafka despejado no storage.

    O job de streaming (`bronze_transactions_stream.py`) le este diretorio com
    Structured Streaming, entao cada arquivo aqui equivale a um micro-batch.
    """
    base_dir.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    chunk = max(1, math.ceil(len(rows) / n_batches))
    for i in range(0, len(rows), chunk):
        batch_id = i // chunk
        target = base_dir / f"events_batch_{batch_id:03d}.json"
        with open(target, "w", encoding="utf-8") as handle:
            for row in rows[i:i + chunk]:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    log.info("Streaming: %s eventos em %s arquivos JSONL -> %s", len(rows),
             math.ceil(len(rows) / chunk), base_dir)


# ===========================================================================
# Orquestracao da geracao
# ===========================================================================

def generate(seed: int | None = None, n_customers: int | None = None,
             n_merchants: int | None = None, n_transactions: int | None = None,
             n_contracts: int | None = None, clean: bool = True) -> Dict[str, int]:
    """Gera todos os datasets brutos. Retorna um resumo com as contagens."""
    cfg = get_settings()
    gen = cfg.data_generation

    rng = random.Random(seed if seed is not None else gen["seed"])
    start = date.fromisoformat(str(gen["start_date"]))
    end = date.fromisoformat(str(gen["end_date"]))
    raw_dir = cfg.layer_path("raw")

    if clean and raw_dir.exists():
        shutil.rmtree(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    if clean:
        # O checkpoint do Structured Streaming guarda quais ARQUIVOS ja foram
        # lidos. Se a camada raw e recriada com os mesmos nomes de arquivo, o
        # stream considera tudo "ja processado" e ingere zero linhas - em
        # silencio. Regerar a origem invalida o checkpoint por definicao, entao
        # ele e removido junto. Em producao o equivalente e resetar o offset do
        # consumidor quando o topico e recriado.
        checkpoint_dir = cfg.layer_path("checkpoints") / "bronze_transactions_stream"
        if checkpoint_dir.exists():
            shutil.rmtree(checkpoint_dir)
            log.info("Checkpoint de streaming removido: %s", checkpoint_dir)

    with log_stage(log, "gerar clientes"):
        customers, profiles = build_customers(
            rng, n_customers or gen["n_customers"], start, end)

    with log_stage(log, "gerar merchants"):
        merchants = build_merchants(rng, n_merchants or gen["n_merchants"], start, end)

    with log_stage(log, "gerar transacoes"):
        transactions = build_transactions(
            rng, profiles, merchants,
            n_transactions or gen["n_transactions"],
            float(gen["fraud_rate"]), start, end,
        )

    with log_stage(log, "gerar contratos de credito"):
        contracts = build_credit_contracts(
            rng, profiles, n_contracts or gen["n_credit_contracts"], start, end,
            int(get_settings().business_rules["default_days_past_due"]),
        )

    with log_stage(log, "injetar registros sujos"):
        dirty_rate = float(gen["dirty_rate"])
        customers = inject_dirty_records(rng, customers, dirty_rate * 0.5)
        transactions = inject_dirty_records(rng, transactions, dirty_rate)
        contracts = inject_dirty_records(rng, contracts, dirty_rate * 0.5)

    with log_stage(log, "gravar arquivos raw"):
        write_csv(raw_dir / "customers" / "customers.csv", customers)
        # `_colluding` e um artefato do gerador: nao vaza para a camada raw.
        write_csv(raw_dir / "merchants" / "merchants.csv", merchants, exclude=["_colluding"])
        write_csv(raw_dir / "credit_contracts" / "credit_contracts.csv", contracts)

        batch_rows, stream_rows = split_batch_and_stream(transactions, end, stream_days=7)
        write_transactions_monthly(raw_dir / "transactions", batch_rows)
        write_streaming_batches(raw_dir / "streaming" / "transactions", stream_rows,
                                int(gen["streaming_batches"]))

    summary = {
        "customers": len(customers),
        "merchants": len(merchants),
        "transactions_batch": len(batch_rows),
        "transactions_stream": len(stream_rows),
        "credit_contracts": len(contracts),
        "fraud_transactions": sum(1 for t in transactions if t["is_fraud"] == "1"),
    }
    log.info("Resumo da geracao: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=f"Gera os dados sinteticos da fintech {BRAND} na camada raw.")
    parser.add_argument("--seed", type=int, default=None, help="semente aleatoria")
    parser.add_argument("--customers", type=int, default=None, help="numero de clientes")
    parser.add_argument("--merchants", type=int, default=None, help="numero de estabelecimentos")
    parser.add_argument("--transactions", type=int, default=None, help="numero de transacoes")
    parser.add_argument("--contracts", type=int, default=None, help="numero de contratos de credito")
    parser.add_argument("--keep-existing", action="store_true",
                        help="nao apaga a camada raw antes de gerar")
    args = parser.parse_args()

    generate(
        seed=args.seed, n_customers=args.customers, n_merchants=args.merchants,
        n_transactions=args.transactions, n_contracts=args.contracts,
        clean=not args.keep_existing,
    )


if __name__ == "__main__":
    main()
