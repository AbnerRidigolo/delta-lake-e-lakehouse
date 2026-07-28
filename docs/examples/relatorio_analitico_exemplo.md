# Relatorio analitico automatico - NeoPag

*Gerado automaticamente pelo pipeline em 2026-07-28T21:36:57 a partir das tabelas da camada Gold.*

**Periodo analisado:** 2024-01-01 a 2024-12-31

## 1. Resumo executivo

No periodo, a NeoPag processou **262.952** transacoes, movimentando um TPV de **R$ 102.488.301,50**. A receita total foi de **R$ 17.726.278,58** (R$ 2.543.346,88 de MDR e R$ 15.182.931,70 de juros da carteira de credito), com perdas de **R$ 7.364.290,92** e recuperacao de R$ 175.665,76. O resultado liquido somou **R$ 10.537.653,42**, uma margem de 59,4%.

O take rate medio ficou em **2,482%** e a taxa de aprovacao em **94,2%**.

Na comparacao de **2024-12** contra **2024-11**: receita com alta de 30,9%, TPV com alta de 28,6% e resultado com alta de 24,1%.

## 2. Evolucao mensal

| Mes | TPV | Receita | Perdas | Resultado | Aprovacao |
|---|---|---|---|---|---|
| 2024-01 | R$ 6.399.815,46 | R$ 500.486,58 | R$ 166.829,01 | R$ 333.657,57 | 94,2% |
| 2024-02 | R$ 6.248.169,76 | R$ 561.865,93 | R$ 199.719,92 | R$ 362.654,15 | 94,2% |
| 2024-03 | R$ 6.959.251,89 | R$ 696.710,13 | R$ 264.958,21 | R$ 431.751,92 | 94,0% |
| 2024-04 | R$ 6.700.605,04 | R$ 782.084,92 | R$ 319.185,33 | R$ 462.899,59 | 94,2% |
| 2024-05 | R$ 7.468.743,01 | R$ 951.723,35 | R$ 780.201,38 | R$ 173.407,13 | 94,2% |
| 2024-06 | R$ 7.495.228,84 | R$ 1.075.570,23 | R$ 448.428,17 | R$ 627.582,12 | 94,2% |
| 2024-07 | R$ 7.893.843,61 | R$ 1.292.823,82 | R$ 620.205,78 | R$ 672.618,04 | 94,5% |
| 2024-08 | R$ 8.178.314,71 | R$ 1.547.000,36 | R$ 312.506,92 | R$ 1.246.929,51 | 94,2% |
| 2024-09 | R$ 8.631.614,29 | R$ 1.810.564,22 | R$ 444.877,75 | R$ 1.367.210,59 | 94,4% |
| 2024-10 | R$ 10.196.027,80 | R$ 2.267.762,22 | R$ 1.061.640,23 | R$ 1.241.404,39 | 94,4% |
| 2024-11 | R$ 11.510.225,42 | R$ 2.702.644,27 | R$ 1.111.396,26 | R$ 1.614.236,40 | 94,2% |
| 2024-12 | R$ 14.806.461,67 | R$ 3.537.042,55 | R$ 1.634.341,96 | R$ 2.003.302,01 | 94,0% |

## 3. Anomalias detectadas

Deteccao por z-score robusto (mediana + MAD), limiar 3,5:

- **2024-12-20**: `tpv` = R$ 714.298,10, acima do normal (mediana do periodo: R$ 250.946,11, z = 8.72).
- **2024-12-23**: `tpv` = R$ 561.891,16, acima do normal (mediana do periodo: R$ 250.946,11, z = 5.85).
- **2024-12-25**: `tpv` = R$ 560.706,62, acima do normal (mediana do periodo: R$ 250.946,11, z = 5.83).
- **2024-11-14**: `tpv` = R$ 553.938,82, acima do normal (mediana do periodo: R$ 250.946,11, z = 5.7).
- **2024-12-29**: `tpv` = R$ 529.896,91, acima do normal (mediana do periodo: R$ 250.946,11, z = 5.25).
- **2024-05-29**: `net_result` = R$ -189.125,24, abaixo do normal (mediana do periodo: R$ 25.797,82, z = -10.51).

## 4. Risco de credito

A carteira soma **R$ 80.341.310,06** de exposicao (EAD) em 6.386 contratos. O saldo inadimplente (NPL 90+) e de R$ 5.383.997,40, equivalente a **6,70%** da carteira. A provisao constituida e de R$ 6.295.278,71, com indice de cobertura de **1.17x**. A perda esperada (EAD x PD x LGD) e de **R$ 1.928.200,26**.

**Safras que exigem atencao:**

| Safra | Produto | Faixa | Contratos | EAD | PD observada | Alerta |
|---|---|---|---|---|---|---|
| 2024-10 | bnpl | D | 9 | R$ 13.807,98 | 44,4% | safra_critica |
| 2024-07 | emprestimo_pessoal | E | 13 | R$ 309.020,74 | 30,8% | safra_critica |
| 2023-03 | cartao_rotativo | D | 10 | R$ 39.687,47 | 30,0% | safra_critica |
| 2024-04 | capital_giro_pj | D | 7 | R$ 250.773,87 | 28,6% | safra_critica |
| 2024-02 | emprestimo_pessoal | E | 7 | R$ 106.736,18 | 28,6% | safra_critica |

## 5. Base de clientes

Sao **4.957** clientes ativos na base, com score medio de **880.1**. Estao em risco de churn **496** clientes (**10,0%** da base) e 284 apresentam inadimplencia acima de 90 dias. Os 10% maiores clientes concentram **41,4%** do volume dos ultimos 90 dias.

| Segmento de valor/risco | Clientes | TPV 90d | Score medio |
|---|---|---|---|
| base_regular | 2279 | R$ 5.384.231,01 | 905.0 |
| alto_valor_baixo_risco | 1881 | R$ 26.604.748,85 | 927.0 |
| em_risco_de_churn | 482 | R$ 87.467,12 | 836.0 |
| alto_valor_alto_risco | 245 | R$ 3.968.756,15 | 528.0 |
| monitorar_credito | 70 | R$ 154.620,67 | 328.0 |

## 6. Antifraude

Foram identificadas **2.744** transacoes fraudulentas (1,044% do volume), somando **R$ 6.154.021,85**. O motor de regras gerou 889 alertas, com precisao de **43,3%** e recall de **14,0%**.

O modelo de ML (`HistGradientBoostingClassifier`) atingiu **PR-AUC de 0.9301** e ROC-AUC de 0.9994. No ponto de operacao otimizado por custo (limiar 0.25), entrega precisao de 66,6% e recall de 99,5%, com ganho financeiro de R$ 392.910,13 sobre o motor de regras.

Principais features do modelo: `tf_payment_method`, `tf_amount`, `tf_channel`, `tf_event_hour`, `tf_amount_vs_merchant_avg`.

| Padrao de fraude | Ocorrencias | Valor | Score medio das regras |
|---|---|---|---|
| account_takeover | 572 | R$ 4.264.045,52 | 38.8 |
| cnp_high_ticket | 154 | R$ 1.154.517,40 | 63.9 |
| merchant_collusion | 606 | R$ 717.120,79 | 7.5 |
| card_testing | 1412 | R$ 18.338,14 | 14.0 |

## 7. Carteira de estabelecimentos

**Lojistas em alerta de risco:**

| Estabelecimento | Categoria | Situacao | TPV | Chargeback | Fraude |
|---|---|---|---|---|---|
| Grupo Bandeirante 120 | eletronicos | descredenciar | R$ 510.982,89 | 10,31% | 18,75% |
| Distribuidora Primavera 288 | varejo_diverso | descredenciar | R$ 82.162,79 | 9,88% | 13,73% |
| Casa Atlantico 220 | supermercado | descredenciar | R$ 124.539,67 | 8,22% | 16,05% |
| Casa Delta 154 | apostas_online | descredenciar | R$ 236.601,91 | 7,63% | 17,72% |
| Loja Delta 116 | supermercado | descredenciar | R$ 181.436,15 | 7,49% | 14,59% |

**Maiores por volume:**

- Mercado Horizonte 123 (agencia_viagem): R$ 1.266.798,15 de TPV, R$ 38.130,60 de receita.
- Loja Nova Era 375 (joalheria): R$ 1.229.176,74 de TPV, R$ 34.785,70 de receita.
- Loja Uniao 139 (agencia_viagem): R$ 1.225.860,42 de TPV, R$ 37.388,74 de receita.
- Casa Delta 380 (agencia_viagem): R$ 1.182.878,53 de TPV, R$ 38.680,13 de receita.
- Rede Tropical 172 (eletronicos): R$ 1.131.264,36 de TPV, R$ 14.932,68 de receita.

## 8. Recomendacoes automaticas

- **Credito:** NPL de 6,70% acima do patamar confortavel de 5%. Priorizar acao de cobranca nas safras sinalizadas como criticas.
- **Retencao:** 10,0% da base esta inativa. Ativar campanha para o segmento `em_risco_de_churn`, que ja vem segmentado na `gold.customer_360`.
- **Antifraude:** o motor de regras captura apenas 14,0% das fraudes. Promover o modelo de ML a decisor primario, mantendo as regras como camada de seguranca e explicabilidade.
- **Adquirencia:** 5 estabelecimentos acima do limiar de chargeback das bandeiras. Iniciar plano de acao ou descredenciamento.

---

*Todos os numeros deste relatorio sao extraidos diretamente das tabelas Delta da camada Gold. A redacao e gerada por template deterministico; os fatos podem ser auditados pelas queries em `src/ai/insight_generator.py`.*