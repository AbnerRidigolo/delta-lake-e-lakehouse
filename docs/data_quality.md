# Qualidade de dados e CI/CD

Como este projeto garante que o dado esta correto — e como o CI garante que ele
continua correto depois de cada mudanca de codigo.

---

## 1. O problema com "temos data quality"

Quase todo pipeline tem alguma checagem de qualidade. Quase sempre e a mesma
coisa: `not_null` e `unique` em algumas colunas. Isso cobre uma fatia pequena
dos defeitos reais.

Os problemas que passam batido por expectativas por coluna:

| Defeito | Por que a expectativa por coluna nao ve |
|---|---|
| Perda de 12% das linhas num join | As linhas que restaram estao todas validas |
| Coluna removida na origem | `mergeSchema` absorve; a coluna vira NULL em silencio |
| Centavos lidos como reais | Cada valor individual esta dentro da faixa |
| Duplicacao de valor num `explode` | A contagem de chaves nao muda |
| A populacao mudou | Todas as linhas continuam validas |
| Escrita fora do pipeline | A expectativa so roda dentro do pipeline |

Este projeto ataca cada um deles com uma tecnica especifica.

---

## 2. As seis camadas de qualidade

```
                        ┌─────────────────────────────────────────┐
   escrita ────────────>│ 1. CHECK constraints (Delta)            │  o storage recusa
                        └─────────────────────────────────────────┘
                        ┌─────────────────────────────────────────┐
   apos escrever ──────>│ 2. Expectativas por coluna              │  nulo, dominio, faixa
                        │ 3. Expectativas estendidas              │  FK, chave composta, SQL
                        └─────────────────────────────────────────┘
                        ┌─────────────────────────────────────────┐
   entre camadas ──────>│ 4. Conciliacao                          │  nada sumiu?
                        └─────────────────────────────────────────┘
                        ┌─────────────────────────────────────────┐
   contra o passado ───>│ 5. Contratos de schema                  │  a estrutura mudou?
                        │ 6. Deteccao de drift                    │  a populacao mudou?
                        └─────────────────────────────────────────┘
                                          │
                                          v
                              ┌───────────────────────┐
                              │  Scorecard + portao   │
                              └───────────────────────┘
```

### 1. CHECK constraints — `src/quality/constraints.py`

A unica camada que **impede** o dado ruim de existir; as outras apenas o medem.
Fica no protocolo da tabela Delta, entao vale para qualquer escrita — inclusive
a do notebook do analista e a do backfill manual, que nao passam pelo pipeline.

```sql
ALTER TABLE delta.`.../silver/transactions`
  ADD CONSTRAINT valor_positivo CHECK (amount > 0);
```

31 constraints em 9 tabelas. **A Bronze nao recebe nenhuma**, de proposito: ela
existe para absorver o dado como veio. Constraint na Bronze rejeitaria na
ingestao o registro que a Silver deveria capturar e explicar.

### 2. Expectativas por coluna — `src/common/data_quality.py`

`not_null`, `unique`, `allowed_values`, `between`, `matches_regex`,
`row_count_min`, `freshness_max_days`. Severidade `error` (derruba) ou `warn`
(registra), com tolerancia configuravel por regra.

As checagens de linha rodam **em uma unica passada** com `agg`, e nao N varreduras.

### 3. Expectativas estendidas — `src/quality/expectations.py`

As que precisam de mais de uma coluna, mais de uma tabela ou da distribuicao:

| Tipo | Pega |
|---|---|
| `composite_unique` | O grao da tabela quebrou (ex.: duas linhas para o mesmo lojista+mes) |
| `referential_integrity` | FK apontando para registro inexistente |
| `mean_between` | A media mudou, mesmo com toda linha valida (erro de escala) |
| `distinct_count_between` | Cardinalidade explodiu ou desabou |
| `custom_sql` | Regra de negocio arbitraria |

Exemplos reais do projeto:

```python
custom_sql("sem_vazamento_temporal",
           "feature_month < date_format(event_date, 'yyyy-MM')")
custom_sql("resultado_fecha",
           "abs(net_result - (revenue_total - loss_total + recovery_amount)) < 0.05")
custom_sql("npl_coerente_com_atraso", "(days_past_due >= 90) = is_npl")
```

A primeira e a mais importante: aplica **em producao, a cada execucao**, o mesmo
invariante de vazamento point-in-time que os testes cobrem em CI.

### 4. Conciliacao — `src/quality/reconciliation.py`

Duas identidades que precisam fechar:

```
linhas da Bronze  ==  linhas da Silver  +  linhas em quarentena
soma de valor na Silver  ==  soma de valor na Gold
```

Nove identidades verificadas. Se a primeira nao fecha, existe um caminho no
codigo que descarta registro sem registrar o motivo. Se a segunda nao fecha,
dinheiro apareceu ou sumiu entre camadas — inaceitavel em dado financeiro.

**Tolerancia calculada, nao chutada.** A Gold arredonda para centavos dentro de
cada grupo, entao somar 366 grupos acumula ate meio centavo por grupo. A
tolerancia e `0,005 x 2 x n_grupos` — o limite teorico do erro de
arredondamento. Uma diferenca maior que isso nao e arredondamento, e defeito.

> Um alarme que dispara sozinho e pior do que nenhum alarme: ele treina o time
> a ignora-lo.

**Reexecucao parcial.** A quarentena e append-only e acumula historico. Para
saber quais registros pertencem a versao atual da Silver, o modulo le o
`userMetadata` do historico de commits do Delta, onde o pipeline carimba o
`run_id` de quem escreveu. E um caso em que o log de transacoes do Delta
responde uma pergunta que os dados da tabela sozinhos nao respondem.

### 5. Contratos de schema — `src/quality/schema_contract.py`

O schema de cada tabela Silver/Gold e congelado em `contracts/*.json`,
versionado no Git. A cada execucao, as diferencas sao classificadas:

| Classificacao | Exemplo | Resultado |
|---|---|---|
| **BREAKING** | Coluna removida; `double` -> `string`; `NOT NULL` -> `NULL` | Falha o pipeline |
| **ADDITIVE** | Coluna nova | Passa, registrado |
| **COMPATIBLE** | `int` -> `bigint`, `float` -> `double` | Passa |

Aceitar uma mudanca exige rodar `--update`, o que gera **diff no Git** e passa
por code review. Ninguem muda o contrato de uma tabela sem que apareca no PR.

A Bronze fica fora de contrato de proposito: absorver mudanca de origem e
exatamente a funcao dela.

### 6. Deteccao de drift — `src/quality/drift.py`

**Volume:** o total de hoje contra a **mediana** das execucoes anteriores
(guardadas na propria `quality.dq_results`). Mediana, e nao media, porque uma
execucao anomala envenena a media e mascara as seguintes.

**Distribuicao (PSI):** o Population Stability Index, padrao de mercado em risco
de credito:

```
PSI = Σ (%atual − %referencia) × ln(%atual / %referencia)

PSI < 0,10          populacao estavel
0,10 ≤ PSI < 0,25   mudanca moderada
PSI ≥ 0,25          mudanca relevante
```

Os cortes vem dos quantis da amostra de **referencia**, nunca da atual —
recalcular na atual esconderia o proprio deslocamento que se quer medir.

**Politica de bloqueio calibrada:** uma feature deslocando e normal (a populacao
muda, a base amadurece). Varias ao mesmo tempo indica mudanca estrutural. O
bloqueio so ocorre acima de `psi_max_drifting_features`.

> **Este monitor achou um defeito real neste projeto.** A feature
> `tf_customer_txn_seq` (numero da transacao no historico do cliente) marcou
> PSI = 0,93. Diagnostico: contador monotonico — num split temporal o periodo
> recente sempre tem valores maiores, e em producao a feature assumiria valores
> que nunca existiram no treino. Limitar o valor nao resolvia; a tendencia
> permanecia. A correcao foi trocar a feature por `tf_is_first_transaction`, que
> mantem o sinal util sem a tendencia. Ver `src/ml/feature_store.py`.

### 7. Scorecard e portao — `src/quality/scorecard.py`

114 checagens por execucao viram um numero e uma decisao:

- nota por tabela, por camada e geral (0–100), ponderada por severidade
  (uma falha `error` custa 3x uma `warn`);
- penalidade direta por falha de conciliacao (−10) e por drift relevante (−5),
  que sao defeitos estruturais e nao violacoes pontuais;
- relatorio em Markdown publicado no `$GITHUB_STEP_SUMMARY` — aparece direto na
  aba do Pull Request;
- **portao**: abaixo de `scorecard_min_score`, o build falha.

---

## 3. Onde os resultados ficam

Tudo em tabelas Delta, o que permite consultar a evolucao da qualidade ao longo
do tempo em vez de ler log:

| Tabela | Conteudo |
|---|---|
| `quality.dq_results` | Toda expectativa executada, com violacoes e severidade |
| `quality.reconciliation_results` | Cada identidade conciliada, esperado vs. obtido |
| `quality.drift_results` | Sinais de drift de volume e PSI |
| `quality.scorecard` | Nota por tabela e por execucao |
| `silver._quarantine.<tabela>` | Registros rejeitados, com o motivo |

```sql
-- A qualidade da tabela de transacoes esta melhorando ou piorando?
SELECT date(checked_at) AS dia, round(avg(score), 1) AS nota
FROM delta.`data/quality/scorecard`
WHERE table = 'silver.transactions'
GROUP BY 1 ORDER BY 1;
```

---

## 4. CI/CD

### O principio

**O CI executa o pipeline inteiro** — as 23 etapas, as 114 checagens — usando o
perfil `ci`, que reduz apenas o **volume** dos dados (12 mil transacoes em vez
de 260 mil). Nenhuma etapa e pulada, nenhuma regra e afrouxada.

Um CI que exercita um caminho diferente do de producao nao testa producao.

O perfil e um overlay mesclado sobre a configuracao base
(`config/settings.ci.yaml` + `LAKEHOUSE_PROFILE=ci`), entao nao existe uma
segunda copia da configuracao para divergir.

### `ci.yml` — a cada push e Pull Request

```
lint ──────────────┐
                   ├──> pipeline completo + portao de qualidade
testes rapidos ────┘
```

| Job | O que faz | Duracao |
|---|---|---|
| `lint` | `ruff check` + `ruff format --check` + `compileall` | ~20 s |
| `testes-rapidos` | Testes sem Spark (config, DAG, catalogo, pesos das regras) | ~15 s |
| `pipeline` | Pipeline completo, testes de integracao, portao de qualidade | ~4 min |

Detalhes que importam:

- **Cache dos JARs do Delta** (`.ivy2`): evita rebaixar do Maven a cada push.
- **`PIPELINE_RUN_ID=gha-${{ github.run_id }}`**: as colunas de auditoria das
  tabelas passam a apontar para a execucao do workflow. Rastrear "qual build
  gerou esta linha" vira uma query.
- **Scorecard no resumo do job**: o relatorio aparece na aba do PR, sem baixar
  artefato.
- **Diagnostico automatico em caso de falha**: quando o portao reprova, um passo
  publica a tabela de regras que falharam direto no resumo.
- **`concurrency` com `cancel-in-progress`**: um push novo cancela o anterior.

### `data-quality.yml` — todo dia

Valida o **dado**, nao a mudanca de codigo. E o que da sentido a deteccao de
drift: comparar hoje com a mediana historica exige uma serie historica, que este
agendamento constroi (a tabela `quality.dq_results` e restaurada do cache entre
execucoes).

Quando reprova, **abre uma issue** com o rotulo `qualidade-de-dados` — ou
comenta na issue ja aberta, para nao empilhar uma por dia. Falha de
monitoramento agendado que so vai para o log de workflow nao e vista por
ninguem.

### Dependabot

Atualiza dependencias semanalmente, agrupando `pyspark` + `delta-spark` (as
versoes sao acopladas). Como o CI roda o pipeline inteiro, cada PR do Dependabot
vira um **teste de compatibilidade automatico**: a atualizacao so passa se o
pipeline continuar produzindo dado valido.

### Pre-commit

O mesmo lint do CI, antes do push, mais um hook local que **bloqueia commit de
dado gerado** (`data/`, `artifacts/`, `reports/`) — o erro mais comum em
repositorio de engenharia de dados.

```bash
pip install pre-commit && pre-commit install
```

---

## 5. Como isso se traduz para producao

| Aqui | Producao |
|---|---|
| Framework proprio de expectativas | Great Expectations, Soda, ou DLT expectations |
| Scorecard em Markdown | Painel de qualidade (Grafana, Databricks Lakehouse Monitoring) |
| Issue automatica | PagerDuty / Opsgenie / canal do time |
| Contratos em JSON no Git | Schema Registry, ou contratos do Unity Catalog |
| PSI em batch | Monitoramento continuo com alerta por feature |
| Portao no CI | Portao no orquestrador, entre a Gold e a publicacao |

As tecnicas nao mudam — muda quem as executa e para onde vai o alerta.
