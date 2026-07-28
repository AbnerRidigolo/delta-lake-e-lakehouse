# Governanca de dados

Como o projeto trata catalogo, classificacao, LGPD, qualidade e linhagem — e
como cada peca se traduz para Unity Catalog (Databricks) ou DataHub (open source).

---

## 1. Principio: governanca como codigo

Catalogo preenchido a mao envelhece em semanas. Neste projeto os metadados vivem
em [`config/catalog.yaml`](../config/catalog.yaml), passam por code review e sao
aplicados pelo pipeline — a mesma disciplina que se usa para infraestrutura.

```
config/catalog.yaml                    (fonte da verdade, versionada no Git)
        │
        ├──> TBLPROPERTIES nas tabelas Delta   (metadado viaja com o dado)
        ├──> docs/generated/data_catalog.md    (documentacao legivel)
        └──> docs/generated/unity_catalog_setup.sql  (equivalente gerenciado)
```

Executado por `src/governance/apply_catalog_metadata.py`, que roda como etapa do
pipeline. Nenhum passo manual.

---

## 2. Modelo de catalogo

### Namespace

| Ambiente | Forma | Exemplo |
|---|---|---|
| Databricks / Unity Catalog | 3 niveis | `fintech.gold.customer_360` |
| Local (metastore embutido) | 2 niveis | `gold.customer_360` |

O metastore Hive embutido nao suporta catalogo nomeado — namespace de tres
partes e um recurso do Unity Catalog. `Settings.full_table_name()` resolve isso
automaticamente conforme o ambiente; e a **mesma tabela Delta**, muda so como
ela e referenciada.

### Dominios e propriedade

Cada tabela pertence a um dominio de negocio com **owner** (o time que produz o
dado e responde por ele) e **steward** (quem zela pela conformidade):

| Dominio | Owner | Steward |
|---|---|---|
| `customer` | squad-customer-360 | data-governance |
| `payments` | squad-payments | data-governance |
| `credit` | squad-credit-risk | risk-office |
| `finance` | squad-fpna | controladoria |
| `ml` | squad-ml-platform | data-governance |

E o modelo de **Data Mesh** aplicado sem cerimonia: o time que entende do
assunto e dono do produto de dados; a governanca central define o padrao, nao o
conteudo.

---

## 3. Classificacao e controle de acesso

| Classificacao | Significado | Grupos com acesso |
|---|---|---|
| `PUBLIC` | Pode ir para relatorio externo | analistas, engenharia, ciencia de dados, negocio |
| `INTERNAL` | Uso interno, sem dado pessoal | analistas, engenharia, ciencia de dados |
| `CONFIDENTIAL` | Dado financeiro/risco sensivel | engenharia, ciencia de dados, risco |
| `PII` | Dado pessoal (LGPD) | engenharia, DPO |

No Unity Catalog isso vira GRANT de verdade:

```sql
GRANT SELECT ON TABLE fintech.gold.customer_360 TO `risco`;
ALTER TABLE fintech.silver.customers
  ALTER COLUMN cpf_masked SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');
```

O arquivo completo e gerado em
[`docs/generated/unity_catalog_setup.sql`](generated/unity_catalog_setup.sql).

---

## 4. LGPD na pratica

| Camada | Tratamento | Justificativa |
|---|---|---|
| **Bronze** | Dado pessoal preservado **cru**, acesso restrito | Bronze e a copia fiel da origem — mascarar aqui destruiria a capacidade de auditar e reprocessar |
| **Silver** | CPF e e-mail **mascarados**; `customer_key` = SHA-256(CPF + salt) | Analytics e ML nao precisam do documento; precisam de uma chave estavel para join |
| **Gold** | Nenhuma coluna de identificacao direta | Camada de consumo amplo |

**Direito ao esquecimento.** Com Parquet puro, apagar um cliente significa
reescrever particoes inteiras na mao. Com Delta, e uma operacao:

```sql
DELETE FROM silver.customers WHERE customer_id = 'CUST-000123';
VACUUM silver.customers RETAIN 168 HOURS;  -- remove fisicamente apos a retencao
```

O `DELETE` e transacional e o `VACUUM` remove os arquivos antigos. Atencao: ate
o VACUUM rodar, o dado continua acessivel via time travel — o que e correto do
ponto de vista de auditoria, mas precisa estar documentado na politica de
retencao.

**Minimizacao.** As tabelas Gold e a feature store nao carregam nome, CPF nem
e-mail: apenas `customer_id` e `customer_key`.

---

## 5. Qualidade de dados

Framework proprio em `src/common/data_quality.py` (~250 linhas), com resultados
persistidos em `data/quality/dq_results` — o que permite montar um painel
historico de qualidade em vez de so um log que ninguem le.

### Tipos de expectativa

| Tipo | Uso |
|---|---|
| `not_null` | Chaves e campos obrigatorios |
| `unique` | Chave primaria (na Bronze, apenas `warn` — duplicata la e esperada) |
| `allowed_values` | Dominio de categorias |
| `between` | Faixa valida de valores numericos |
| `matches_regex` | Formato (e-mail, documento) |
| `row_count_min` | Deteccao de arquivo truncado na origem |
| `freshness_max_days` | Dado parou de chegar |

### Severidade e tolerancia

- `error` -> interrompe o pipeline (`DataQualityError`).
- `warn` -> registra e segue.
- `threshold` -> fracao de linhas que pode violar sem falhar (nem toda regra
  admite zero excecao).

### Quarentena

Registro rejeitado na Silver nao e descartado: vai para
`silver/_quarantine/<tabela>` com `_reject_reason`. Motivos observados na
execucao de referencia: chave estrangeira orfa, timestamp invalido, valor nao
numerico, valor nao positivo, categoria fora do dominio.

```sql
-- Diagnostico de qualidade da origem
SELECT _reject_reason, count(*) AS registros
FROM delta.`data/silver/_quarantine/transactions`
GROUP BY _reject_reason ORDER BY registros DESC;
```

---

## 6. Linhagem

Duas camadas de linhagem, uma tecnica e uma de negocio:

**No nivel da linha** — colunas tecnicas em todas as tabelas:

| Coluna | Responde |
|---|---|
| `_source_file` | De qual arquivo esta linha veio |
| `_source_system` | De qual sistema de origem |
| `_ingested_at` | Quando chegou |
| `_pipeline_run_id` | Qual execucao do pipeline a gravou |

**No nivel do commit** — `DESCRIBE HISTORY` mostra, para cada versao da tabela,
a operacao, as metricas e o `userMetadata` que o pipeline carimbou:

```sql
DESCRIBE HISTORY delta.`data/silver/transactions`;
```

**No nivel do grafo** — o DAG em `orchestration/run_pipeline.py` declara as
dependencias entre tabelas de forma explicita e legivel. E a linhagem que o
Unity Catalog (ou o DataHub) construiria automaticamente a partir das queries.

---

## 7. Retencao e custo

| Camada | Retencao sugerida | Racional |
|---|---|---|
| Raw | 90 dias | Reprocessamento de emergencia |
| Bronze | 2 anos | Auditoria e reconstrucao das camadas superiores |
| Silver | Indefinida | Base analitica |
| Gold | Indefinida | Volume pequeno |
| Time travel (Delta log) | 30 dias | Equilibrio entre auditoria e custo de storage |

`VACUUM` roda em `src/maintenance/delta_maintenance.py`. Cuidado: retencao curta
demais apaga a capacidade de time travel e pode remover arquivos que uma query
longa ainda esta lendo — por isso o padrao do Delta e 7 dias.

---

## 8. Como isso vira Unity Catalog ou DataHub

| Conceito do projeto | Unity Catalog | DataHub |
|---|---|---|
| `config/catalog.yaml` | Objetos gerenciados + TAGS | Ingestion recipe (arquivo/API) |
| Classificacao | Tags de coluna + row/column masking | Glossary terms + tags |
| Owner / steward | Object owner + grupos | Ownership types |
| Grupos de acesso | `GRANT SELECT ... TO` | Politicas de acesso |
| Linhagem | Automatica pelas queries | Emitida via API ou plugin do Spark |
| Qualidade (`dq_results`) | Lakehouse Monitoring / DLT expectations | Assertions |
| Feature store | Databricks Feature Store | MLFeatureTable |

A escolha entre os dois e de plataforma, nao de arquitetura: o modelo
declarativo deste projeto alimenta qualquer um dos dois.
