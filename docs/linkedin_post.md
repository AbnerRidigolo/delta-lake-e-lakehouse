# Textos prontos para publicacao

Tres versoes do mesmo projeto, para contextos diferentes. Substitua
`[LINK DO REPOSITORIO]` pela URL do GitHub e ajuste os numeros com os da sua
execucao (eles aparecem no fim de `make pipeline`).

---

## 1. Post principal (LinkedIn)

> **Passei os ultimos dias construindo um Data Lakehouse completo com Delta Lake.
> O que mais aprendi nao foi sobre Spark — foi sobre como um dataset "limpo
> demais" mente para voce.**
>
> Deixa eu explicar.
>
> Montei do zero o pipeline de dados de uma fintech ficticia de pagamentos e
> credito: 250 mil transacoes, 5 mil clientes, 6,5 mil contratos de credito,
> ingestao batch e streaming, arquitetura Medallion (Bronze/Silver/Gold) sobre
> Delta Lake, feature store, modelo de deteccao de fraude e uma camada de IA
> generativa que responde perguntas de negocio em linguagem natural.
>
> Treinei o primeiro modelo de fraude e ele veio com **100% de recall**.
>
> Quem ja treinou modelo sabe: isso nao e vitoria, e sintoma. Fui investigar e
> achei o problema no meu proprio gerador de dados — **nenhuma transacao
> legitima tinha duas compras em 10 minutos**. Ou seja, "velocity alta"
> separava fraude de forma perfeita. O modelo nao tinha aprendido nada sobre
> fraude; tinha aprendido um artefato do meu codigo.
>
> A correcao foi modelar o comportamento legitimo com honestidade: sessoes de
> compra com varios itens, retentativas depois de uma recusa, gente trocando de
> celular e — o mais importante — cerca de 8% das fraudes **nunca sendo
> reportadas**. Rotulo ruidoso e a regra em antifraude, nao a excecao.
>
> Foi a parte mais valiosa do projeto inteiro.
>
> 🏗 **O que a arquitetura entrega**
>
> 🥉 **Bronze** — fidelidade absoluta a origem. Tudo entra como STRING (sim, de
> proposito: um valor `"N/A"` viraria NULL silencioso no cast automatico; como
> string, ele vira evidencia). Batch e streaming escrevem na MESMA tabela Delta —
> e o ACID garante que isso e seguro.
>
> 🥈 **Silver** — dedup deterministica, cast seguro, integridade referencial e
> LGPD (CPF mascarado + hash com salt). Nada e descartado em silencio: todo
> registro rejeitado vai para uma **quarentena com o motivo**. Quando o negocio
> pergunta "por que o faturamento caiu 4%?", a resposta esta a uma query de
> distancia.
>
> 🥇 **Gold** — 5 tabelas de negocio: visao 360 do cliente com score proprio,
> sinais de fraude por transacao (10 regras ponderadas + velocity), carteira de
> credito por safra com PD/LGD/EAD e perda esperada, DRE diaria e performance de
> lojistas.
>
> 🤖 **ML** — feature store com **correcao point-in-time**: features de julho so
> enxergam dados ate junho. Tem teste automatizado que falha se alguem quebrar
> isso. Split temporal (nao aleatorio), PR-AUC como metrica (nao ROC-AUC, que
> engana com 1% de positivos) e ponto de operacao escolhido por **custo real**:
> quanto se salva de fraude menos quanto custa a revisao manual.
>
> 🧠 **IA generativa** — RAG sobre a camada Gold, com o vector store sendo uma
> **tabela Delta** (versionamento e time travel valem para embeddings tambem).
> A decisao que mais importa: separar **extracao de fatos** (query Spark,
> auditavel) de **redacao** (LLM). O numero sempre vem de uma query, nunca da
> memoria do modelo. Em contexto financeiro, alucinar uma taxa de inadimplencia
> e inaceitavel.
>
> 🛠 **Stack:** Python, PySpark 3.5, Delta Lake 3.2, Airflow, scikit-learn,
> Structured Streaming.
>
> Roda inteiro na sua maquina com **um comando**. Sem cloud, sem chave de API.
>
> Codigo, documentacao de arquitetura e dicionario de dados abertos aqui:
> 👉 [LINK DO REPOSITORIO]
>
> Se voce ja pegou um vazamento sutil em producao, me conta nos comentarios —
> quero colecionar esses casos. 👇
>
> #DataEngineering #DeltaLake #Lakehouse #ApacheSpark #PySpark #MachineLearning
> #MLOps #RAG #IAGenerativa #Fintech #DadosBrasil #Databricks

---

## 2. Versao curta (para quem prefere post enxuto)

> Construi um **Data Lakehouse completo com Delta Lake** — do dado sintetico ao RAG.
>
> Uma fintech ficticia: 250 mil transacoes, ingestao batch + streaming,
> arquitetura Medallion, feature store point-in-time, modelo de fraude e uma
> camada de IA que responde perguntas de negocio em linguagem natural.
>
> A licao que levo: meu primeiro modelo deu **100% de recall**. Nao era vitoria,
> era vazamento — meu proprio gerador de dados nunca criava duas compras
> legitimas em 10 minutos, entao "velocity" separava fraude perfeitamente.
> Corrigir isso (sessoes de compra, retentativas, 8% de fraude nao reportada)
> foi mais educativo que treinar o modelo.
>
> 🛠 PySpark 3.5 · Delta Lake 3.2 · Airflow · scikit-learn
> ⚡ Roda local com um comando, sem cloud e sem chave de API
>
> 👉 [LINK DO REPOSITORIO]
>
> #DataEngineering #DeltaLake #Lakehouse #PySpark #MLOps #RAG

---

## 3. Versao para o README de perfil / portfolio

> **NeoPag Lakehouse** — Data Lakehouse de ponta a ponta com Delta Lake
>
> Pipeline completo de uma fintech ficticia: ingestao batch e streaming,
> arquitetura Medallion (Bronze/Silver/Gold), 5 tabelas Gold de negocio (risco de
> credito, antifraude, KPIs financeiros), feature store com correcao
> point-in-time, modelo de deteccao de fraude avaliado por custo e um pipeline
> RAG com vector store em Delta.
>
> Inclui governanca como codigo (catalogo declarativo -> Unity Catalog),
> framework proprio de data quality com quarentena e demonstracao pratica de time
> travel, `MERGE`, `RESTORE` e schema evolution.
>
> `Python` `PySpark` `Delta Lake` `Airflow` `scikit-learn` `RAG`

---

## Dicas de publicacao

**Imagem.** Posts com imagem tem alcance muito maior. Sugestoes, em ordem de
impacto:
1. O diagrama Mermaid do README renderizado (print da pagina do GitHub).
2. Um print do terminal com o resumo final do `make pipeline` (as 18 etapas com
   `[OK]` e os tempos) — mostra que o projeto **roda de verdade**.
3. Um print da resposta do RAG citando a tabela de origem.

**Carrossel (alto engajamento).** Um slide por tema:
`o problema` -> `arquitetura` -> `Bronze` -> `Silver` -> `Gold` ->
`o vazamento que encontrei` -> `feature store point-in-time` -> `RAG` ->
`link do repo`.

**Primeiro comentario.** Coloque o link do repositorio tambem no primeiro
comentario — o algoritmo do LinkedIn costuma penalizar link externo no corpo do
post.

**Horario.** Terca a quinta, entre 8h e 10h ou 12h e 13h (horario de Brasilia).

**Responda todo mundo** nas primeiras 2 horas: e o que sustenta o alcance.

**Numeros reais.** Antes de publicar, rode `make pipeline` e substitua os
numeros do texto pelos da sua execucao. Autenticidade aparece — e alguem sempre
pergunta.
