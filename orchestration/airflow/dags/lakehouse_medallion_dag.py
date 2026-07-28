"""
DAG do Airflow para o pipeline Medallion do lakehouse NeoPag.

Como este arquivo se relaciona com `orchestration/run_pipeline.py`
------------------------------------------------------------------
O grafo de dependencias e definido **uma unica vez**, na lista `PIPELINE` do
runner Python. Este DAG importa essa lista e gera as tasks a partir dela. Ou
seja: nao existem duas definicoes de pipeline para manter em sincronia — mudar
uma etapa no runner atualiza o DAG automaticamente.

Estrutura resultante:

    generate_data
        └── ingestao (TaskGroup)
              ├── bronze_batch
              └── bronze_stream
                    └── silver (TaskGroup)
                          ├── silver_customers
                          ├── silver_merchants
                          ├── silver_transactions
                          └── silver_credit_contracts
                                └── gold (TaskGroup)
                                      ├── gold_customer_360
                                      ├── gold_fraud_signals
                                      ├── gold_credit_risk
                                      ├── gold_financial_kpis
                                      └── gold_merchant_performance
                                            ├── ml (TaskGroup)
                                            │     ├── feature_store
                                            │     └── train_fraud_model
                                            ├── ia (TaskGroup)
                                            │     ├── ai_insights
                                            │     └── rag_index
                                            └── governanca (TaskGroup)
                                                  ├── catalog_metadata
                                                  └── delta_maintenance

Como usar
---------
1. `pip install apache-airflow==2.9.3`
2. Copie (ou faca symlink de) este arquivo para `$AIRFLOW_HOME/dags/`.
3. Garanta que a raiz do projeto esteja no `PYTHONPATH` do Airflow.
4. Configure a variavel `LAKEHOUSE_ROOT` se o storage nao for local.

Em producao com Databricks, cada `PythonOperator` viraria um
`DatabricksSubmitRunOperator` apontando para um job/notebook do workspace — a
estrutura do DAG permaneceria identica.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Garante que `src/` e `orchestration/` sejam importaveis quando o Airflow
# carrega este arquivo a partir de outro diretorio.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG  # noqa: E402
from airflow.operators.empty import EmptyOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from airflow.utils.task_group import TaskGroup  # noqa: E402

from orchestration.run_pipeline import PIPELINE, STAGES_BY_NAME  # noqa: E402

# Mapa etapa -> TaskGroup, para o DAG ficar legivel na interface do Airflow.
TASK_GROUPS = {
    "generate_data": None,
    "bronze_batch": "ingestao",
    "bronze_stream": "ingestao",
    "silver_customers": "silver",
    "silver_merchants": "silver",
    "silver_transactions": "silver",
    "silver_credit_contracts": "silver",
    "gold_customer_360": "gold",
    "gold_fraud_signals": "gold",
    "gold_credit_risk": "gold",
    "gold_financial_kpis": "gold",
    "gold_merchant_performance": "gold",
    "feature_store": "ml",
    "train_fraud_model": "ml",
    "ai_insights": "ia",
    "rag_index": "ia",
    "catalog_metadata": "governanca",
    "delta_maintenance": "governanca",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    # Sem alerta por e-mail neste projeto; em producao, plugar no on-call.
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Uma etapa travada nunca deve segurar o slot do scheduler indefinidamente.
    "execution_timeout": timedelta(hours=2),
}


def make_callable(stage_name: str):
    """Cria o callable da task, propagando o `run_id` do Airflow para o lakehouse.

    Usar o `run_id` do Airflow como `PIPELINE_RUN_ID` faz com que as colunas de
    auditoria das tabelas Delta apontem diretamente para a execucao do DAG.
    Rastrear "qual DAG run gravou esta linha" vira uma query.
    """
    def _run(**context):
        os.environ["PIPELINE_RUN_ID"] = str(context["run_id"])[:32]
        return STAGES_BY_NAME[stage_name].runner()

    return _run


with DAG(
    dag_id="lakehouse_medallion_neopag",
    description="Pipeline Medallion (Bronze/Silver/Gold) + Feature Store + IA sobre Delta Lake",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * *",              # diariamente as 05:00
    start_date=datetime(2024, 1, 1),
    catchup=False,                     # nao reprocessa o historico ao ligar o DAG
    max_active_runs=1,                 # evita duas execucoes escrevendo na mesma tabela
    tags=["lakehouse", "delta", "medallion", "fintech", "ml", "genai"],
    doc_md=__doc__,
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(task_id="fim", trigger_rule="all_done")

    tasks = {}
    groups = {}

    # 1) Cria as tasks, agrupadas por TaskGroup.
    for stage in PIPELINE:
        group_name = TASK_GROUPS.get(stage.name)

        if group_name and group_name not in groups:
            groups[group_name] = TaskGroup(group_id=group_name, dag=dag)

        tasks[stage.name] = PythonOperator(
            task_id=stage.name,
            python_callable=make_callable(stage.name),
            task_group=groups.get(group_name) if group_name else None,
            doc_md=f"**{stage.description}**\n\n"
                   f"Depende de: {', '.join(stage.depends_on) or 'nenhuma etapa'}.",
            # Etapas nao criticas nao derrubam o DAG inteiro.
            retries=stage.retries + (0 if stage.critical else 1),
            dag=dag,
        )

    # 2) Liga as dependencias declaradas no runner Python.
    for stage in PIPELINE:
        if not stage.depends_on:
            inicio >> tasks[stage.name]
        for upstream in stage.depends_on:
            tasks[upstream] >> tasks[stage.name]

    # 3) Tudo que nao tem filho aponta para o marcador de fim.
    com_filhos = {dep for stage in PIPELINE for dep in stage.depends_on}
    for stage in PIPELINE:
        if stage.name not in com_filhos:
            tasks[stage.name] >> fim
