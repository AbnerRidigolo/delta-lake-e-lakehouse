"""
Quarentena de registros rejeitados na camada Silver.

Regra do projeto: **nenhum registro e descartado em silencio.** Toda linha que
nao passa nas validacoes vai para `data/silver/_quarantine/<tabela>` com o
motivo da rejeicao e a data do processamento.

Isso resolve dois problemas classicos:

* o time de negocio pergunta "por que o faturamento de ontem caiu 4%?" e a
  resposta esta a uma query de distancia;
* o time de engenharia consegue medir a qualidade da origem ao longo do tempo
  (a tabela de quarentena e, na pratica, um backlog priorizavel).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import write_delta
from src.common.logging_utils import get_logger

log = get_logger(__name__)


def quarantine(df: DataFrame, table: str) -> int:
    """Grava os registros rejeitados e devolve quantos foram.

    Os dados vao como STRING (o mesmo formato cru da Bronze) porque justamente
    o que falhou foi a conversao de tipo - forcar cast aqui perderia a evidencia.
    """
    cfg = get_settings()
    count = df.count()
    if count == 0:
        log.info("Quarentena %s: nenhum registro rejeitado", table)
        return 0

    path = str(cfg.layer_path("silver") / "_quarantine" / table)
    payload = (
        df.withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_quarantine_date", F.current_date())
    )
    write_delta(payload, path, mode="append", partition_by=["_quarantine_date"],
                merge_schema=True, comment=f"quarentena silver.{table}")

    breakdown = (
        df.groupBy("_reject_reason").count().orderBy(F.desc("count")).collect()
    )
    log.warning("Quarentena %s: %s registros rejeitados | %s", table, count,
                {row["_reject_reason"]: row["count"] for row in breakdown})
    return count
