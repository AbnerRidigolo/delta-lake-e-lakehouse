"""
Logging padronizado para os jobs do lakehouse.

Todos os jobs escrevem no mesmo formato, o que facilita ler a saida do
orquestrador (`orchestration/run_pipeline.py`) e do Airflow.
"""

from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager

_LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s"
_configured = False


def get_logger(name: str) -> logging.Logger:
    """Devolve um logger ja configurado (idempotente)."""
    global _configured
    if not _configured:
        logging.basicConfig(
            level=logging.INFO,
            format=_LOG_FORMAT,
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )
        # O Spark e barulhento demais no nivel INFO.
        logging.getLogger("py4j").setLevel(logging.ERROR)
        logging.getLogger("pyspark").setLevel(logging.WARN)
        _configured = True
    return logging.getLogger(name)


@contextmanager
def log_stage(logger: logging.Logger, stage: str):
    """Mede e loga a duracao de uma etapa do pipeline.

    Uso:
        with log_stage(log, "bronze.transactions"):
            ...
    """
    logger.info("[INICIO] %s", stage)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        logger.exception("[FALHA]  %s", stage)
        raise
    else:
        elapsed = time.perf_counter() - started
        logger.info("[OK]     %s concluido em %.2fs", stage, elapsed)
