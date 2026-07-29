"""
Fabrica da SparkSession com Delta Lake habilitado.

O mesmo codigo roda em dois cenarios:

1. **Local / PySpark puro** - baixamos os JARs do Delta via Maven na primeira
   execucao (`configure_spark_with_delta_pip`) e usamos `local[*]`.
2. **Databricks (Community ou workspace)** - o cluster ja tem Spark e Delta;
   nesse caso reaproveitamos a sessao existente sem reconfigurar nada.

A deteccao e automatica: se ja existir uma SparkSession ativa (caso do
Databricks/notebook), ela e reutilizada.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config import Settings, get_settings
from src.common.logging_utils import get_logger

log = get_logger(__name__)


def _running_on_databricks(spark: SparkSession) -> bool:
    """Heuristica simples para identificar um runtime Databricks."""
    return spark.conf.get("spark.databricks.clusterUsageTags.clusterName", None) is not None


def get_spark(app_suffix: str | None = None, settings: Settings | None = None) -> SparkSession:
    """Cria (ou reaproveita) a SparkSession do projeto.

    Args:
        app_suffix: sufixo do nome da aplicacao, normalmente o nome do job
            (ex.: "bronze_transactions"). Ajuda a identificar o job na Spark UI.
        settings: configuracao ja carregada; se omitida, e lida do YAML.
    """
    cfg = settings or get_settings()

    existing = SparkSession.getActiveSession()
    if existing is not None:
        log.info("Reaproveitando SparkSession existente (%s)", existing.sparkContext.appName)
        return existing

    app_name = cfg.spark["app_name"]
    if app_suffix:
        app_name = f"{app_name}::{app_suffix}"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(cfg.spark["master"])
        .config("spark.driver.memory", cfg.spark["driver_memory"])
        .config("spark.sql.shuffle.partitions", cfg.spark["shuffle_partitions"])
        # Extensoes que ligam a sintaxe e o catalogo do Delta no Spark SQL.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Warehouse local para as tabelas registradas no metastore embutido.
        .config("spark.sql.warehouse.dir", str(cfg.layer_path("root") / "_warehouse"))
        # Cache dos JARs baixados, para nao rebaixar a cada execucao.
        .config("spark.jars.ivy", str(cfg.project_root / ".ivy2"))
        .config("spark.ui.showConsoleProgress", "false")
    )

    for key, value in (cfg.spark.get("configs") or {}).items():
        builder = builder.config(key, value)

    try:
        # Resolve a versao correta do delta-core/delta-spark compativel com o
        # pacote pip instalado e injeta em spark.jars.packages.
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:  # pragma: no cover - ambiente Databricks
        log.info("Pacote `delta` nao encontrado no Python; assumindo runtime com Delta nativo.")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if _running_on_databricks(spark):
        log.info("Rodando no Databricks Runtime.")
    log.info("SparkSession pronta: %s (Spark %s)", app_name, spark.version)
    return spark


def stop_spark() -> None:
    """Encerra a sessao ativa, se houver. Usado ao final do pipeline local."""
    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
