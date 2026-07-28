"""
Demonstracao pratica dos recursos do Delta Lake que justificam a arquitetura.

Este script e didatico por design: cada bloco executa uma operacao, mostra o
resultado e explica o que aconteceu. Rode-o depois do pipeline principal.

Recursos demonstrados
---------------------
1. **Historico e auditoria** - `DESCRIBE HISTORY`: quem escreveu, quando, o que
   mudou e qual `pipeline_run_id` originou o commit.
2. **Time travel** - ler a tabela como ela era na versao N (`versionAsOf`) ou em
   um instante do passado (`timestampAsOf`).
3. **Schema evolution** - adicionar coluna nova sem reescrever nem quebrar quem
   ja consome a tabela.
4. **MERGE (upsert)** - aplicar correcao tardia da origem sem duplicar registro.
   E a operacao que torna CDC e SCD viaveis em um data lake.
5. **RESTORE** - desfazer uma escrita ruim voltando a tabela a uma versao
   anterior, com um comando.
6. **Constraints (CHECK)** - impedir que dado invalido entre na tabela, no
   nivel do storage, nao do job.

Tudo roda em uma copia isolada (`data/_demo/`), entao nao ha risco para as
tabelas do pipeline.

Execucao:
    python -m src.maintenance.delta_features_demo
"""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import get_settings
from src.common.delta_io import describe_history, read_delta, write_delta
from src.common.logging_utils import get_logger
from src.common.spark_session import get_spark

log = get_logger(__name__)

DEMO_TABLE = "transactions_demo"


def _banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def run() -> None:
    cfg = get_settings()
    spark = get_spark("delta_features_demo")

    demo_dir = Path(cfg.layer_path("root")) / "_demo"
    if demo_dir.exists():
        shutil.rmtree(demo_dir)
    demo_path = str(demo_dir / DEMO_TABLE)

    source = spark.read.format("delta").load(cfg.table_path("silver", "transactions"))

    # ==================================================================== v0
    _banner("VERSAO 0 - carga inicial")
    base = source.select("transaction_id", "event_date", "customer_id", "merchant_id",
                         "amount", "channel", "status").limit(5000)
    write_delta(base, demo_path, mode="overwrite", partition_by=["event_date"],
                comment="carga inicial da demo")
    print(f"Linhas gravadas: {read_delta(spark, demo_path).count()}")

    # ==================================================================== v1
    _banner("VERSAO 1 - SCHEMA EVOLUTION (coluna nova sem quebrar a tabela)")
    print("A origem passou a enviar `review_channel`. Com mergeSchema=true o Delta")
    print("adiciona a coluna e preenche as linhas antigas com NULL - sem reescrever")
    print("a tabela e sem quebrar quem le so as colunas antigas.\n")

    incremento = (
        source.select("transaction_id", "event_date", "customer_id", "merchant_id",
                      "amount", "channel", "status")
        .orderBy(F.desc("event_date")).limit(300)
        .withColumn("review_channel", F.lit("app_notificacao"))
    )
    write_delta(incremento, demo_path, mode="append", partition_by=["event_date"],
                merge_schema=True, comment="schema evolution: +review_channel")

    evolved = read_delta(spark, demo_path)
    print("Schema apos a evolucao:")
    for field in evolved.schema.fields:
        print(f"   - {field.name}: {field.dataType.simpleString()}")
    print(f"\nLinhas com a coluna nova preenchida: "
          f"{evolved.where(F.col('review_channel').isNotNull()).count()}")
    print(f"Linhas antigas (review_channel NULL): "
          f"{evolved.where(F.col('review_channel').isNull()).count()}")

    # ==================================================================== v2
    _banner("VERSAO 2 - MERGE (correcao tardia da origem, sem duplicar)")
    print("Cenario real: o sistema de origem reprocessou e corrigiu o valor de")
    print("algumas transacoes. Sem MERGE, a opcao seria reescrever a particao")
    print("inteira. Com MERGE, atualizamos so o que mudou - e inserimos o que e novo.\n")

    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, demo_path)
    amostra = read_delta(spark, demo_path).limit(50).select("transaction_id").collect()
    ids = [row["transaction_id"] for row in amostra]

    antes = read_delta(spark, demo_path).where(F.col("transaction_id").isin(ids)) \
        .agg(F.round(F.sum("amount"), 2)).collect()[0][0]

    correcoes = (
        read_delta(spark, demo_path).where(F.col("transaction_id").isin(ids))
        .select("transaction_id", "event_date", "customer_id", "merchant_id",
                "channel", "status")
        # A origem corrigiu: os valores estavam 10% menores do que o real.
        .withColumn("amount", F.lit(999.99))
        .withColumn("review_channel", F.lit("correcao_origem"))
    )

    (target.alias("destino")
     .merge(correcoes.alias("origem"), "destino.transaction_id = origem.transaction_id")
     .whenMatchedUpdate(set={"amount": "origem.amount",
                             "review_channel": "origem.review_channel"})
     .whenNotMatchedInsertAll()
     .execute())

    depois = read_delta(spark, demo_path).where(F.col("transaction_id").isin(ids)) \
        .agg(F.round(F.sum("amount"), 2)).collect()[0][0]
    print(f"Soma dos 50 registros antes do MERGE:  R$ {antes}")
    print(f"Soma dos 50 registros depois do MERGE: R$ {depois}")
    print(f"Total de linhas (nao mudou, foi UPDATE): {read_delta(spark, demo_path).count()}")

    # ================================================================ historico
    _banner("HISTORICO - auditoria completa da tabela")
    print("Cada commit registra a operacao, as metricas e o `userMetadata` que o")
    print("pipeline carimbou (com o run_id). E a trilha de auditoria que uma")
    print("area de compliance pede - e que um data lake em Parquet puro nao tem.\n")
    describe_history(spark, demo_path, limit=10).show(truncate=60, vertical=False)

    # ============================================================== time travel
    _banner("TIME TRAVEL - lendo o passado")
    versions = [row["version"] for row in
                describe_history(spark, demo_path, limit=20).collect()]
    for version in sorted(versions):
        snapshot = read_delta(spark, demo_path, version=version)
        columns = len(snapshot.columns)
        print(f"   versao {version}: {snapshot.count():>6} linhas | {columns} colunas")

    print("\nO mesmo registro, em duas versoes diferentes:")
    primeiro_id = ids[0]
    for version in (0, max(versions)):
        try:
            valor = (read_delta(spark, demo_path, version=version)
                     .where(F.col("transaction_id") == primeiro_id)
                     .select("amount").collect())
            print(f"   versao {version}: amount = {valor[0]['amount'] if valor else 'ausente'}")
        except Exception as exc:
            print(f"   versao {version}: indisponivel ({exc})")

    print("\nTime travel por timestamp tambem funciona:")
    print("   spark.read.format('delta').option('timestampAsOf', '2024-07-01').load(path)")

    # =================================================================== restore
    _banner("RESTORE - desfazendo uma escrita ruim")
    print("O MERGE colocou R$ 999,99 em 50 registros. Suponha que foi um bug.")
    print("Em vez de reprocessar tudo, restauramos a versao anterior.\n")

    versao_antes_do_merge = max(versions) - 1
    spark.sql(f"RESTORE TABLE delta.`{demo_path}` TO VERSION AS OF {versao_antes_do_merge}")

    restaurado = read_delta(spark, demo_path).where(F.col("transaction_id").isin(ids)) \
        .agg(F.round(F.sum("amount"), 2)).collect()[0][0]
    print(f"Soma apos o RESTORE: R$ {restaurado} (era R$ {antes} antes do MERGE)")
    print("O RESTORE tambem e um commit: nada foi perdido, so voltamos no tempo.")

    # =============================================================== constraints
    _banner("CONSTRAINTS - qualidade garantida pelo storage")
    print("Uma CHECK constraint impede que dado invalido entre na tabela, mesmo que")
    print("o job tenha bug. E a ultima linha de defesa da qualidade.\n")

    spark.sql(f"ALTER TABLE delta.`{demo_path}` "
              "ADD CONSTRAINT valor_positivo CHECK (amount > 0)")
    print("Constraint `valor_positivo` criada. Tentando inserir um valor negativo...")

    invalido = read_delta(spark, demo_path).limit(1) \
        .withColumn("amount", F.lit(-50.0)) \
        .withColumn("transaction_id", F.lit("TXN-INVALIDA"))
    print("   (o Spark vai imprimir um stack trace abaixo - e o esperado: a")
    print("    violacao acontece dentro da task, no momento da gravacao)\n")
    try:
        invalido.write.format("delta").mode("append").save(demo_path)
        print("   [!] A escrita passou - constraint nao foi aplicada.")
    except Exception as exc:
        # A causa util fica no meio do stack trace do Java; extraimos so ela.
        detalhe = next(
            (linha.strip() for linha in str(exc).splitlines()
             if "CHECK constraint" in linha or "InvariantViolation" in linha),
            str(exc).strip().splitlines()[0],
        )
        print(f"\n   [OK] Escrita REJEITADA pelo Delta: {detalhe[:160]}")

    _banner("FIM DA DEMONSTRACAO")
    print(f"Tabela de demonstracao: {demo_path}")
    print("Nenhuma tabela do pipeline foi alterada.\n")


if __name__ == "__main__":
    run()
