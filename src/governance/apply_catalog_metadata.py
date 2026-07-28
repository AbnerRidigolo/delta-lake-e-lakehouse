"""
Governanca como codigo: aplica os metadados de `config/catalog.yaml` nas tabelas Delta.

O problema que isso resolve
--------------------------
Catalogo preenchido a mao envelhece em semanas. Aqui os metadados vivem no
repositorio, passam por code review e sao aplicados pelo pipeline - a mesma
disciplina que se usa para infraestrutura (IaC), aplicada a governanca.

O que o script faz
------------------
1. Le `config/catalog.yaml` (dominio, owner, classificacao, SLA, colunas PII).
2. Aplica `TBLPROPERTIES` em cada tabela Delta correspondente: os metadados
   passam a viajar junto com a tabela, visiveis em `DESCRIBE DETAIL`.
3. Gera `docs/generated/data_catalog.md` - a documentacao que qualquer pessoa
   do time consulta.
4. Gera `docs/generated/unity_catalog_setup.sql` - o script equivalente para um
   ambiente Databricks com Unity Catalog (COMMENT, TAGS e GRANTs), mostrando
   como a mesma definicao se traduz para o catalogo gerenciado.

Execucao:
    python -m src.governance.apply_catalog_metadata
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import yaml

from src.common.config import get_settings
from src.common.delta_io import table_exists
from src.common.logging_utils import get_logger, log_stage
from src.common.spark_session import get_spark

log = get_logger(__name__)

# Perfis de acesso do lakehouse. Em Unity Catalog viram GRANTs de verdade.
ACCESS_PROFILES = {
    "PUBLIC": ["analistas", "engenharia_dados", "ciencia_dados", "negocio"],
    "INTERNAL": ["analistas", "engenharia_dados", "ciencia_dados"],
    "CONFIDENTIAL": ["engenharia_dados", "ciencia_dados", "risco"],
    "PII": ["engenharia_dados", "dpo"],
}


def load_catalog() -> Dict[str, Any]:
    """Le o catalogo declarativo do repositorio."""
    root = get_settings().project_root
    with open(root / "config" / "catalog.yaml", "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def apply_table_properties(spark, catalog: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Grava os metadados como TBLPROPERTIES nas tabelas Delta existentes."""
    cfg = get_settings()
    domains = catalog.get("domains", {})
    applied: List[Dict[str, Any]] = []

    for entry in catalog["tables"]:
        layer, table = entry["name"].split(".", 1)
        path = cfg.table_path(layer, table)

        if not table_exists(spark, path):
            log.warning("Tabela ainda nao materializada, pulando: %s", entry["name"])
            continue

        domain = domains.get(entry["domain"], {})
        properties = {
            "governanca.dominio": entry["domain"],
            "governanca.owner": domain.get("owner", "nao_definido"),
            "governanca.steward": domain.get("steward", "nao_definido"),
            "governanca.classificacao": entry["classification"],
            "governanca.sla_horas": str(entry.get("sla_hours", "")),
            "governanca.colunas_pii": ",".join(entry.get("pii_columns", []) or []),
            "governanca.acesso": ",".join(ACCESS_PROFILES.get(entry["classification"], [])),
            "governanca.camada": entry["layer"],
            "comment": entry["description"],
        }

        assignments = ", ".join(
            f"'{key}' = '{value}'" for key, value in properties.items()
        ).replace("\\", "")
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ({assignments})")

        applied.append({**entry, **{"path": path, "owner": properties["governanca.owner"],
                                    "steward": properties["governanca.steward"],
                                    "acesso": properties["governanca.acesso"]}})
        log.info("Metadados aplicados: %s (%s / %s)", entry["name"], entry["domain"],
                 entry["classification"])

    return applied


def render_catalog_markdown(catalog: Dict[str, Any], applied: List[Dict[str, Any]]) -> str:
    """Gera a pagina de documentacao do catalogo de dados."""
    cfg = get_settings()
    lines: List[str] = [
        "# Catalogo de dados - Lakehouse NeoPag",
        "",
        "*Documento gerado automaticamente por "
        "`src/governance/apply_catalog_metadata.py` a partir de `config/catalog.yaml`. "
        "Nao edite manualmente.*",
        "",
        "## Classificacao de dados",
        "",
        "| Classificacao | Significado | Quem acessa |",
        "|---|---|---|",
    ]
    for name, description in catalog["classifications"].items():
        lines.append(f"| `{name}` | {description} | {', '.join(ACCESS_PROFILES.get(name, []))} |")

    lines += ["", "## Dominios e responsaveis", "",
              "| Dominio | Owner (produto de dados) | Steward (governanca) |", "|---|---|---|"]
    for domain, meta in catalog["domains"].items():
        lines.append(f"| `{domain}` | {meta['owner']} | {meta['steward']} |")

    for layer in ["bronze", "silver", "gold", "feature_store"]:
        tables = [t for t in catalog["tables"] if t["layer"] == layer]
        if not tables:
            continue
        lines += ["", f"## Camada {layer.upper()}", "",
                  "| Tabela | Nome no Unity Catalog | Dominio | Classificacao | SLA | Descricao |",
                  "|---|---|---|---|---|---|"]
        for entry in tables:
            _, table_name = entry["name"].split(".", 1)
            uc_name = cfg.uc_table_name(entry["layer"], table_name)
            lines.append(
                f"| `{entry['name']}` | `{uc_name}` | {entry['domain']} | "
                f"`{entry['classification']}` | {entry.get('sla_hours', '-')}h | "
                f"{entry['description']} |"
            )

    pii_tables = [t for t in catalog["tables"] if t.get("pii_columns")]
    lines += ["", "## Dados pessoais (LGPD)", "",
              "Colunas classificadas como dado pessoal e o tratamento aplicado:", "",
              "| Tabela | Colunas | Tratamento |", "|---|---|---|"]
    for entry in pii_tables:
        treatment = ("mascaramento + hash com salt" if entry["layer"] != "bronze"
                     else "acesso restrito (dado bruto preservado para auditoria)")
        lines.append(f"| `{entry['name']}` | {', '.join(entry['pii_columns'])} | {treatment} |")

    lines += ["", f"*Tabelas materializadas e anotadas nesta execucao: {len(applied)}.*", ""]
    return "\n".join(lines)


def render_unity_catalog_sql(catalog: Dict[str, Any]) -> str:
    """Gera o equivalente do catalogo para Databricks Unity Catalog.

    Mostra como a mesma definicao declarativa se traduz em objetos gerenciados:
    catalogo, schemas, comentarios, tags de classificacao e GRANTs.
    """
    cfg = get_settings()
    catalog_name = cfg.catalog
    lines = [
        "-- =====================================================================",
        "-- Setup do Unity Catalog equivalente ao catalogo declarativo do projeto.",
        "-- Gerado por src/governance/apply_catalog_metadata.py",
        "-- =====================================================================",
        "",
        f"CREATE CATALOG IF NOT EXISTS {catalog_name}",
        f"  MANAGED LOCATION 's3://neopag-lakehouse/{catalog_name}';",
        "",
    ]
    for layer in ["bronze", "silver", "gold", "feature_store"]:
        lines.append(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{layer};")
    lines.append("")

    for entry in catalog["tables"]:
        layer, table = entry["name"].split(".", 1)
        full = f"{catalog_name}.{layer}.{table}"
        description = entry["description"].replace("'", "''")
        lines += [
            f"-- {full}",
            f"COMMENT ON TABLE {full} IS '{description}';",
            f"ALTER TABLE {full} SET TAGS ("
            f"'classificacao' = '{entry['classification']}', "
            f"'dominio' = '{entry['domain']}', "
            f"'sla_horas' = '{entry.get('sla_hours', '')}');",
        ]
        for group in ACCESS_PROFILES.get(entry["classification"], []):
            lines.append(f"GRANT SELECT ON TABLE {full} TO `{group}`;")
        for column in entry.get("pii_columns", []) or []:
            lines.append(
                f"ALTER TABLE {full} ALTER COLUMN {column} "
                f"SET TAGS ('pii' = 'true', 'lgpd' = 'dado_pessoal');")
        lines.append("")

    lines += [
        "-- Mascaramento dinamico de PII: quem nao esta no grupo ve o dado mascarado.",
        "CREATE OR REPLACE FUNCTION mask_pii(valor STRING)",
        "  RETURN CASE WHEN is_account_group_member('dpo') THEN valor ELSE '***' END;",
        "",
        f"ALTER TABLE {catalog_name}.silver.customers "
        "ALTER COLUMN full_name SET MASK mask_pii;",
        "",
    ]
    return "\n".join(lines)


def run() -> Dict[str, Any]:
    cfg = get_settings()
    spark = get_spark("governance_catalog")

    with log_stage(log, "governance.apply_catalog_metadata"):
        catalog = load_catalog()
        applied = apply_table_properties(spark, catalog)

        output_dir = cfg.project_root / "docs" / "generated"
        output_dir.mkdir(parents=True, exist_ok=True)

        (output_dir / "data_catalog.md").write_text(
            render_catalog_markdown(catalog, applied), encoding="utf-8")
        (output_dir / "unity_catalog_setup.sql").write_text(
            render_unity_catalog_sql(catalog), encoding="utf-8")
        (output_dir / "catalog_applied.json").write_text(
            json.dumps(applied, indent=2, ensure_ascii=False), encoding="utf-8")

        log.info("Catalogo documentado em %s", output_dir)
        return {"tabelas_anotadas": len(applied),
                "tabelas_declaradas": len(catalog["tables"])}


if __name__ == "__main__":
    run()
