"""
Carregamento da configuracao do projeto (`config/settings.yaml`).

Por que isso existe
-------------------
Jobs de dados envelhecem mal quando caminhos e parametros de negocio ficam
espalhados em `hardcode` dentro dos scripts. Aqui centralizamos tudo em um
unico YAML e expomos um objeto `Settings` com helpers para resolver caminhos
das tabelas Delta.

Uso tipico:

    from src.common.config import get_settings

    cfg = get_settings()
    path = cfg.table_path("silver", "transactions")   # data/silver/transactions
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml

# Nome do arquivo usado para descobrir a raiz do projeto subindo diretorios.
_MARKER = Path("config") / "settings.yaml"


def find_project_root(start: Path | None = None) -> Path:
    """Sobe na arvore de diretorios ate encontrar `config/settings.yaml`.

    Isso permite executar os scripts de qualquer lugar (`python src/...`,
    notebook, Airflow) sem depender do diretorio de trabalho atual.
    """
    current = (start or Path(__file__)).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / _MARKER).exists():
            return candidate
    raise FileNotFoundError(
        "Nao encontrei config/settings.yaml subindo a partir de "
        f"{current}. Rode a partir da raiz do projeto."
    )


class Settings:
    """Wrapper leve sobre o dicionario de configuracao.

    Mantemos o acesso "cru" via `cfg.raw` para nao precisar mapear cada chave
    nova em um atributo, mas expomos helpers para o que e usado o tempo todo:
    caminhos de camada e caminhos de tabela.
    """

    def __init__(self, data: Dict[str, Any], root: Path) -> None:
        self.raw = data
        self.project_root = root

    # -- blocos da configuracao ---------------------------------------------
    @property
    def project(self) -> Dict[str, Any]:
        return self.raw["project"]

    @property
    def spark(self) -> Dict[str, Any]:
        return self.raw["spark"]

    @property
    def data_generation(self) -> Dict[str, Any]:
        return self.raw["data_generation"]

    @property
    def business_rules(self) -> Dict[str, Any]:
        return self.raw["business_rules"]

    @property
    def ml(self) -> Dict[str, Any]:
        return self.raw["ml"]

    @property
    def ai(self) -> Dict[str, Any]:
        return self.raw["ai"]

    @property
    def catalog(self) -> str:
        return self.project["catalog"]

    # -- caminhos ------------------------------------------------------------
    def layer_path(self, layer: str) -> Path:
        """Diretorio raiz de uma camada (`raw`, `bronze`, `silver`, `gold`...).

        Se a variavel de ambiente LAKEHOUSE_ROOT estiver definida, ela
        substitui a raiz configurada no YAML. E o gancho para apontar o mesmo
        codigo para `s3://meu-bucket/lakehouse` ou para o DBFS do Databricks.
        """
        configured = self.raw["paths"][layer]
        override_root = os.getenv("LAKEHOUSE_ROOT")

        if override_root:
            # Troca o prefixo configurado (ex.: "data/") pela raiz externa.
            base_root = self.raw["paths"]["root"]
            relative = str(configured)
            if relative.startswith(base_root):
                relative = relative[len(base_root):].lstrip("/")
            return Path(override_root) / relative if relative else Path(override_root)

        candidate = Path(configured)
        return candidate if candidate.is_absolute() else self.project_root / candidate

    def table_path(self, layer: str, table: str) -> str:
        """Caminho fisico de uma tabela Delta dentro de uma camada."""
        return str(self.layer_path(layer) / table)

    def checkpoint_path(self, name: str) -> str:
        """Caminho de checkpoint para jobs de Structured Streaming."""
        return str(self.layer_path("checkpoints") / name)

    def full_table_name(self, layer: str, table: str) -> str:
        """Nome qualificado da tabela, adaptado ao metastore em uso.

        * **Databricks / Unity Catalog**: namespace de tres niveis
          (`fintech.gold.customer_360`).
        * **Local (metastore embutido do Spark)**: apenas dois niveis
          (`gold.customer_360`), porque o Hive metastore nao suporta catalogo
          nomeado. E a mesma tabela Delta - muda so como ela e referenciada.
        """
        if self.project.get("environment") == "databricks":
            return f"{self.catalog}.{layer}.{table}"
        return f"{layer}.{table}"

    def uc_table_name(self, layer: str, table: str) -> str:
        """Nome no padrao Unity Catalog, usado na documentacao e no catalogo."""
        return f"{self.catalog}.{layer}.{table}"

    def ensure_directories(self) -> None:
        """Cria todos os diretorios de camada declarados no YAML."""
        for layer in self.raw["paths"]:
            self.layer_path(layer).mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Le e memoriza a configuracao (o YAML e lido uma unica vez por processo)."""
    root = find_project_root()
    with open(root / _MARKER, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return Settings(data, root)
