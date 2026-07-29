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
from typing import Any

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

    def __init__(self, data: dict[str, Any], root: Path) -> None:
        self.raw = data
        self.project_root = root
        # Preenchido por `get_settings()`; util para logar em qual perfil o job rodou.
        self.profile = "default"

    # -- blocos da configuracao ---------------------------------------------
    @property
    def project(self) -> dict[str, Any]:
        return self.raw["project"]

    @property
    def spark(self) -> dict[str, Any]:
        return self.raw["spark"]

    @property
    def data_generation(self) -> dict[str, Any]:
        return self.raw["data_generation"]

    @property
    def business_rules(self) -> dict[str, Any]:
        return self.raw["business_rules"]

    @property
    def ml(self) -> dict[str, Any]:
        return self.raw["ml"]

    @property
    def ai(self) -> dict[str, Any]:
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
                relative = relative[len(base_root) :].lstrip("/")
            root = Path(override_root)
            candidate = root / relative if relative else root
        else:
            candidate = Path(configured)

        # SEMPRE absoluto. A sintaxe SQL do Delta (`delta.`<caminho>``) nao
        # resolve caminho relativo contra o diretorio de trabalho - ela procura
        # no catalogo e falha com TABLE_OR_VIEW_NOT_FOUND. O DataFrameReader
        # aceitaria relativo, o que torna o bug intermitente e confuso: funciona
        # na leitura e quebra no ALTER TABLE.
        if candidate.is_absolute():
            return candidate
        # Um LAKEHOUSE_ROOT relativo (ex.: "data-ci") e resolvido contra a raiz
        # do projeto, e nao contra o CWD, para o comportamento nao depender de
        # onde o comando foi disparado.
        return (self.project_root / candidate).resolve()

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


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Mescla `overlay` sobre `base` recursivamente (o overlay vence)."""
    result = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Le e memoriza a configuracao (o YAML e lido uma unica vez por processo).

    Perfis
    ------
    A variavel de ambiente `LAKEHOUSE_PROFILE` seleciona um overlay que e
    mesclado sobre `config/settings.yaml`:

        LAKEHOUSE_PROFILE=ci  ->  config/settings.yaml + config/settings.ci.yaml

    O overlay so precisa declarar o que muda - o resto vem da configuracao base.
    E assim que o CI roda o pipeline inteiro com um dataset reduzido sem manter
    uma segunda copia da configuracao (que inevitavelmente divergiria).
    """
    root = find_project_root()
    with open(root / _MARKER, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)

    profile = os.getenv("LAKEHOUSE_PROFILE", "").strip()
    if profile:
        overlay_path = root / "config" / f"settings.{profile}.yaml"
        if not overlay_path.exists():
            raise FileNotFoundError(
                f"Perfil `{profile}` selecionado, mas {overlay_path} nao existe."
            )
        with open(overlay_path, encoding="utf-8") as handle:
            data = _deep_merge(data, yaml.safe_load(handle) or {})

    settings = Settings(data, root)
    settings.profile = profile or "default"
    return settings
