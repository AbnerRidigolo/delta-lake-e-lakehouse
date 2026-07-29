"""
Configuracao compartilhada dos testes.

O que este arquivo resolve
--------------------------
O CI separa "testes rapidos" (segundos, sem Spark) de "testes de integracao"
(precisam de uma SparkSession e das tabelas materializadas). Fazer essa
separacao com uma expressao `-k "nome or outro_nome or ..."` funciona ate
alguem adicionar o proximo teste e esquecer de atualizar a lista - e ai o teste
some do CI em silencio, que e o pior tipo de falha de teste.

A marcacao aqui e automatica: qualquer teste que peca a fixture `spark` recebe
o marcador `spark`. O CI usa `-m "not spark"` e a separacao se mantem sozinha.
"""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(config, items):  # noqa: ARG001
    """Marca automaticamente os testes que dependem de Spark."""
    for item in items:
        if "spark" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.spark)
