"""
Qualidade de dados avancada do lakehouse.

O framework de expectativas basico vive em `src/common/data_quality.py` (nulo,
unico, dominio, faixa, volume, frescor). Este pacote acrescenta as tecnicas que
pegam os problemas que uma expectativa por coluna nao ve:

* `schema_contract`  - mudanca de schema na origem (quebra vs. aditiva);
* `reconciliation`   - perda silenciosa de dados e de valor entre camadas;
* `drift`            - mudanca de distribuicao (PSI) e de volume ao longo do tempo;
* `constraints`      - regras aplicadas pelo proprio storage (CHECK do Delta);
* `expectations`     - tipos adicionais de expectativa (chave composta, FK, SQL);
* `scorecard`        - consolidacao de tudo em uma nota por tabela e por camada.
"""
