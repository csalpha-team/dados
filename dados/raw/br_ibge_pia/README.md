# raw / br_ibge_pia — Pesquisa Industrial Anual – Empresa (IBGE)

## Ruptura de série em 2024

O IBGE rompeu a série da PIA-Empresa no ano de referência **2024** (Nota Técnica
PIA-Empresa 01/2026). As séries **não são diretamente comparáveis**.

| tabela | agregado | cobertura | recorte |
|---|---|---|---|
| `tbl_1849` | 1849 | 2007–2023 (série antiga, encerrada) | UF × divisões CNAE (32 categorias) |
| `tbl_10457` | 10457 | 2024– (série nova) | UF × divisões CNAE (32 categorias) |
| `tbl_1848` | 1848 | 2007–2023 (série antiga, encerrada) | UF × divisões e grupos (143 categorias) |
| `tbl_10454` | 10454 | 2024– (série nova) | UF × divisões e grupos (143 categorias) |
| `tbl_1987`, `tbl_1988` | 1987, 1988 | 1996–2007 | séries históricas encerradas |

Mudanças de variável na série nova: `706` ("Número de unidades locais") vira
`13816` ("Número de unidades locais de empresas com 5 ou mais pessoas
ocupadas") e entra `2086` ("Número de empresas com 5 ou mais pessoas
ocupadas"). As demais variáveis mantêm os mesmos ids.

Downstream: só as tabelas por divisão têm silver (`tbl_1849`, `tbl_10457`);
no gold, `pia_industrias` e `pia_industrias_serie_nova`.
