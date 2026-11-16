# raw / al_ibge_pac — Pesquisa Anual de Comércio (IBGE)

## Ruptura de série em 2024

O IBGE rompeu a série da PAC no ano de referência **2024** (Nota Técnica PAC
01/2026): mudou a classificação de empresas inativas e o Cempre passou a usar o
eSocial no lugar da RAIS. Os resultados das duas séries **não são diretamente
comparáveis**.

| tabela | agregado | cobertura | geografia | classificação |
|---|---|---|---|---|
| `tbl_1407` | 1407 | 2007–2023 (série antiga, encerrada) | classificação 12354 | 12354 + 11066 |
| `tbl_10653` | 10653 | 2024– (série nova) | `localidades` N1/N2/N3 | 11066 |

As variáveis são as mesmas nas duas tabelas (368, 312, 314, 503, 866) e a
classificação de comércio 11066 mantém as 23 categorias. A diferença estrutural
é só a origem da geografia.

Downstream: `silver.br_ibge_pac.tbl_1407` / `tbl_10653`; no gold, as séries
ficam em tabelas separadas (`pac_comercio` e `pac_comercio_serie_nova`).
