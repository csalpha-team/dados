# raw / br_ibge_pas — Pesquisa Anual de Serviços (IBGE)

## Ruptura de série em 2024

O IBGE rompeu a série da PAS no ano de referência **2024** (Nota Técnica PAS
01/2026), pelos mesmos motivos da PAC: reclassificação de empresas inativas e
troca da base do Cempre (RAIS → eSocial). As séries **não são diretamente
comparáveis**.

| tabela | agregado | cobertura | geografia | salários |
|---|---|---|---|---|
| `tbl_2715` | 2715 | 2007–2023 (série antiga, encerrada) | classificação 12354 | variável 673 |
| `tbl_10762` | 10762 | 2024– (série nova) | `localidades` N1/N2/N3 | variável 857 |

A classificação de atividades 12355 (16 categorias) é a mesma nas duas.

Downstream: `silver.br_ibge_pas.tbl_2715` / `tbl_10762`; no gold,
`pas_servicos` e `pas_servicos_serie_nova` (em `pa_servicos_industria_comercio`
e em `br_servicos`).
