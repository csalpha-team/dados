# silver / al_ibge_ppm

Pesquisa da Pecuária Municipal (IBGE, agregado 3939), municípios da Amazônia Legal.

| tabela | PK | origem raw |
|---|---|---|
| `efetivo_rebanhos` | `id_municipio`, `ano`, `produto` | `raw.al_ibge_ppm.efetivo_rebanhos` |

- Única variável da fonte: 105 — *Efetivo dos rebanhos*, em **cabeças**. Não há
  variável monetária, portanto não se aplica `currency_fix`.
- O raw usa `PERIODOS="all"`: a cobertura temporal acompanha o que o IBGE publica
  (1974 até o último ano divulgado) sem alteração de código.
- Sentinelas não numéricas do IBGE (`-`, `..`, `...`, `X`) são tratadas por
  `fix_ibge_digits` agrupando por `id_municipio`/`ano`/`produto`.
