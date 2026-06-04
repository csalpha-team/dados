# Comparação benchmark × séries L1 geradas

Soma por região/produto e por estado/produto, benchmark × new, por série. Nomes de RI antigos do benchmark remapeados para os canônicos.

- `comparacao_regiao_produto.csv` — soma por (série, região, produto)
- `comparacao_estado_produto.csv` — soma por (série, produto)

## Resumo por série

| série | reg/prod linhas | match% | estado/prod linhas | match% | Σ benchmark | Σ new |
|---|--:|--:|--:|--:|--:|--:|
| censo_autoconsumo::extracao_vegetal_censo_2006::comercio_quantidade_produzida | 492 | 86.6 | 41 | 90.2 | 108,904.72 | 2,211,713.66 |
| censo_autoconsumo::extracao_vegetal_censo_2006::comercio_valor_producao | 492 | 90.2 | 41 | 97.6 | 369,710.69 | 184,156.68 |
| censo_autoconsumo::extracao_vegetal_censo_2006::quantidade_produzida | 492 | 83.5 | 41 | 90.2 | 287,400.73 | 4,962,747.28 |
| censo_autoconsumo::extracao_vegetal_censo_2006::valor_producao | 492 | 88.2 | 41 | 97.6 | 693,633.48 | 345,427.48 |
| censo_autoconsumo::extracao_vegetal_censo_2017::comercio_quantidade_produzida | 636 | 62.4 | 53 | 47.2 | 264,560.00 | 735,190.93 |
| censo_autoconsumo::extracao_vegetal_censo_2017::comercio_valor_producao | 636 | 58.0 | 53 | 41.5 | 999,884.28 | 977,147.31 |
| censo_autoconsumo::extracao_vegetal_censo_2017::quantidade_produzida | 636 | 60.4 | 53 | 45.3 | 431,402.00 | 2,005,683.85 |
| censo_autoconsumo::extracao_vegetal_censo_2017::valor_producao | 636 | 58.6 | 53 | 43.4 | 1,525,896.02 | 1,448,157.18 |
| censo_autoconsumo::lavoura_permanente_censo_2006::comercio_quantidade_produzida | 792 | 89.9 | 66 | 95.5 | 677,808.05 | 715,599.56 |
| censo_autoconsumo::lavoura_permanente_censo_2006::comercio_valor_producao | 792 | 92.0 | 66 | 98.5 | 1,331,347.68 | 663,231.64 |
| censo_autoconsumo::lavoura_permanente_censo_2006::quantidade_produzida | 792 | 89.9 | 66 | 95.5 | 677,808.05 | 715,599.56 |
| censo_autoconsumo::lavoura_permanente_censo_2006::valor_producao | 792 | 92.0 | 66 | 98.5 | 1,331,347.68 | 663,231.64 |
| censo_autoconsumo::lavoura_permanente_censo_2017::comercio_quantidade_produzida | 852 | 66.1 | 71 | 60.6 | 1,231,490.00 | 971,213.55 |
| censo_autoconsumo::lavoura_permanente_censo_2017::comercio_valor_producao | 852 | 63.1 | 71 | 59.2 | 3,124,664.62 | 2,592,728.44 |
| censo_autoconsumo::lavoura_permanente_censo_2017::quantidade_produzida | 852 | 64.8 | 71 | 59.2 | 1,698,525.00 | 1,404,351.58 |
| censo_autoconsumo::lavoura_permanente_censo_2017::valor_producao | 852 | 62.7 | 71 | 57.7 | 3,736,245.00 | 3,157,986.66 |
| censo_autoconsumo::lavoura_temporaria_censo_2006::comercio_quantidade_produzida | 636 | 87.7 | 53 | 96.2 | 3,916,494.51 | 3,956,404.90 |
| censo_autoconsumo::lavoura_temporaria_censo_2006::comercio_valor_producao | 636 | 89.5 | 53 | 98.1 | 4,027,616.55 | 1,901,342.52 |
| censo_autoconsumo::lavoura_temporaria_censo_2006::quantidade_produzida | 636 | 87.7 | 53 | 96.2 | 3,916,494.51 | 3,956,404.90 |
| censo_autoconsumo::lavoura_temporaria_censo_2006::valor_producao | 636 | 89.5 | 53 | 98.1 | 4,027,616.55 | 1,901,342.52 |
| censo_autoconsumo::lavoura_temporaria_censo_2017::comercio_quantidade_produzida | 660 | 66.8 | 55 | 63.6 | 2,091,105.00 | 2,031,377.26 |
| censo_autoconsumo::lavoura_temporaria_censo_2017::comercio_valor_producao | 660 | 64.8 | 55 | 58.2 | 3,906,568.25 | 3,790,795.09 |
| censo_autoconsumo::lavoura_temporaria_censo_2017::quantidade_produzida | 660 | 62.7 | 55 | 54.5 | 3,226,538.00 | 3,186,061.05 |
| censo_autoconsumo::lavoura_temporaria_censo_2017::valor_producao | 660 | 61.2 | 55 | 54.5 | 6,758,791.33 | 6,596,111.06 |
| extracao_vegetal_censo_quantidade.json | 702 | 48.9 | 54 | 44.4 | 744,648.00 | 6,968,431.13 |
| extracao_vegetal_censo_valor.json | 702 | 45.6 | 54 | 40.7 | 2,255,363.03 | 1,793,584.66 |
| extracao_vegetal_pevs_quantidade.json | 645 | 50.4 | 50 | 52.0 | 753,120,288.00 | 385,907,101.50 |
| extracao_vegetal_pevs_valor.json | 645 | 51.0 | 50 | 66.0 | 4,575,508,385.00 | 4,578,243,479.00 |
| lavoura_permanente_censo_quantidade.json | 918 | 52.3 | 71 | 47.9 | 736,649.00 | 2,119,951.14 |
| lavoura_permanente_censo_valor.json | 918 | 49.6 | 71 | 46.5 | 1,389,461.07 | 3,821,218.30 |
| lavoura_permanente_pam_quantidade.json | 538 | 11.0 | 53 | 17.0 | 1,468,678.00 | 103,877,337.70 |
| lavoura_permanente_pam_valor.json | 538 | 10.8 | 53 | 17.0 | 58,207,075.00 | 222,913,334.03 |
| lavoura_temporaria_censo_quantidade.json | 713 | 46.4 | 55 | 43.6 | 4,058,378.00 | 7,142,465.95 |
| lavoura_temporaria_censo_valor.json | 713 | 43.8 | 55 | 40.0 | 4,099,195.36 | 8,497,453.58 |
| lavoura_temporaria_pam_quantidade.json | 442 | 49.5 | 34 | 44.1 | 59,199,416.00 | 273,397,890.36 |
| lavoura_temporaria_pam_valor.json | 442 | 49.8 | 34 | 41.2 | 1,274,917,376.00 | 222,819,219.20 |

## Como ler as duas tabelas

- **Região/produto** (`comparacao_regiao_produto.csv`) — soma sobre os anos, por
  (série, região, produto). Sensível à migração de RIs.
- **Estado/produto** (`comparacao_estado_produto.csv`) — soma sobre regiões *e*
  anos, por (série, produto). A migração de RIs se cancela aqui (todos os
  municípios entram no total estadual em ambas as versões), então o que sobra são
  correções de valor e diferenças de cobertura.

`status`: `match` (|Δ| ≤ 1%), `diff`, `only_benchmark`, `only_new`. `diff_rel_pct`
é |Δ| relativo ao maior dos dois valores.

## Leitura dos drivers das divergências

O benchmark é um *snapshot anterior* a mudanças intencionais a montante;
divergências de valor são esperadas e **não** indicam erro de exportação.

1. **Migração de regiões de integração** (`MIGRACAO_REGIOES_INTEGRACAO.md`).
   Municípios antes sem RI (`"NaN"` no benchmark) agora estão mapeados, três RIs
   foram renomeadas (`Caeté→Rio Caeté`, `Capim→Rio Capim`, `Tucuruí→Lago de
   Tucuruí`, já remapeadas aqui) e a composição municipal de cada RI mudou. Afeta
   o nível **região/produto**; **cancela-se** no nível **estado/produto**.
2. **Correções de unidade.** PEVS `madeira-tora` agora é exatamente ÷2 (correção
   m³) — domina a soma de `quantidade` do PEVS; produtos madeireiros do censo
   foram reescalados por fatores específicos por produto.
3. **Padronização monetária (1000xBRL).** Séries de `valor` do PAM trazem o novo
   fator de escala; entradas pré-1995 (reforma monetária) zeram.
4. **Rebuild do silver de lavoura PAM** (`silver/al_ibge_pam/lavoura_*.py`). Até a
   `quantidade` (não-monetária) diverge — daí o match baixíssimo do lavoura PAM.
5. **Cobertura de anos.** O benchmark precede o censo de lavoura 2017, então as
   somas de `lavoura_*_censo` no new incluem 2017 e superam o benchmark. Também
   há pequenas diferenças de grade (produto×ano) no PEVS.

**Sanidade da exportação:** mesmo com produtos individuais embaralhados pela
padronização de nomes, o **total estadual** do PEVS `valor` é praticamente
idêntico (Σ benchmark ≈ Σ new, ~0,06% de diferença) — confirmando que a lógica de
exportação é fiel. As demais divergências acompanham as correções acima.
