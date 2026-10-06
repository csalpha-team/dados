# IPCA anual — camada silver

Converte a variação mensal do IPCA (SGS `433`) em dois indicadores anuais:

- `indice_ipca`: média anual do nível mensal encadeado, apropriada para
  converter fluxos monetários anuais para preços constantes;
- `variacao_acumulada_ano_pct`: composição das doze variações mensais.

`ano_completo` só é verdadeiro quando existem exatamente doze meses. A camada
gold de renda rejeita anos incompletos e usa a razão entre índices anuais; por
isso, o nível inicial arbitrário do encadeamento cancela no fator de correção.
