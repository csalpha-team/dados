# Coeficientes técnicos de emprego rural

`pa_coeficientes_emprego_rural.censo_2017_grupo_atividade` publica os
coeficientes técnicos de emprego rural do Pará, calculados com o Censo
Agropecuário 2017.

## Fontes e cálculo

O fluxo combina as tabelas Gold intermediárias provenientes do Censo:

- tabela 6888: pessoal ocupado e número de estabelecimentos;
- tabela 6897: valor da produção e número de estabelecimentos com produção.

Para cada combinação de `ano`, `id_municipio` e
`grupo_atividade_economica`, o coeficiente é:

`coeficiente_tecnico = valor_producao / pessoal_ocupado`

`valor_producao` e `coeficiente_tecnico` usam a unidade `mil R$`; portanto,
o coeficiente é expresso em `mil R$ por pessoa` a preços correntes de 2017.

O cálculo preserva os grupos do IBGE. Em particular, `Produção de lavouras
temporárias` e `Produção de lavouras permanentes` são registros distintos.

## Regra para valores não calculáveis

O coeficiente é `NULL` quando o valor da produção ou o pessoal ocupado é nulo,
zero ou negativo. Assim, um dado indisponível ou sem base produtiva não se
transforma em um coeficiente igual a zero.

## Uso pela modelagem

Para estimar o emprego no ano `y`, a modelagem deve atualizar o coeficiente
monetário de 2017 pelo fator de inflação acumulado entre 2017 e `y` e calcular:

`pessoal_ocupado_estimado_y = valor_producao_y / coeficiente_tecnico_y`

O VBP e o coeficiente devem estar na mesma unidade monetária antes da divisão.
