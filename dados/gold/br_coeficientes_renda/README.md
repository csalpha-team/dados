# Coeficientes de renda

Esta camada prepara os coeficientes de renda usados pelo modelo a partir das
bases silver da PIA e da PAC. O fluxo publica três tabelas gold:

- `br_coeficientes_renda.preparacao_camada_renda`;
- `br_coeficientes_renda.renda_produtividade`;
- `br_coeficientes_renda.renda_salario`.

## Contrato

A tabela principal publica:

- `ano`: ano de referência;
- `conta_alfa`: setor operacional da modelagem;
- `tipo_coeff`: `prod_mon_trab` ou `salario_medio`;
- `coeff`: coeficiente calculado para a célula.

As tabelas auxiliares `renda_produtividade` e `renda_salario` seguem o mesmo
contrato usado pelos dumps da Layer 2: `ano`, `conta_alfa`, `coeff`.

## Previsão

Os parâmetros pedem uma série anual de `1995` a `2023`, mas as bases PIA/PAC
usadas aqui começam apenas em `2007`. Os anos não observados são imputados
antes do cálculo dos coeficientes finais.

O método padrão de previsão é `theil_sen`, baseado no estimador de Theil-Sen:

1. calcular todas as inclinações entre pares de pontos observados;
2. usar a mediana dessas inclinações como tendência anual robusta;
3. usar a mediana dos interceptos `valor - slope * ano`;
4. projetar cada variável bruta por `ano` e `divisao_grupo_cnae_2`;
5. calcular `prod_mon_trab` e `salario_medio` a partir das variáveis projetadas.

Essa escolha reduz a influência de outliers em comparação com uma regressão
linear simples ou com CAGR ponta-a-ponta.

## Tolerância de crescimento

Depois que `prod_mon_trab` e `salario_medio` são calculados, a série final passa
por uma tolerância de crescimento anual. O parâmetro
`max_annual_growth_rate = 0.5` limita aumentos a no máximo 50% ao ano dentro de
cada par `conta_alfa` + `tipo_coeff`.

Exemplo: se uma série tem `coeff = 50` em um ano, o ano seguinte pode chegar no
máximo a `75`. Se a projeção calculada produzir `100`, o valor publicado será
limitado a `75`.

O clamp é aplicado sobre crescimento positivo. Quedas não são limitadas por
essa regra, e a regra só opera quando o valor anterior é positivo.

## Fluxo

O fluxo implementado em `utils.py` e `preparacao_camada_renda.py` segue esta
ordem:

1. carregar PIA e PAC da silver;
2. limpar e padronizar colunas numéricas;
3. agregar as bases por `ano` + `divisao_grupo_cnae_2`;
4. projetar variáveis brutas com `IncomeForecaster(method="theil_sen")`;
5. agregar os dados projetados por `conta_alfa`;
6. calcular `prod_mon_trab` e `salario_medio`;
7. adicionar `AAProdução` como série constante por pressuposto temporário;
8. aplicar a tolerância de crescimento anual;
9. validar os models Pydantic;
10. publicar as tabelas na zona gold.

## AAProdução

A conta `AAProdução` continua sendo tratada por pressuposto temporário. Sua
produtividade monetária e seu salário médio são mantidos constantes conforme
`parametros_coeficientes_renda.json`.
