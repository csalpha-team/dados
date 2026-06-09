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

## Parâmetros

O arquivo `parametros_coeficientes_renda.json` concentra os pressupostos do
fluxo:

- `config_previsao`: define o método e as restrições da projeção. O método
  atual é `theil_sen`, com pelo menos `2` anos observados por série,
  truncamento de valores negativos quando `clamp_non_negative = true`, janela
  móvel de `3` anos para métodos que usam média móvel e limite de crescimento
  anual positivo de `0.5`.
- `anos_alvo`: intervalo fechado de anos publicados. Com `start = 1995` e
  `end = 2023`, a saída contém todos os anos entre 1995 e 2023.
- `valores_producao_aa`: valores constantes usados para preencher
  `AAProdução` enquanto não houver fonte própria para essa conta.
- `mapa_setores`: mapeia códigos de `divisao_grupo_cnae_2` para as contas
  alfa. O carregador aceita chaves auxiliares começando com `_` como metadados
  e não as usa no cálculo.

Em `mapa_setores.PAC_COMERCIO`, as contas de atacado usam o prefixo `3` e as
contas de varejo usam o prefixo `4`.

Em `mapa_setores.PIA_INDUSTRIA`, as contas de indústria beneficiada usam os
prefixos `10`, `11`, `12`, `17`, `19`, `20` e `24`. As contas de indústria de
transformação usam os prefixos `13`, `14`, `15`, `16`, `18`, `21`, `22`, `23`,
`25`, `26`, `27`, `28`, `29`, `30`, `31`, `32` e `33`.

Os significados documentados dos códigos da PIA ficam em
`mapa_setores.PIA_INDUSTRIA._codigos`:

| Código | Descrição |
| --- | --- |
| `10` | Fabricação de produtos alimentícios |
| `11` | Fabricação de bebidas |
| `12` | Fabricação de produtos de fumo |
| `13` | Fabricação de produtos têxteis |
| `14` | Confecção de artigos do vestuário e acessórios |
| `15` | Preparação de couros e fabricação de artefatos de couro, artigos para viagem e calçados |
| `16` | Fabricação de produtos de madeira |
| `17` | Fabricação de celulose, papel e produtos de papel |
| `19` | Fabricação de coque, de produtos derivados do petróleo e de biocombustíveis |
| `20` | Fabricação de produtos químicos |
| `21` | Fabricação de produtos farmoquímicos e farmacêuticos |
| `22` | Fabricação de produtos de borracha e de material plástico |
| `23` | Fabricação de produtos de minerais não-metálicos |
| `24` | Metalurgia |

### Produtos associados aos CNAEs

A relação abaixo é apenas indicativa. Ela registra possibilidades de associação
entre produtos da sociobiodiversidade e atividades CNAE, com vínculo
condicional fraco: a presença de um produto não implica enquadramento automático
na atividade, e a atividade não é usada para inferir diretamente a origem dos
produtos. O uso deve ser interpretativo e depende de validação caso a caso.

| CNAE | Atividade | Produtos associados |
| ---: | --- | --- |
| `10` | Fabricação de produtos alimentícios | Açaí, cacau, castanha, palmito, cupuaçu, mel, pupunha, bacuri, murici, taperebá, bacaba, uxi, piquiá, leites vegetais etc. |
| `11` | Fabricação de bebidas | Sucos, polpas, néctares, fermentados, bebidas de açaí, cupuaçu, bacuri, taperebá, bacaba etc. |
| `16` | Fabricação de produtos de madeira | Pode se relacionar com cumaru, piquiá, artesanato, madeira manejada e artefatos. |
| `20` | Fabricação de produtos químicos | Óleos, resinas, corantes, extratos, cosméticos, sabões, bioinsumos, breu-branco, andiroba, copaíba, urucum etc. |
| `21` | Farmoquímicos e farmacêuticos | Plantas medicinais, copaíba, andiroba, breu-branco, óleos vegetais, extratos bioativos. |
| `22` | Borracha e material plástico | Borracha natural, látex, artefatos de borracha e possíveis bioplásticos/biomateriais. |

## AAProdução

A conta `AAProdução` continua sendo tratada por pressuposto temporário. Sua
produtividade monetária e seu salário médio são mantidos constantes conforme
`parametros_coeficientes_renda.json`.
