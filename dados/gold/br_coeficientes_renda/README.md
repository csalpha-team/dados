# Coeficientes de renda

Esta camada prepara os coeficientes de renda usados pelo modelo a partir das
bases da PIA e da PAC. O resultado final e a tabela
`br_coeficientes_renda.preparacao_camada_renda`, com duas medidas por
`conta_alfa` e por ano:

- `prod_mon_trab`;
- `salario_medio`.

## Problema identificado

Os parametros da camada pedem uma serie anual de `1995` a `2023`, mas os dados
efetivamente disponiveis nas bases usadas por este modulo comecam apenas em
`2007`. Na pratica, isso significa que qualquer valor entre `1995` e `2006`
nao e observado: ele precisa ser imputado.

Na implementacao anterior, a camada aplicava previsao linear diretamente sobre
as variaveis brutas de PIA e PAC para cobrir todo o intervalo pedido. Esse
desenho produzia um efeito indesejado:

1. a regressao linear extrapolava algumas variaveis para valores negativos nos
   anos anteriores a `2007`;
2. como a previsao era truncada em zero, os numeradores de algumas razoes
   viravam `0.0` enquanto o denominador seguia positivo;
3. o resultado final era uma serie de coeficientes `0.0` em anos antigos para
   alguns setores, o que gerava matrizes com setores aparentando nao pagar
   salarios ou nao ter produtividade monetaria do trabalho.

Esse zero nao representava um fato economico observado. Era um artefato da
extrapolacao.

## Solucao adotada

Para resolver o problema, a camada passou a operar com uma regra mais
conservadora:

1. os coeficientes finais sao calculados primeiro apenas com anos observados nas
   bases;
2. para anos anteriores ao primeiro valor observado de cada serie
   (`conta_alfa` + `tipo_coeff`), aplica-se backcast constante ancorado no
   primeiro ano observado;
3. para anos posteriores ao ultimo valor observado da serie, mantem-se o ultimo
   valor observado, tambem por criterio conservador.

Em outras palavras, a camada nao retroprojeta mais os dados brutos de receita,
producao, ocupacao e massa salarial para anos muito anteriores ao inicio da
base. O preenchimento agora acontece no nivel do coeficiente final, que e o
objeto efetivamente consumido pelas matrizes.

## Por que essa escolha e mais defensavel

Com a cobertura atual dos dados, tentar extrapolar dez anos para tras por
regressao linear simples exige assumir uma trajetoria temporal que a base nao
consegue sustentar. Mesmo quando um metodo de interpolacao ou extrapolacao
parece tecnicamente simples, ele entra em risco de produzir trajetorias
irreais, porque o problema principal nao e a falta de tecnica de previsao, e
sim a falta de observacoes historicas suficientes.

Por isso, nesta camada:

- nao tratamos retroprojecao longa como substituto de dado real;
- evitamos extrapolar os componentes brutos para anos sem observacao;
- adotamos um criterio de preenchimento mais estavel e explicitamente
  documentado.

A conclusao metodologica e direta: para melhorar essa serie de forma robusta, e
preciso ampliar a base historica. Nao basta escolher um metodo mais agressivo e
extrapolar muitos anos para tras.

## Caso especifico de `AAProdução`

A conta `AAProdução` continua sendo tratada por pressuposto temporario. Neste
modulo, sua:

- `prod_mon_trab` e mantida constante;

Essa decisao e provisoria e deve ser entendida como uma solucao de contorno
enquanto nao houver uma integracao mais consistente com futuros dados sobre as
trajetorias tecnologicas da atividade. A resolucao estrutural desse ponto fica
em aberto para revisao futura da camada.

## Fluxo atual da camada

O fluxo implementado em `utils.py` e `preparacao_camada_renda.py` agora segue
esta ordem:

1. carregar os dados de PIA e PAC;
2. limpar e padronizar colunas numericas;
3. agregar as observacoes por `conta_alfa` e por ano observado;
4. calcular os coeficientes finais observados;
5. completar os anos alvo com backcast constante antes do primeiro observado e
   manutencao constante depois do ultimo observado;
6. adicionar `AAProdução` como serie constante por pressuposto temporario;
7. publicar o resultado na zona gold.
