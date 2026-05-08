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

A camada voltou a usar `previsao_renda.py` como etapa central da preparacao,
isto e, a imputacao acontece nas variaveis brutas de PIA e PAC antes do calculo
dos coeficientes finais.

A regra atual e:

1. agregar as variaveis brutas por `ano` + `divisao_grupo_cnae_2`;
2. projetar essas series agregadas com o
   `IncomeForecaster`;
3. manter o valor da regressao linear enquanto a retroprojecao permanecer
   positiva;
4. no primeiro ano em que a retroprojecao linear ficar `<= 0`, congelar a serie
   para tras no ultimo valor retroprojetado positivo, em vez de zerar;
5. agregar os dados projetados por `conta_alfa`;
6. calcular `prod_mon_trab` e `salario_medio` a partir dessa base projetada;
7. adicionar `AAProdução` como serie constante por pressuposto temporario.

Em outras palavras, a camada continua retroprojetando as variaveis brutas, mas
o forecaster nao deixa mais a serie colapsar para zero quando a regressao
linear cruza o eixo.

## Implicacao metodologica

Essa abordagem preserva o trecho da regressao linear que ainda produz valores
positivos e evita o artefato mais grave da implementacao antiga, que era zerar
os numeradores de algumas razoes em anos antigos.

Ao mesmo tempo, a limitacao estrutural continua valendo: como as bases
observadas comecam apenas em `2007`, qualquer retroprojecao longa ainda depende
de uma hipotese forte sobre a trajetoria historica. O ajuste atual melhora o
comportamento do forecast, mas nao substitui a necessidade de ampliar a base
historica se a intencao for sustentar inferencias mais fortes para anos muito
anteriores.

## Caso especifico de `AAProdução`

A conta `AAProdução` continua sendo tratada por pressuposto temporario. Neste
modulo, sua:

- `prod_mon_trab` e mantida constante;
- `salario_medio` e mantido constante.

Essa decisao e provisoria e deve ser entendida como uma solucao de contorno
enquanto nao houver uma integracao mais consistente com futuros dados sobre as
trajetorias tecnologicas da atividade. A resolucao estrutural desse ponto fica
em aberto para revisao futura da camada.

## Fluxo atual da camada

O fluxo implementado em `utils.py` e `preparacao_camada_renda.py` agora segue
esta ordem:

1. carregar os dados de PIA e PAC;
2. limpar e padronizar colunas numericas;
3. agregar as bases por `ano` + `divisao_grupo_cnae_2`;
4. aplicar `IncomeForecaster` nessas series agregadas;
5. usar a regra de estabilizacao retroativa quando a regressao ficar `<= 0`;
6. agregar os dados projetados por `conta_alfa`;
7. calcular os coeficientes finais para todos os anos alvo;
8. adicionar `AAProdução` como serie constante por pressuposto temporario;
9. publicar o resultado na zona gold.
