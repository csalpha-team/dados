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

Na implementacao anterior, a camada aplicava previsao linear sobre as variaveis
brutas de PIA e PAC para cobrir todo o intervalo pedido. Esse desenho produzia
efeitos indesejados:

1. a regressao linear extrapolava algumas variaveis para valores negativos nos
   anos anteriores a `2007`;
2. como a previsao era truncada em zero, os numeradores de algumas razoes
   viravam `0.0` enquanto o denominador seguia positivo;
3. o resultado final era uma serie de coeficientes `0.0` em anos antigos para
   alguns setores, o que gerava matrizes com setores aparentando nao pagar
   salarios ou nao ter produtividade monetaria do trabalho.
4. mesmo depois de estabilizar as variaveis brutas para evitar zeros, a passagem
   de `2006` para `2007` podia continuar abrupta, porque `2006` era imputado
   sobre variaveis brutas e `2007` ja era observado.

Esse zero nao representava um fato economico observado. Era um artefato da
extrapolacao. A mudanca brusca em `2007` tambem nao deveria ser lida como
crescimento economico observado, mas como efeito da transicao entre anos
retroprojetados e o primeiro ano efetivamente disponivel.

## Solucao adotada

A camada passou a calcular primeiro os coeficientes diretamente nos anos
observados e, so depois, completar os anos fora da janela observada por uma
retroprojecao ancorada no primeiro coeficiente observado.

O CAGR, sigla de *Compound Annual Growth Rate*, e a taxa media anual composta
de crescimento de uma serie entre dois pontos no tempo. Ele responde a pergunta:
"qual taxa anual constante levaria o primeiro valor observado ao ultimo valor
observado?". Nesta camada, o CAGR e calculado para cada par `conta_alfa` +
`tipo_coeff` usando os coeficientes observados positivos. A taxa resultante nao
e tratada como crescimento historico observado antes de `2007`; ela e apenas a
regra tecnica usada para construir uma trajetoria imputada mais continua.

A regra atual e:

1. agregar as variaveis brutas por `ano` + `divisao_grupo_cnae_2`;
2. agregar essas variaveis por `conta_alfa`;
3. calcular `prod_mon_trab` e `salario_medio` somente para anos observados;
4. para cada par `conta_alfa` + `tipo_coeff`, calcular o CAGR entre o primeiro
   e o ultimo coeficiente observado positivo;
5. preencher anos anteriores ao primeiro observado caminhando para tras a partir
   do primeiro valor observado:

   `coef_ano = coef_primeiro_observado / (1 + CAGR) ** distancia`

6. preencher anos posteriores ao ultimo observado caminhando para frente a partir
   do ultimo valor observado:

   `coef_ano = coef_ultimo_observado * (1 + CAGR) ** distancia`

7. quando nao houver dois valores positivos para calcular CAGR, manter o valor
   observado constante para fora da janela observada;
8. adicionar `AAProdução` como serie constante por pressuposto temporario.

Em outras palavras, a retroprojecao deixou de ser feita sobre as variaveis
brutas e passou a ser feita sobre os coeficientes finais, sempre ancorada no
primeiro ano observado. Isso reduz a descontinuidade artificial entre `2006` e
`2007`, porque `2006` passa a ser uma extensao geometrica direta do nivel
observado em `2007`.

## Implicacao metodologica

Essa abordagem evita que a retroprojecao de numeradores e denominadores brutos
crie razoes instaveis perto do primeiro ano observado. A serie imputada fica
ancorada no coeficiente observado e segue uma taxa media anual composta estimada
com base na propria trajetoria observada.

Ao mesmo tempo, a limitacao estrutural continua valendo: como as bases
observadas comecam apenas em `2007`, qualquer retroprojecao longa ainda depende
de uma hipotese forte sobre a trajetoria historica. O CAGR usado para `1995` a
`2006` deve ser entendido como imputacao tecnica, nao como observacao economica.
O ajuste atual melhora a continuidade da serie, mas nao substitui a necessidade
de ampliar a base historica se a intencao for sustentar inferencias mais fortes
para anos muito anteriores.

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
3. identificar os anos observados de cada base;
4. agregar as bases observadas por `ano` + `divisao_grupo_cnae_2`;
5. agregar os dados observados por `conta_alfa`;
6. calcular os coeficientes observados;
7. completar anos nao observados por CAGR ancorado no primeiro/ultimo valor
   observado;
8. adicionar `AAProdução` como serie constante por pressuposto temporario;
9. publicar o resultado na zona gold.
