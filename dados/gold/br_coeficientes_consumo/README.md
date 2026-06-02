# Valores de consumo

Esta camada prepara a biblioteca de valores monetarios de consumo que alimenta
a modelagem Layer 2. A fonte e a POF/IBGE na tabela silver
`br_ibge_pof.tbl_6970`, usando a variável
`Despesa monetária e não monetária média mensal familiar`, originalmente em
Reais.

Pela metodologia revisada das Contas Alfa, a gold nao deve antecipar o calculo
do coeficiente tecnico. A razao e simples: o coeficiente so ganha sentido
quando e confrontado com a base economica sobre a qual ele incide. Essa base
esta no repositorio de modelagem, junto com ano, produto, regiao, VBP, renda,
grid de parametros e mapa de incidencia. Aqui, portanto, o papel do ETL e
preservar o valor observado da fonte secundaria e entregar esse valor em uma
taxonomia que o modelo reconhece.

Em termos praticos, a saida desta camada e:

- `ano`: ano de referencia da POF;
- `coeff_key`: chave de demanda usada pela modelagem;
- `valor`: despesa monetaria observada, convertida para `Mil Reais`.

E importante manter essa escala em mente. O raw da POF vem em Reais unitarios,
mas esta gold divide os valores por 1000 para exportar consumo na mesma escala
monetaria da camada de custos:

- `br_ibge_pof.tbl_6970`: `Despesa monetária e não monetária média mensal familiar` com `unidade = Reais`;
- a mesma tabela tambem traz a variavel de distribuicao com `unidade = %`, mas
  essa variavel percentual nao e usada nesta gold.
- a saida `valor` desta gold fica em `Mil Reais`, ou seja, cada unidade
  numerica representa R$ 1.000.

`valor` nao e percentual, nao deve somar 1 e nao deve ser reescalado para o
intervalo `[0, 1]`. Ele e o insumo monetario que a modelagem usara depois para
calcular a intensidade tecnica do consumo no contexto correto.

## Compatibilizacao

O arquivo `equivalencia_despesas.json` faz o de-para entre a classificacao de
despesas da POF e as chaves de demanda esperadas pela modelagem. Ele nao
armazena coeficientes nem valores finais; apenas explicita a ponte entre a
taxonomia da fonte e a taxonomia operacional da CSAlpha.

A regra de situacao do domicilio segue a compatibilizacao historica:

- chaves estaduais usam a abertura urbana;
- as demais chaves usam a abertura rural.

Essa escolha nao transforma o dado em proporcao. Ela apenas seleciona qual
recorte da POF alimenta cada chave do modelo.

## Fluxo

O fluxo `preparacao_camada_consumo.py` executa:

1. ler a POF da silver;
2. carregar `equivalencia_despesas.json`;
3. filtrar o ano alvo e a variavel monetaria em Reais;
4. aplicar a regra urbano/rural de compatibilizacao;
5. dividir `valor` por 1000 para harmonizar a escala monetaria com custos;
6. publicar `ano`, `coeff_key` e `valor`.

## Leitura para a modelagem

O consumo obedece a mesma leitura geral da camada de custos: a gold entrega o
valor bruto observado, e o repositorio de modelagem calcula o coeficiente
tecnico quando tiver a matriz de incidencia e a base de aplicacao. Isso evita
misturar duas etapas diferentes: tratamento de dados secundarios neste repo e
calibracao economica no repo `csalpha`.
