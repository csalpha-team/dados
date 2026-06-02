# Valores de custo

Esta camada prepara uma biblioteca de valores monetarios de custo rural para a
modelagem Layer 2. A fonte principal e o Censo Agropecuario, lido a partir das
tabelas silver `al_ibge_censoagro.tbl_1909_2006` e
`al_ibge_censoagro.tbl_6899_2017`, filtradas para municipios do Para.

Pela metodologia revisada das Contas Alfa, esta gold nao calcula mais
participacoes do total nem coeficientes tecnicos. O coeficiente tecnico depende
do contexto economico em que sera aplicado: produto, regiao, ano, VBP, grid de
parametros e matriz de incidencia. Esses elementos pertencem ao repositorio de
modelagem. O papel deste repo de dados e tratar a fonte secundaria, preservar o
valor monetario observado e entregar uma tabela pronta para a calibracao no
`csalpha`.

Em termos praticos, a saida desta camada e:

- `ano`: ano do Censo Agropecuario;
- `nome_regiao_integracao`: regiao de integracao do Para;
- `tipo_coeff`: chave de custo esperada pela modelagem;
- `valor`: despesa monetaria observada, em Reais.

`valor` nao e uma razao contra a despesa total. A linha `Total` da fonte e
removida porque nao representa um item de custo; ela so poderia ser usada como
denominador em outro contexto. A divisao pelo VBP ou por qualquer base de
incidencia deve acontecer na modelagem, nao nesta gold.

## Compatibilizacao

O arquivo `parametros_coeficientes_custo.json` faz o de-para entre os itens de
despesa do Censo Agropecuario e as chaves de custo usadas pela CSAlpha. Ele nao
armazena valores finais nem coeficientes prontos; ele apenas explicita como a
taxonomia da fonte deve alimentar a taxonomia operacional do modelo.

Alguns exemplos:

- `InsumoEnergia` recebe `Energia elétrica`;
- `InsumosCombustível` recebe `Combustíveis e lubrificantes`;
- `InsumosMecânicos` recebe `Aluguel de máquina` e
  `Compra de máquinas e veículos`;
- `EmbalagemBenefEstad`, `EmbalagemBenefLoc`, `EmbalagemTransfEstad` e
  `EmbalagemTransfLoc` recebem `Sacarias e embalagens`.

Quando um item do Censo alimenta mais de uma chave, o valor e replicado. Essa
replicacao e intencional: cada chave representa uma incidencia distinta que sera
resolvida na modelagem. A gold nao tenta repartir esse valor sem conhecer a
matriz de incidencia.

## Fluxo

O fluxo `preparacao_camada_custo.py` executa:

1. ler as despesas do Censo Agropecuario na silver;
2. mapear cada municipio para sua regiao de integracao;
3. carregar `parametros_coeficientes_custo.json`;
4. remover a despesa `Total`;
5. expandir cada `expense_type` para uma ou mais chaves `tipo_coeff`;
6. agregar `valor` por ano, regiao de integracao e chave de custo;
7. publicar a tabela gold `pa_coeficientes_custo.preparacao_camada_custo`.

## Leitura para a modelagem

Na leitura metodologica, cada linha desta tabela representa o valor observado
de um item de custo em uma regiao e ano. O repositorio `csalpha` deve combinar
esse valor com o VBP, o produto, a agregacao e a incidencia para obter o
coeficiente tecnico que efetivamente escala a matriz. Essa separacao evita que
um percentual agregado do Censo seja aplicado como se fosse coeficiente
especifico de produto ou fluxo.

## Manutencao

- Ajustes de taxonomia devem ser feitos em `parametros_coeficientes_custo.json`.
- Renomear `tipo_coeff` exige cuidado, pois essas chaves sao contrato com a
  modelagem.
- A ordem das entradas no JSON nao altera o resultado, mas agrupamentos
  coerentes facilitam revisao.
- Novas fontes ou novos anos devem continuar preservando a unidade monetaria em
  `valor`.
