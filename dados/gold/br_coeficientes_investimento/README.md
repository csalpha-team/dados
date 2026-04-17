# Coeficientes de investimento

Esta camada publica um conjunto pequeno de coeficientes de investimento usados
como parametros do modelo. Diferente de outras etapas da gold, esses valores
nao sao estimados aqui a partir de uma base transacional. Eles sao mantidos
como parametros fixos em `coeficientes_investimento.json` e carregados por
`preparacao_camada_investimento.py`.

## Origem dos valores hardcoded

Os numeros do JSON vem de percentuais de custos obtidos em pesquisa anterior
conduzida pelo professor Francisco de Assis Costa. O pipeline atual nao
reproduz esse calculo original. A responsabilidade desta etapa e bem mais
simples: versionar esses percentuais, validar a estrutura do arquivo e
disponibilizar o resultado na tabela
`br_coeficientes_investimento.coeficientes_investimento`.

Esses percentuais descrevem a necessidade de investimentos observada no balanco
patrimonial, principalmente em tres grupos:

- veiculos;
- construcao civil e benfeitorias;
- maquinas e equipamentos.

No modelo, cada chave identifica um tipo de investimento e cada valor indica a
parcela de referencia associada a essa necessidade de investimento segundo a
pesquisa usada como base.

## Por que os valores ficam hardcoded

Os coeficientes estao hardcoded por desenho, nao por ausencia de implementacao.
A logica de estimacao foi produzida fora deste repositorio, em estudo anterior.
Como o dado que chega aqui ja e um percentual consolidado, esta camada apenas:

1. le o JSON versionado no codigo;
2. converte o conteudo para as colunas `coeff_key` e `coeff`;
3. grava o resultado na zona gold para consumo pelas demais etapas.

Se a pesquisa de referencia for revisada, a atualizacao deve ser feita
manualmente no arquivo `coeficientes_investimento.json`, preservando as chaves
esperadas nas camadas que consomem esses coeficientes.

## Leitura das chaves

As chaves do JSON nao sao so nomes descritivos; elas funcionam como contrato de
integracao com outras camadas. Por isso, o ETL nao tenta renomea-las nem
recalcula-las.

| Chave | Uso no modelo |
| --- | --- |
| `InvestConstCivilEstad` | Necessidade de investimento associada a construcao civil na chave usada para consumo estadual. |
| `InvestConstCivilLoc` | Necessidade de investimento associada a construcao civil na chave usada para consumo local. |
| `InvestConstEBenfeitorias` | Necessidade de investimento vinculada a construcao civil e benfeitorias na chave geral. |
| `InvestConstEBenfeitoriasEstadual` | Necessidade de investimento vinculada a construcao civil e benfeitorias na chave estadual. |
| `InvestMáqEVeículos` | Necessidade de investimento vinculada a maquinas, equipamentos e veiculos na chave geral. |
| `InvestMáqEVeículosEstad` | Necessidade de investimento vinculada a maquinas, equipamentos e veiculos na chave estadual. |
| `InvestMaqEVeículosLoc` | Necessidade de investimento vinculada a maquinas, equipamentos e veiculos na chave local. A grafia da chave e preservada exatamente como esta porque ela ja e consumida assim por outras etapas. |
| `InvestPlantio` | Coeficiente mantido separadamente para formacao de plantio, hoje usado no mapeamento de novas culturas permanentes, silvicultura e formacao de pastagens. |

Os sufixos `Loc`, `Estad` e `Estadual` indicam as chaves que outras camadas
esperam encontrar. Nesta etapa nao existe desagregacao territorial adicional:
o pipeline apenas publica o valor associado a cada chave.

## Fluxo da camada

O fluxo implementado em `preparacao_camada_investimento.py` e direto:

1. localizar o JSON de coeficientes, seja no caminho padrao ou via variavel de ambiente;
2. carregar o arquivo com `carregar_coeficientes_investimento`;
3. validar que o conteudo e um objeto simples de chave e valor;
4. transformar o conteudo em tabela com `coeff_key` e `coeff`;
5. publicar o resultado no schema `br_coeficientes_investimento`.

Em outras palavras: a camada de investimento nao gera os percentuais; ela
formaliza, versiona e distribui percentuais ja definidos pela pesquisa de
referencia.
