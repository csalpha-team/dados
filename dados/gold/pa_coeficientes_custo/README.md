# Coeficientes de custo

Esta camada prepara os coeficientes de custo rural para a modelagem Layer 2. A
fonte principal é o Censo Agropecuário, lido a partir das tabelas silver
`al_ibge_censoagro.tbl_1909_2006` e `al_ibge_censoagro.tbl_6899_2017`,
filtradas para municípios do Pará.

## Contrato

A tabela gold `pa_coeficientes_custo.preparacao_camada_custo` publica:

- `ano`: ano do Censo Agropecuário mais recente disponível para a chave;
- `nome_regiao_integracao`: região de integração do Pará;
- `tipo_coeff`: chave de custo esperada pela modelagem;
- `coeff`: participação do item de despesa em relação à despesa total do
  estabelecimento, agregada por região.

O campo `coeff` é a coluna contratual do repositório para coeficientes. A linha
`Total` da fonte é usada como denominador municipal e não é exportada como item
de custo.

## Compatibilização

O arquivo `parametros_coeficientes_custo.json` faz o de-para entre os itens de
despesa do Censo Agropecuário e as chaves de custo usadas pela CSAlpha.

Alguns exemplos:

- `InsumoEnergia` recebe `Energia elétrica`;
- `InsumosCombustível` recebe `Combustíveis e lubrificantes`;
- `InsumosMecânicos` recebe `Aluguel de máquina` e
  `Compra de máquinas e veículos`;
- `EmbalagemBenefEstad`, `EmbalagemBenefLoc`, `EmbalagemTransfEstad` e
  `EmbalagemTransfLoc` recebem `Sacarias e embalagens`.

Quando um item do Censo alimenta mais de uma chave, o coeficiente é replicado.
Essa replicação é intencional porque cada chave representa uma incidência
distinta na modelagem.

## Fluxo

O fluxo `preparacao_camada_custo.py` executa:

1. ler as despesas do Censo Agropecuário na silver;
2. mapear cada município para sua região de integração;
3. carregar `parametros_coeficientes_custo.json`;
4. calcular `coeff = valor_despesa / Total` por município e item;
5. expandir cada `expense_type` para uma ou mais chaves `tipo_coeff`;
6. agregar os coeficientes por ano, região de integração e chave de custo;
7. selecionar o ano mais recente para cada região e chave;
8. validar o schema Pydantic;
9. publicar a tabela gold `pa_coeficientes_custo.preparacao_camada_custo`.
