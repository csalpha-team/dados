# Valores de custo

Esta camada prepara os valores monetários de custo rural para a modelagem Layer
2. A fonte principal é o Censo Agropecuário, lido a partir das tabelas silver
`al_ibge_censoagro.tbl_1909_2006` e `al_ibge_censoagro.tbl_6899_2017`,
filtradas para municípios do Pará.

## Contrato

A tabela gold `pa_coeficientes_custo.preparacao_camada_custo` publica:

- `ano`: ano do Censo Agropecuário;
- `nome_regiao_integracao`: região de integração do Pará;
- `tipo_coeff`: chave de custo esperada pela modelagem;
- `valor`: despesa monetária observada, em `Mil Reais`.

O campo `valor` não é proporção contra a despesa total. A linha `Total` da
fonte é removida como item de custo e a Layer 2 calcula a incidência técnica
posteriormente.

## Compatibilização

O arquivo `parametros_coeficientes_custo.json` faz o de-para entre os itens de
despesa do Censo Agropecuário e as chaves de custo usadas pela CSAlpha. Quando
um item do Censo alimenta mais de uma chave, o valor é replicado para cada
chave, pois cada uma representa uma incidência distinta na modelagem.

## Fluxo

O fluxo `preparacao_camada_custo.py` executa:

1. ler as despesas do Censo Agropecuário na silver;
2. enriquecer cada município com sua região de integração;
3. carregar `parametros_coeficientes_custo.json`;
4. remover a despesa `Total`;
5. expandir cada `expense_type` para uma ou mais chaves `tipo_coeff`;
6. somar `valor` por ano, região e chave;
7. validar o schema Pydantic;
8. publicar a tabela gold `pa_coeficientes_custo.preparacao_camada_custo`.
