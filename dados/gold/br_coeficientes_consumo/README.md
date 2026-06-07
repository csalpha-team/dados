# Valores de consumo

Esta camada prepara os valores monetários de consumo que alimentam a modelagem
Layer 2. A fonte é a POF/IBGE na tabela silver `br_ibge_pof.tbl_6970`, usando a
variável `Despesa monetária e não monetária média mensal familiar`.

## Contrato

A tabela gold `br_coeficientes_consumo.preparacao_camada_consumo` publica:

- `ano`: ano de referência da POF;
- `coeff_key`: chave de demanda usada pela modelagem;
- `valor`: despesa monetária observada, convertida de Reais para `Mil Reais`.

O campo `valor` não é proporção e não deve somar 1. A Layer 2 usa esse valor
monetário para calcular os coeficientes técnicos no contexto da modelagem.

## Compatibilização

O arquivo `equivalencia_despesas.json` faz o de-para entre a classificação de
despesas da POF e as chaves de demanda esperadas pela modelagem. A regra de
situação do domicílio segue a compatibilização histórica:

- chaves estaduais usam a abertura urbana;
- as demais chaves usam a abertura rural.

## Fluxo

O fluxo `preparacao_camada_consumo.py` executa:

1. ler a POF da silver;
2. carregar `equivalencia_despesas.json`;
3. filtrar o ano alvo e a variável monetária;
4. aplicar a regra urbano/rural de compatibilização;
5. converter Reais para `Mil Reais`;
6. validar o schema Pydantic;
7. publicar `ano`, `coeff_key` e `valor` na gold.
