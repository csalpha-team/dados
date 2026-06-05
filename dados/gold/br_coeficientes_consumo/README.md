# Coeficientes de consumo

Esta camada prepara os coeficientes de consumo que alimentam a modelagem Layer
2. A fonte é a POF/IBGE na tabela silver `br_ibge_pof.tbl_6970`, usando a
variável `Distribuição da despesa monetária e não monetária média mensal
familiar`, originalmente publicada em percentual.

## Contrato

A tabela gold `br_coeficientes_consumo.preparacao_camada_consumo` publica:

- `ano`: ano de referência da POF;
- `coeff_key`: chave de demanda usada pela modelagem;
- `coeff`: participação da despesa convertida para razão no intervalo `[0, 1]`.

O campo `coeff` é a coluna contratual do repositório para coeficientes. A
variável percentual da POF é dividida por 100 durante a transformação.

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
3. filtrar o ano alvo e a variável percentual;
4. aplicar a regra urbano/rural de compatibilização;
5. converter percentual em razão;
6. validar o schema Pydantic;
7. publicar `ano`, `coeff_key` e `coeff` na gold.
