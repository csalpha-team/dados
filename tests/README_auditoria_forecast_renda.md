# Auditoria do forecast de renda

Este documento descreve os testes adicionados para auditar os coeficientes de
renda gerados pela retroprojecao ancorada por CAGR.

## Objetivo

Os testes verificam se a serie final de coeficientes:

1. permanece positiva, isto e, sem valores iguais a zero ou negativos;
2. respeita uma banda estatistica de variacao temporal;
3. gera um CSV de auditoria quando uma variacao anual ultrapassa a banda;
4. identifica `conta_alfa`, `tipo_coeff`, ano, valor e possivel motivo da
   discrepancia.

## Banda de tolerancia

A auditoria usa variacoes logaritmicas anuais:

`variacao_log = log(coef_ano / coef_ano_anterior)`

Para cada par `conta_alfa` + `tipo_coeff`, a auditoria calcula uma taxa esperada
em log a partir do CAGR da serie positiva:

`cagr_log = log(coef_ultimo / coef_primeiro) / distancia_em_anos`

Em seguida, calcula os residuos entre a variacao anual observada e esse CAGR.
A banda estatistica usa uma escala robusta baseada em MAD (*median absolute
deviation*), convertida para uma aproximacao de desvio padrao:

`sigma_robusto = 1.4826 * MAD`

A tolerancia final e:

`max(sigma_multiplier * sigma_robusto, log(1 + min_relative_tolerance))`

Esse piso evita bandas degeneradas quando a serie e quase perfeitamente
geometrica. Por padrao, a tolerancia minima relativa e `20%` e o multiplicador
estatistico e `3.0`.

## Discrepancias auditadas

O CSV de auditoria contem uma linha por problema encontrado. Os tipos atuais sao:

- `valor_nao_positivo`: o coeficiente e `<= 0` ou nulo;
- `intervalo_temporal_irregular`: existe salto de anos na serie;
- `acima_banda_superior`: a variacao logaritmica anual excede a banda superior;
- `abaixo_banda_inferior`: a variacao logaritmica anual excede a banda inferior.

A coluna `possivel_motivo` traz uma interpretacao operacional para orientar a
inspecao. Ela nao substitui analise economica; serve para apontar rapidamente
se o problema parece vir de valor nao positivo, lacuna temporal ou quebra
abrupta de variacao.

## Testes implementados

Os testes ficam em `tests/test_renda_forecast_pipeline.py`.

1. `test_auditoria_aprova_serie_cagr_sem_discrepancia`
   Verifica que uma serie com crescimento geometrico constante nao gera
   discrepancias.

2. `test_auditoria_exporta_csv_para_variacao_abrupta`
   Cria uma serie com salto abrupto, valida o disclaimer de auditoria e confirma
   a exportacao em CSV.

3. `test_auditoria_reprova_coeficientes_zero_ou_negativos`
   Confirma que coeficientes iguais a zero ou negativos sao sempre reprovados,
   independentemente da banda estatistica.
