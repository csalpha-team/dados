# Diagnóstico e correção monetária dos coeficientes de renda

## Conclusão do diagnóstico

A queda acentuada anterior a 2007 não era explicada apenas pela inflação. Ela
resultava da combinação de duas decisões do pipeline:

1. produção/receita, salários e emprego eram projetados separadamente por uma
   tendência aditiva, sem ancorar a extrapolação no valor observado da borda,
   e a produtividade era calculada somente depois;
2. o limite de crescimento de 50% era aplicado em ordem cronológica desde
   1995, permitindo que um backcast pequeno rebaixasse inclusive os valores
   observados seguintes.

Nos dumps anteriores à correção, `AFIndustBenef`, por exemplo, tinha
produtividade `24,06` em 2002 e depois seguia exatamente a progressão de 50% ao
ano até `182,72` em 2007. Foram encontrados cinco passos iguais a 50% nas
contas de indústria beneficiada e sete nas contas de transformação. Portanto,
a forma geométrica vista no gráfico era em grande parte uma consequência do
clamp, não uma evidência de comportamento econômico exponencial.

A ausência de correção monetária era um segundo problema confirmado. As APIs
do IBGE identificam produção, receita e salários em `Mil Reais`, enquanto
`pessoal_ocupado_31_12` está em `Pessoas`. O código misturava valores monetários
nominais de anos diferentes antes de estimar a tendência.

As diferenças OLD/NEW posteriores a 2007 também não são apenas estatísticas. O
mapeamento industrial antigo usava `10, 11, 12, 17, 19, 20, 24` para indústria
beneficiada e `13–33` (com exceções) para transformação. A configuração atual
usa apenas `10, 11, 20` e `11`, respectivamente. Portanto, parte da divergência
decorre de uma mudança explícita de definição setorial; sua validade econômica
depende da regra de negócio que motivou os commits anteriores.

## Dados e ano-base

- PIA observada: 2007–2022;
- PAC observada: 2007–2023;
- saída: 1995–2023;
- último ano observado comum e ano-base: **2022**;
- inflação: IPCA mensal, série **SGS 433**, Banco Central do Brasil;
- fonte da API: <https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados>.

A série 433 fornece variações percentuais mensais, não um número-índice pronto.
A silver encadeia os fatores mensais, calcula a média anual do nível de preços
e marca se o ano contém doze observações. A média anual é usada porque PIA e
PAC representam fluxos monetários acumulados durante o ano.

Para cada ano `t`, o fator aplicado é:

```text
fator(t) = indice_ipca(2022) / indice_ipca(t)
valor_real(t) = valor_nominal(t) * fator(t)
```

O fator de 2022 é exatamente `1`. Anos ausentes, incompletos, duplicados ou com
índice inválido interrompem o fluxo; nenhum valor é preenchido silenciosamente
com zero.

## Alterações metodológicas

- Somente colunas monetárias da PIA/PAC são corrigidas antes do forecast.
- Emprego continua sendo quantidade física e não recebe fator IPCA.
- `AAProdução`, cujo valor nominal era replicado pelo legado em todos os anos,
  tem cada réplica anual convertida pela mesma razão de índices; em 2022 mantém
  exatamente o valor de configuração.
- A tolerância de 50% não altera mais nenhum ano observado. No backcast, ela é
  aplicada para trás a partir do primeiro observado; no forecast, para frente
  a partir do último observado.
- As extrapolações lineares/Theil–Sen são ancoradas no primeiro valor observado
  para trás e no último para frente, removendo o salto causado pelo intercepto
  robusto na fronteira.
- Os coeficientes finais têm unidade anual de `mil R$ de 2022 por trabalhador`.

Com os dados oficiais extraídos em 22/07/2026, `AFIndustBenef` corrigido passa
de `307,25` (1995) para `578,85` (2007), em vez da escada geométrica anterior.
No ano-base, o valor observado é preservado (`890,39` em 2022). Para
`AAProdução`, o valor configurado permanece `10,789` em 2022.

Depois da ancoragem da extrapolação, a passagem 2006→2007 do emprego usado pelo
preditor varia `1,70%` na indústria, `2,23%` no atacado e `1,87%` no varejo. A
correção IPCA não muda nenhum desses valores; a melhora vem exclusivamente da
regra de continuidade do preditor.

## Gráficos e limites

- `produtividade_coeficientes_comparacao.png` compara o dump antigo, o dump
  atual nominal e a implementação corrigida. Contas A/B/C com a mesma família
  são apresentadas pela média simples apenas para facilitar a leitura.
- `emprego_entrada_auditoria.png` demonstra que as séries de emprego fornecidas
  ao preditor são idênticas antes e depois da correção monetária.
- `resumo_produtividade.csv` contém os pontos selecionados do comparativo.
- `coeficientes_produtividade_corrigidos.csv` contém a saída de produtividade
  corrigida usada no gráfico.

O indicador downstream **G14 — Emprego Setorial** não é calculado neste
repositório e sua fórmula não foi encontrada no workspace. Assim, o segundo
gráfico audita a entrada quantitativa deste pipeline, mas não pretende
reproduzir o G14 anexado. Para fechar essa comparação ponta a ponta, o consumidor
Layer 2 que deriva G14 dos coeficientes de renda precisa ser executado com o
novo pacote exportado.

## Reprodução dos gráficos

```bash
uv run python scripts/gerar_diagnostico_correcao_renda.py \
  --old-wide /caminho/income_productivity_OLD.csv \
  --current-wide /caminho/income_productivity_NEW.csv \
  --corrected-long /caminho/coeficientes_corrigidos.csv \
  --pia-silver /caminho/pia_silver.csv \
  --pac-silver /caminho/pac_silver.csv \
  --output-dir docs/diagnostico_correcao_renda
```
