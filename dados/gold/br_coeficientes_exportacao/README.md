# Coeficientes de exportacao

Este modulo prepara coeficientes de exportacao para a zona gold a partir dos
registros de comercio exterior da COMEXSTAT. O resultado final e uma tabela por
ano e produto com:

- `valor_fob_dolar`;
- `valor_fob_real`;
- `coeff`, calculado como a participacao do produto no valor total exportado do
  ano considerado.

A entrada principal da camada e o arquivo
`parametros_coeficientes_exportacao.json`, que define quais NCMs devem ser
associados a cada produto e quais regras de reparticao especifica devem ser
aplicadas quando a classificacao aduaneira e mais ampla do que o produto de
interesse.

## Origem metodologica

Esta camada usa como referencia a metodologia empregada para estimar as
exportacoes de derivados de acai no Para quando a base de comercio exterior nao
identifica o produto com granularidade suficiente. A ideia central e a
seguinte:

1. o codigo NCM `20079921` corresponde especificamente a polpa de acai e, por
   isso, entra integralmente no calculo;
2. os codigos NCM `20098990`, `20089900` e `08119000` sao classificacoes mais
   amplas, que incluem outros produtos;
3. para o estado do Para, considera-se que o acai e seus derivados representam,
   em media, 87% do volume e do valor observados nessas tres classificacoes;
4. esse fator medio e aplicado para estimar a parcela do acai dentro desses
   NCMs agregados.

Essa regra e atribuida por Homma et al. (2017) e reaparece em nota tecnica
mais recente sobre a economia do acai, usada aqui como referencia de apoio para
descrever a parametrizacao adotada no modulo.

## Como a logica entra em `parametros_coeficientes_exportacao.json`

O JSON tem dois blocos centrais:

- `composicao_produtos`: define o "de para" entre cada produto do modelo e os
  NCMs encontrados na base, usando objetos com `id_ncm` e `nome_ncm`. O
  processamento usa `id_ncm` como chave principal e mantém `nome_ncm` como
  descrição/auditoria;
- `participacoes_especificas`: sobrescreve a divisao padrao quando um produto
  precisa de um peso diferente de reparticao.

Cada item de `composicao_produtos` tambem pode receber campos opcionais para
explicitar a regra adotada:

- `tipo_match`: `exato`, `generico_outros`, `estimado` ou `excluir`;
- `participacao`: peso aplicado diretamente ao produto naquele NCM;
- `grupo_distribuicao`: rotulo auditavel para NCMs genericos repartidos entre
  varios produtos.

Quando `tipo_match` nao e informado, descricoes iniciadas por "Outros/Outras"
ou com "de outras/de outros" sao classificadas como `generico_outros`. Quando
essa inferencia e ampla demais, o proprio item deve receber `tipo_match:
exato`, como ocorre em NCMs do tipo "Outros sucos de abacaxi" ou "Outros oleos
de milho". A distribuicao produtiva continua usando a regra historica:
participacoes explicitas primeiro; saldo restante dividido igualmente entre os
produtos relacionados ao mesmo NCM.

Depois da revisao manual dos matches, foram removidos da parametrizacao os NCMs
textuais que geravam falsos positivos recorrentes, como misturas genericas de
frutas, caramelos sem acucar e casos especificos sem aderencia ao produto. Os
itens anotados como falsos genericos foram mantidos no de-para com
`tipo_match: exato`; o caso de residuos de coco/copra foi marcado como
`generico_outros`.

O arquivo tambem referencia `taxa_cambio.csv`, da raw `pa_me_comex_stat`, para
converter `valor_fob_dolar` em `valor_fob_real` com a taxa correspondente a cada
ano. Assim, a conversao nao depende mais de uma taxa fixa.

No caso do acai, a logica funciona assim:

1. `AcaiFruto` e associado em `composicao_produtos` a quatro NCMs:
   `Purês de açaí (Euterpe oleracea)`,
   `Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar`,
   `Outras frutas não cozidas ou cozidas em água ou vapor, congeladas, mesmo adicionadas de açúcar ou de outros edulcorantes`
   e `Outras frutas, partes de plantas, preparadas/conservadas de outro modo`.
2. essas descricoes representam, na pratica, as classificacoes especifica e
   genericas usadas para captar o acai e seus derivados na base de exportacao.
3. em `participacoes_especificas`, a descricao especifica
   `Purês de açaí (Euterpe oleracea)` recebe `1.0`, porque corresponde ao item
   diretamente identificado como derivado de acai.
4. as tres descricoes genericas recebem `0.87`, traduzindo a hipotese de que,
   no Para, 87% do valor e da quantidade observados nessas classificacoes dizem
   respeito ao acai e seus derivados.

Em outras palavras, o JSON nao armazena os coeficientes finais de exportacao.
Ele armazena as regras que permitem estimar a parte do valor exportado atribuida
ao produto de interesse antes da normalizacao anual em `coeff`.

## Fluxo da camada

O fluxo implementado em `preparacao_camada_exportacao.py` e direto:

1. carregar os parametros de `parametros_coeficientes_exportacao.json`;
2. consultar a base de exportacao e obter `ano`, `id_ncm`,
   `nome_ncm_portugues`, `sigla_uf_ncm` e `valor_fob_dolar`;
3. filtrar os registros da UF alvo, hoje `PA`;
4. agregar os valores por ano, `id_ncm` e descricao NCM;
5. projetar a serie anual com previsao linear quando necessario;
6. distribuir os valores dos NCMs entre os produtos definidos no parametro,
   usando `id_ncm` como chave principal;
7. aplicar as `participacoes_especificas` quando houver regra explicita, como o
   `0.87` do acai;
8. converter o valor em dolar para real usando a taxa BRL/USD do ano;
9. calcular `coeff` como a participacao do produto no total do ano;
10. gravar o resultado em `br_coeficientes_exportacao.preparacao_camada_exportacao`.

## Execucao local com Postgres

Suba o banco local:

```bash
docker compose up -d postgres
```

Garanta que a raw contenha:

- `pa_me_comex_stat.ncm_exportacao`;
- `br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul`.

Quando essas tabelas ainda nao existirem, carregue primeiro os flows raw:

```bash
python -m dados.raw.pa_me_comex_stat.ncm_exportacao
python -m dados.raw.br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul
```

Depois gere a gold:

```bash
python -m dados.gold.br_coeficientes_exportacao.preparacao_camada_exportacao
```

O flow escreve no banco definido por `DB_GOLD_ZONE`; se essa variavel nao
existir, usa `DB_AGREGATED_ZONE`. Essa compatibilidade preserva a nomenclatura
historica do repositorio e a arquitetura de medalhoes documentada na raiz.

## Auditoria das series

Depois de carregar a tabela gold, gere o resumo e o grafico das series:

```bash
python -m dados.gold.br_coeficientes_exportacao.preparacao_camada_exportacao --auditoria-series
```

Os arquivos saem em `tmp_data/br_coeficientes_exportacao/output/`:

- `resumo_coeficientes.csv`;
- `series_coeficientes_exportacao.png`.

O grafico mostra as series de `coeff` dos principais produtos por participacao
media. A primeira leitura esperada e se as series somam 1 por ano, se existem
saltos provocados por mudanca de composicao NCM e se produtos amplos, como
preparacoes alimenticias, nao dominam a serie sem justificativa metodologica.

Para auditar os matches NCM-produto parametrizados:

```bash
python -m dados.gold.br_coeficientes_exportacao.preparacao_camada_exportacao --auditoria-matches
```

Essa auditoria nao executa algoritmo de descoberta textual nem gera CSVs. Ela
gera apenas `dados/gold/br_coeficientes_exportacao/resultados/relatorio_matches_ncm.xlsx`,
com uma aba `matches` e tres colunas: `produto`, `id_ncm` e `nome_ncm`. Esse
arquivo e a verificacao final do de-para consolidado no JSON.

## Testes

A bateria focada fica em `tests/test_export_coefficients.py` e valida:

- leitura do novo formato enriquecido de `composicao_produtos`;
- taxa de cambio anual carregada de CSV;
- compatibilidade dos parametros com `produtos_unicos_matrizes.json`, `NCM.csv`
  e `taxa_cambio.csv`;
- transformacao gold com filtro de UF, reparticao por participacao especifica,
  normalizacao anual de `coeff` e contrato pydantic;
- geracao do resumo/grafico de series e do XLSX sintetico de matches.

Execute:

```bash
python -m pytest dados/tests/test_export_coefficients.py
```

## Observacoes de manutencao

- A camada fica concentrada em dois arquivos Python: `preparacao_camada_exportacao.py`
  para o flow, schema e comandos operacionais; `utils.py` para transformacoes,
  distribuicao, exportacao JSON e geracao dos artefatos de auditoria.
- Se a metodologia para o acai mudar, o ajuste deve ser feito primeiro em
  `participacoes_especificas`.
- Se uma nova descricao de NCM passar a representar o produto, ela precisa
  entrar em `composicao_produtos` com `id_ncm` e `nome_ncm`; use `tipo_match`
  apenas quando a classificacao automatica por texto "Outros/Outras" precisar
  ser corrigida.
- NCMs genericos recorrentes so devem voltar ao JSON se houver criterio
  metodologico claro de reparticao ou participacao.
- Se a base passar a identificar o produto de forma direta e suficiente, a
  regra de estimacao por participacao media pode ser revista ou removida.
- A documentacao desta camada assume a nomenclatura NCM tal como aparece na base
  lida pela consulta de exportacao.

## Referencias

BENTES, E. dos S.; HOMMA, A. K. O.; SANTOS, C. A. N. dos. Exportacoes de polpa
de acai do estado do Para: situacao atual e perspectivas. In: CONGRESSO DA
SOCIEDADE BRASILEIRA DE ECONOMIA, ADMINISTRACAO E SOCIOLOGIA RURAL, 55., 2017,
Santa Maria, RS. Anais [...]. Santa Maria: UFSM, 2017. Disponivel em:
<https://www.embrapa.br/busca-de-publicacoes/-/publicacao/1074510/exportacoes-de-polpa-de-acai-do-estado-do-para-situacao-atual-e-perspectivas>.
Acesso em: 17 abr. 2026.

FUNDACAO AMAZONIA DE AMPARO A ESTUDOS E PESQUISAS DO PARA. Nota tecnica:
Conjuntura da economia do acai. [s.l.: s.n.], [s.d.]. Disponivel em:
<https://drive.google.com/file/d/1grGvWI6j2a0CPkhaf56_3XEj4_dWhyqT/view>.
Acesso em: 17 abr. 2026.

FUNDACAO AMAZONIA DE AMPARO A ESTUDOS E PESQUISAS DO PARA. O contexto economico
e ambiental do acai. [s.l.: s.n.], 2026. Disponivel em:
<https://portalamazonia.com/wp-content/uploads/2026/03/Nota-Tecnica-O-CONTEXTO-ECONOMICO-E-AMBIENTAL-DO-ACAI.VERSAO-PUBLICACAOdocx-1-1.pdf>.
Acesso em: 17 abr. 2026.
