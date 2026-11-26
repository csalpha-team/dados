# Atualização 2024 — pendências, problemas e correções

Registro da atualização das séries anuais feita em novembro de 2026 (PR
[#31](https://github.com/csalpha-team/dados/pull/31), branch
`feat/atualizacao-2024`). Documenta o que foi encontrado pelo caminho, o que
foi corrigido e o que ficou em aberto.

Escopo combinado: atualizar as séries anuais; **censo 2006/2017 e POF ficam
intocados**.

---

## 1. Cobertura antes e depois

Diferença medida contra o estado do banco no início do trabalho. Nenhum
`min(ano)` subiu e nenhuma contagem de linhas caiu — não houve regressão.

| Série | Antes | Depois |
|---|---|---|
| PAM permanente / temporária | 1974–2024 | **1974–2025** |
| PEVS | 1986–2024 | **1986–2025** |
| PPM efetivo dos rebanhos | só raw | **raw + silver + gold, 1974–2025** |
| PPM origem animal / aquicultura | não existiam | **raw + silver** |
| PIA-Empresa (série antiga) | 2007–2022 | **2007–2023** |
| PAC / PAS / PIA 2024 | — | **série nova em raw + silver + gold** |
| RAIS | só raw, 2006–2024 | **silver, 2006–2025** |
| IPCA (SGS 433) | tabela inexistente | **raw + silver, 1995–2026** |
| Comex Stat (PA) | 1997–2025 | **1997–2026** |
| Coeficientes de renda | 1995–2023, preços de 2022 | **1995–2024, preços de 2024** |
| Censo 2006/2017, POF | — | intocados, como combinado |

PAM, PEVS e PPM passaram de 2024 porque o IBGE já publicou 2025 para essas
pesquisas; os flows usam `PERIODOS="all"` e acompanham a fonte.

---

## 2. Problemas encontrados

### 2.1 Ruptura das séries de PAC, PAS e PIA em 2024

O IBGE rompeu as três séries no ano de referência 2024 — mudança na
classificação de empresas inativas e migração do Cempre da RAIS para o eSocial
(Notas Técnicas PAC 01/2026, PAS 01/2026 e PIA-Empresa 01/2026). Os dados de
2024 saem em **agregados novos**, não como um período a mais nas tabelas
antigas, e o próprio IBGE afirma que as séries não são diretamente comparáveis.

| Pesquisa | Série antiga (até 2023) | Série nova (2024–) | Divulgação |
|---|---|---|---|
| PAC | 1407 | 10653 | 29/07/2026 |
| PAS | 2715 | 10762 | 27/08/2026 |
| PIA-Empresa (divisões) | 1849 | 10457 | 24/06/2026 |
| PIA-Empresa (divisões e grupos) | 1848 | 10454 | 24/06/2026 |

Mudanças estruturais, além da numeração:

- **PAC e PAS**: a geografia deixa de ser classificação (12354) e passa a vir
  em `localidades` (N1/N2/N3).
- **PAS**: a variável de salários muda de 673 para 857.
- **PIA**: `706` ("Número de unidades locais") vira `13816` e entra `2086`
  ("Número de empresas com 5 ou mais pessoas ocupadas").

### 2.2 PIA estava dois anos atrasada

As tabelas 1848/1849 paravam em 2022, mas a PIA 2023 havia sido divulgada em
25/06/2025. Era lacuna de ingestão, não da fonte.

### 2.3 Renumeração dos produtos da PEVS e produtos novos na PAM

Na safra 2025 o IBGE inseriu produtos na classificação 193 e **renumerou os
rótulos** — `Castanha-de-caju`, por exemplo, passou de `1.2` para `1.5`. Como
`dicionario_produtos_pevs` era chaveado pelo rótulo completo, o mapeamento
quebrou em silêncio: produto sem chave virava `NaN` e todas essas linhas
colidiam na PK, derrubando o silver com **834.840 duplicatas** na PEVS e
**120.588** na PAM permanente.

Produtos sem mapeamento no momento da quebra: 27 na PEVS, 3 na PAM permanente
(Acerola, Cupuaçu, Graviola) e 11 na PAM temporária (olerícolas e Canola,
Gergelim, Milho verde).

### 2.4 Estouro de memória ao recarregar as tabelas grandes

`PostgresETL.load_data` convertia o dataframe inteiro em lista de dicionários
(`to_dict("records")`) antes de inserir; `download_data` fazia o equivalente na
leitura, com `RealDictCursor` + `fetchall`; e a validação pydantic repetia o
padrão. Com 13,5 milhões de linhas na PAM, numa máquina de 7 GB, o consumo
inviabilizava a recarga.

### 2.5 Bugs pré-existentes (commit d0ddf59, outubro/2026)

1. `dados/gold/br_coeficientes_renda/utils.py` chamava
   `_aplicar_tolerancia_crescimento_anual` sem o argumento obrigatório
   `observed_years` — o flow de renda estava quebrado em `main` desde então.
2. `dados/export/dump_gold_l2.py` selecionava a coluna `valor` em
   `pa_coeficientes_custo.preparacao_camada_custo` e em
   `br_coeficientes_consumo.preparacao_camada_consumo`, onde o gold
   materializa `coeff` — o export da Layer 2 falhava.
3. `dados/raw/br_ibge_pia/utils.py` usava `print`, contra a regra do
   `CLAUDE.md`.

### 2.6 Inconsistências de documentação e configuração

- `CLAUDE.md` e `README.md` apontavam para `REFACTORING.md`, que sumiu da
  árvore no merge 5607712. A cópia arquivada (`git show 4d6a34c:REFACTORING.md`)
  está desatualizada: fala em dbt, que o projeto não usa, e aponta models em
  `dados/<zone>/models/<dataset_id>.py`, caminho que mudou.
- A documentação citava `DB_AGREGATED_ZONE` para a zona gold, mas código e
  `.env` usam `DB_GOLD_ZONE`.
- `README.md` listava `al_ibge_paic/`, diretório que não existe, e omitia
  `br_bcb_sgs_ipca/`.

---

## 3. Correções e ajustes feitos

### 3.1 Séries novas de 2024, lado a lado com as antigas

Nada de histórico foi sobrescrito: cada série tem a sua tabela, em todas as
zonas, e o silver carrega uma coluna `serie` para o gold nunca misturar as duas
sem intenção.

| Zona | Série antiga | Série nova |
|---|---|---|
| raw | `tbl_1407`, `tbl_2715`, `tbl_1848`, `tbl_1849` | `tbl_10653`, `tbl_10762`, `tbl_10454`, `tbl_10457` |
| silver | `tbl_1407`, `tbl_2715`, `tbl_1849` | `tbl_10653`, `tbl_10762`, `tbl_10457` |
| gold | `pac_comercio`, `pas_servicos`, `pia_industrias` | as mesmas com sufixo `_serie_nova` |

Arquivos novos de apoio: `dados/raw/utils/agregados_parser.py` (parser do
formato da série nova, com geografia em `localidades`) e um `README.md` por
dataset afetado (`dados/raw/al_ibge_pac/`, `br_ibge_pas/`, `br_ibge_pia/`)
documentando a ruptura.

A **exceção deliberada** é `br_coeficientes_renda`, que lê as duas séries com
`UNION ALL` para 2024 entrar como ano observado em vez de projetado. Isso é
seguro porque os coeficientes são razões calculadas dentro de cada ano
(produção ou salário por pessoa ocupada): a ruptura de nível não atravessa a
razão. Os valores confirmam a continuidade — `AFIndustBenef` vai de 51,7 em
2023 para 52,2 em 2024. Quem precisar dos **níveis** por série deve usar as
tabelas `_serie_nova`.

### 3.2 Camadas que faltavam

- **PPM**: silver para `efetivo_rebanhos`, `producao_origem_animal` e
  `producao_aquicultura`, mais o indexador gold
  `pa_indexadores_producao_rural.efetivo_rebanhos_ppm`.
- **RAIS**: silver `pa_rf_rais.up_rais` com contrato pydantic.
- **IPCA**: raw e silver materializados (a camada de renda depende deles e as
  tabelas não existiam no banco).
- Os flows raw de `producao_origem_animal` e `producao_aquicultura` passaram a
  enriquecer a unidade por produto com `get_classificacao_unidades`, como a
  PEVS já fazia — antes gravavam `unidade_medida` vazia.

### 3.3 Rebase dos coeficientes de renda para 2024

`ano_ancora` passou de 2022 para 2024 em
`parametros_coeficientes_renda.json`, com os defaults correspondentes em
`utils.py` e `correcao_monetaria.py`, e as unidades declaradas nos models
passaram a `thousand_BRL_2024_per_worker_year`. **Todos os valores mudam de
escala** (cerca de 9%): comparações com números antigos exigem regenerar os
benchmarks.

### 3.4 Vocabulário de produtos

- `dicionario_produtos_pevs` **rechaveado por `id_produto`** (categoria da
  classificação 193, estável), com o rótulo vigente em comentário ao lado de
  cada entrada.
- **24 produtos novos** incluídos nos dicionários de PAM e PEVS, seguindo a
  regra que mantém a unificação com o censo: se o censo coleta a mesma
  espécie/parte, reusa-se exatamente o nome padronizado do dicionário do censo;
  só quando não há par é que se cria nome novo.

| Fonte | Novos | Casam com o censo |
|---|---|---|
| PEVS | 10 | 9 — `baru-amêndoa`, `buriti-coco`, `cupuaçu`, `juçara-fruto`, `macaúba-fruto`, `pupunha-coco`, `tucumã`, `andiroba-semente`, `murumuru-semente` |
| PAM permanente | 3 | 3 — `acerola`, `cupuaçu`, `graviola` |
| PAM temporária | 11 | 3 — `abóbora`, `colza-canola`, `gergelim-semente` |

Sem par no censo: `cajá-fruto` (PEVS) e as olerícolas da PAM temporária
(alface, cenoura, chuchu, inhame, milho verde, morango, pimentão, repolho), que
o Censo Agro apura em tabela própria, fora de 6957/2337.

- Os flows de PAM e PEVS agora **falham com a lista de produtos sem
  mapeamento**, em vez de produzir `NaN` e estourar em PK duplicada muito
  depois da causa.

### 3.5 Desempenho e memória

| Mudança | Onde |
|---|---|
| `load_data` materializa uma fatia por vez e aceita `max_workers` (uma conexão por thread) | `dados/raw/utils/postgres_interactions.py` |
| `download_data` usa cursor do lado do servidor e monta o dataframe por lotes | idem |
| `validate_rows` e `parallel_map` fatiam validação pydantic e parse de JSON em threads | `dados/utils/parallel.py` (novo) |
| `currency_fix_series`, versão vetorizada de `currency_fix` | `dados/utils/agricultural_conversions.py` |

Medições (máquina de 8 CPUs, 7 GB):

| Flow | Antes | Depois |
|---|---|---|
| raw PAM permanente (13,5M linhas) | ~25min | **14min03**, pico 2,9 GB |
| raw PAM temporária | ~25min | **14min01**, pico 3,2 GB |
| silver PAM permanente | 8min39 | **4min16** |
| silver PAM temporária | 7min42 | **5min26** |
| silver PEVS | — | 1min40, pico 1,4 GB |
| silver PPM efetivo | 40s | **14s**, pico 393 MB |

A versão vetorizada de `currency_fix` foi conferida contra o caminho linha a
linha em todas as faixas de moeda, com resultado idêntico.

### 3.6 Documentação e operação

- Os módulos novos foram registrados em `scripts/run_all_raws.sh`,
  `run_all_silvers.sh` e `run_all_golds.sh`.
- `README.md` ganhou a seção "Datasets grandes (PAM, PEVS, PPM)" e a nota sobre
  as rupturas de série; as listas de datasets por zona foram corrigidas.
- `CLAUDE.md` e `README.md` deixaram de apontar para o `REFACTORING.md`
  inexistente e passaram a usar `DB_GOLD_ZONE`.

---

## 4. Pendências

### 4.1 Decisões em aberto

1. **Pico de memória do silver da PAM (~5,7 GB).** Depois das otimizações, o
   que resta vem da conversão das 8 colunas numéricas para `Decimal` antes da
   validação e da carga — 1,7 milhão de linhas × 8 colunas de objetos Python.
   Reduzir exige validar e carregar em fatias, convertendo cada fatia na hora
   (preserva a precisão, custa um refactor do contrato `validate`/`load`), ou
   manter `float64` até a carga (simples, mas altera a semântica numérica de
   valores monetários). Não foi alterado por conta própria.
2. **Três nomes da PEVS alinhados ao censo**: `Buriti (fruto)`→`buriti-coco`,
   `Pupunha (fruto)`→`pupunha-coco`, `Andiroba (amêndoa)`→`andiroba-semente`.
   Seguem a mesma lógica do `Umbu (fruto)`→`umbu` que já existia, mas o rótulo
   final não é o que a PEVS usa. Reverter são três linhas em
   `dados/silver/constants/produtos.py` mais o reprocessamento de PEVS.
3. **Tabela órfã** `raw_zone.al_me_comex_stat.ncm_exportacao`: 23 linhas de
   2023, nenhum flow no repositório a alimenta (a versão do Pará está completa).
   Candidata a remoção.
4. **Arquivos reformatados fora do escopo**: o `pre-commit` aplicou
   `ruff-format`/`yamlfix` em arquivos não relacionados à atualização. São
   mudanças cosméticas que os hooks fariam em qualquer commit.

### 4.2 Problemas conhecidos, não corrigidos

- **Unidade monetária da PAS rotulada como `BRL`** nos models antigos
  (`dados/silver/br_ibge_pas/models.py` e o gold correspondente), enquanto o
  IBGE publica em Mil Reais. Os models da série nova declaram `thousand_BRL`.
  Corrigir os antigos é mudança de contrato e merece passo próprio.
- **Benchmarks desatualizados**: `benchmark/` e `comparacao_benchmark/` ainda
  refletem a âncora de 2022. Precisam ser regenerados
  (`scripts/gerar_diagnostico_correcao_renda.py`) antes de qualquer comparação
  de nível com a renda nova.
- **Tabelas gold duplicadas no banco**: `pa_servicos_industria_comercio` tem
  `pia_industria` (2007–2022, sem flow correspondente) além de
  `pia_industrias`; e `br_despesas_familiares` aparece também como
  `brasil_despesas_familiares`. Falta decidir o que é canônico.
- **PIA 10454 não tem silver**, por paridade com a 1848, que também não tem.

### 4.3 Operação

- As séries novas de PAC/PAS/PIA hoje têm `PERIODOS` fixo em `2024`. Quando o
  IBGE publicar 2025, basta acrescentar o período na constante do módulo raw
  correspondente.
- A PEVS pode renumerar rótulos de novo a cada safra; como o dicionário agora é
  chaveado por id, isso deixou de ser um risco silencioso. Produto realmente
  novo continua exigindo entrada no dicionário, e o flow avisa qual é.

---

## 5. Como revalidar

```bash
# cobertura por tabela em cada zona
export PGPASSWORD=$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2-)
psql -h localhost -U postgres -d raw_zone -c "
  SELECT table_schema, table_name FROM information_schema.columns
  WHERE column_name = 'ano' ORDER BY 1, 2;"

# pipeline completo
bash scripts/run_all_raws.sh && bash scripts/run_all_silvers.sh && bash scripts/run_all_golds.sh

# validação cruzada com as âncoras censitárias (2006, 2017)
uv run python -m dados.gold.pa_indexadores_producao_rural.validacao_series

# pacotes consumidos pelos algoritmos
uv run python -m dados.export.dump_gold_l1
uv run python -m dados.export.dump_gold_l2

uv run pytest && uv run pre-commit run --all-files
```

Estado na entrega: `pytest` 34/34, `pre-commit` passando, `validacao_series`
com 69.756 linhas em 6 pesquisas, e os dois pacotes de export gerados
(`gold_export_l1.zip` com 13 arquivos; `gold_export/layer2_new_values.zip` com
6).
