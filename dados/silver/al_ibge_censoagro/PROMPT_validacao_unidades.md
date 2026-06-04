# Tarefa: validar e padronizar unidades de medida no silver do `al_ibge_censoagro` (Censo Agropecuário 2006 e 2017)

## Contexto
Já implementamos a validação/conversão de unidades de medida no silver do `al_ibge_pevs`
(extração vegetal) e replicamos o mesmo trabalho no `al_ibge_pam` (lavouras permanente e
temporária). Agora investigue **este mesmo problema** nas tabelas dos **censos de 2006 e
2017** do `al_ibge_censoagro` (Amazônia Legal).

Vários modelos pydantic do censo anotam `quantidade_produzida` (e `quantidade_vendida`,
`autoconsumo_*`, `comercio_*`) como `ton`, mas a tabela pode conter produtos que **não
estão em toneladas** — confirme e trate.

**Use como template a implementação já feita no PEVS** (mesma arquitetura, padrões e estilo):
- Raw: `dados/raw/al_ibge_pevs/extracao_vegetal.py` (enriquece `unidade_medida` por produto
  via metadados, **após o parse, dentro do fluxo** — não no parser compartilhado).
- Helper de metadados: `get_classificacao_unidades(agregado, classificacao_id)` em
  `dados/raw/utils/ibge_api_crawler.py` (REUSE — retorna `{id_categoria: unidade}`).
- Conversão dirigida por unidade: `pevs_volume_to_weight` em
  `dados/utils/agricultural_conversions.py` (m³ → t por máscara em `unidade_medida`).
- Constante de fator: `PEVS_DENSIDADE_TON_M3 = 0.5` em `dados/silver/constants/produtos.py`.
- Silver de referência: `dados/silver/al_ibge_pevs/extracao_vegetal.py` (carrega `unidade_medida`
  no SQL pegando a unidade da linha de **quantidade**; converte no `transform`; coage
  `NaN`/`''`/`'NaN'` → `None` no `validate`).
- Documentação: `dados/silver/al_ibge_pevs/readme.md`.
- Regras do projeto: `CLAUDE.md` e `REFACTORING.md` (camadas raw→silver→gold, schema pydantic
  obrigatório com `description`/`unit`, `get_logger`, `PostgresETL`).

## ⚠️ Diferença essencial em relação ao PAM
O problema de unidade no PAM tinha **duas naturezas**: (1) por produto (estática) e
(2) **temporal** (frutas em "mil frutos"/"mil cachos" **antes de 2001**, tratada por
`products_weight_ratio_fix`).

**No censo, a frente temporal NÃO se aplica.** O Censo Agropecuário é pontual: só há os
anos **2006 e 2017**, ambos **pós-2001**. Logo:
- **NÃO** ligue/chame `products_weight_ratio_fix` aqui — não há período pré-2001 nessas
  tabelas. (Confirme no diagnóstico que `ano ∈ {2006, 2017}`.)
- O que **resta tratar** é a unidade **por produto (estática)**, como no PEVS: a unidade
  correta por produto está nos metadados do agregado (classificações 229 / 227 / 226).
  Em extração vegetal isso inclui produtos madeireiros em **m³** (lenha, madeira em tora,
  carvão etc.) que precisam ir para **toneladas** (× `PEVS_DENSIDADE_TON_M3`).

## Fatos dos fluxos do censo (ancore aqui)
Tabelas **com quantidade produzida** (sujeitas ao problema de unidade). Agrupadas pela
classificação de produto (o `id_produto`/categoria que casa com os metadados):

| Tema | Classif. produto | Dicionário de produtos | Tabela 2006 (AGREGADO) | Tabela 2017 (AGREGADO) |
|---|---|---|---|---|
| Extração vegetal | `229` | `dicionario_produtos_censo_6949_2233` | `tbl_2233_2006` (**2233**) | `tbl_6949_2017` (**6949**) |
| Lavoura permanente | `227` | `dicionario_produtos_censo_6955_2518` | `tbl_2518_2006` (**2518**) | `tbl_6955_2017` (**6955**) |
| Lavoura temporária | `226` | `dicionario_produtos_censo_6957_2337` | `tbl_2284_2006` (**2284**), `tbl_2337_2006` (AGREGADO **2237**) | `tbl_6957_2017` (**6957**) |

> Atenção: `tbl_2337_2006` tem `AGREGADO = "2237"` (o nome do arquivo não bate com o
> agregado); use o valor real de cada `*.py`. O `ID_PRODUTO_CLASSIFICACAO` já está
> declarado em cada fluxo raw — reutilize, não chumbe.

Modelos: `dados/silver/al_ibge_censoagro/models.py`. As métricas de quantidade vivem
sobretudo nas bases compartilhadas `_Censo2233Like` (→ `tbl_2233`, `tbl_2518`),
`_Censo2017ProdutoLike` (→ `tbl_6949`, `tbl_6955`, `tbl_6957`) e em
`AlIbgeCensoagroTbl22842006` / `AlIbgeCensoagroTbl23372006`. Adicionar `unidade_medida`
nas bases propaga para as filhas.

**Tabelas SEM quantidade produzida — fora do escopo** (só contagem de estabelecimentos e
valores em BRL; não há unidade física a tratar): `tbl_1909_2006`, `tbl_1931_2006`,
`tbl_2782_2006`, `tbl_6885_2017`, `tbl_6898_2017`, `tbl_6899_2017`. Confirme no diagnóstico
e **não as altere**.

O `COLUMNS_DDL` do raw **já tem** a coluna `unidade_medida`, mas hoje o `parse_agrocenso_json`
e o `parse_agrocenso_destinacao` (`dados/raw/al_ibge_censoagro/utils.py`) preenchem com
`var_unit` — a unidade da **variável** (genérica, ex. o texto "Vide categorias…"), não por
produto. Metadados: `https://servicodados.ibge.gov.br/api/v3/agregados/{AGREGADO}/metadados`.

## Tarefas

### 1. Diagnóstico (faça antes de codar; reporte os achados)
- Conecte no banco raw (`PostgresETL`, var `DB_RAW_ZONE` do `.env`) e inspecione
  `unidade_medida` por produto nas 7 tabelas com quantidade. Está vazia/genérica por produto?
- Confirme que `ano` só assume `2006`/`2017` em cada tabela (justifica não usar a frente
  temporal de 2001).
- Extraia o mapa `{id_produto: unidade}` dos metadados de cada agregado/classificação
  (229, 227, 226 — para 2006 e 2017). Liste as unidades distintas e quais produtos fogem
  de `Toneladas` (espera-se m³ em extração vegetal; verifique se há outras).
- Compare as unidades por produto entre 2006 e 2017 (mesma classificação) — devem coincidir;
  registre divergências.

### 2. Implementação (espelhe o PEVS)
**Raw** (as 7 tabelas com quantidade — `tbl_2233_2006`, `tbl_2284_2006`, `tbl_2337_2006`,
`tbl_2518_2006`, `tbl_6949_2017`, `tbl_6955_2017`, `tbl_6957_2017`):
- Após o parse, preencha `unidade_medida` por produto **apenas nas linhas da variável de
  quantidade produzida** usando
  `get_classificacao_unidades(AGREGADO, ID_PRODUTO_CLASSIFICACAO)`, via `map` por `id_produto`.
  Produtos sem unidade → `None` (NULL), não a string `"NaN"` (use o padrão `.where(notna, None)`
  do PEVS).
- **Não altere** `parse_agrocenso_json`/`parse_agrocenso_destinacao` (compartilhados) — faça
  o enriquecimento no fluxo, como no PEVS. Como o passo se repete em 7 fluxos, considere um
  pequeno helper compartilhado (ex. em `dados/raw/al_ibge_censoagro/utils.py` ou um
  `_common.py` do raw) que receba `df`, `agregado`, `id_classificacao` e o id da variável de
  quantidade — mantendo o estilo e sem duplicar 7×.

**Silver** (os 7 fluxos correspondentes):
- Carregue `unidade_medida` no SQL de `extract()` pegando a unidade da **linha de quantidade
  produzida** (mesmo padrão do PEVS:
  `MAX(CASE WHEN nome_variavel = '<variável de quantidade>' THEN unidade_medida END)`),
  usando exatamente o `nome_variavel` de quantidade já presente em cada `extract()`.
- No `transform()`, **após** `fix_ibge_digits` (e, onde houver, **antes** de
  `calcula_autoconsumo_comercio`, para que autoconsumo/comércio sejam derivados já em toneladas):
  - Converta as quantidades em m³ → toneladas de forma **data-driven pela coluna
    `unidade_medida`**. Reutilize `pevs_volume_to_weight` quando a métrica for
    `quantidade_produzida`; para as demais colunas de quantidade física do censo
    (`quantidade_vendida`, `autoconsumo_quantidade_*`, `comercio_quantidade_*`) generalize o
    helper para aceitar a coluna alvo (`qty_col`) ou crie um equivalente, **sem chumbar lista
    de produtos**. Após converter, a `unidade_medida` efetiva passa a `Toneladas`.
  - Se o diagnóstico revelar unidades não-tonelada além de m³, trate-as também de forma
    data-driven (novo fator/constante em `dados/silver/constants/produtos.py`, no estilo de
    `PEVS_DENSIDADE_TON_M3`). Contagens (se houver, ex. "Mil …") **não** se convertem —
    permanecem com sua `unidade_medida`.
  - **Não** chame `products_weight_ratio_fix` (ver diferença essencial acima).
- Adicione o campo `unidade_medida: str | None` aos modelos em
  `dados/silver/al_ibge_censoagro/models.py` (com `description` e `unit: "code"`),
  preferencialmente nas bases `_Censo2233Like` e `_Censo2017ProdutoLike` (e em
  `AlIbgeCensoagroTbl22842006` / `AlIbgeCensoagroTbl23372006`). Atualize as
  `description`/`unit` das métricas de quantidade para refletir o tratamento.
  `unidade_medida` deve refletir a unidade **efetiva** do valor armazenado.
- No `validate()`, coaja `unidade_medida` `NaN`/`''`/`'NaN'` → `None` (como no PEVS). Mantenha
  o uso dos helpers compartilhados de `_common.py` (`assert_pk_unique`, `coerce_decimals`,
  `download_raw`, `write_silver`).

### 3. Execução e verificação
- Rode `uv run python -m dados.raw.al_ibge_censoagro.<tabela>` (re-crawl usa BigQuery via
  `bd.read_sql` + API IBGE) e depois `uv run python -m dados.silver.al_ibge_censoagro.<tabela>`
  para as 7 tabelas afetadas.
- Verifique: distribuição de `unidade_medida` na silver (por tabela); uma amostra de produto
  madeireiro de extração vegetal (ex. lenha/madeira em tora) com a conversão m³ → t aplicada
  (compare antes/depois ≈ ×0,5); produtos já em toneladas inalterados; coerência entre 2006 e
  2017; sem PK duplicada; `ruff check` limpo.
- Se houver fluxo gold dependente do censo, confirme a propagação.

### 4. Documentação
- Crie `dados/silver/al_ibge_censoagro/readme.md` no mesmo formato do
  `dados/silver/al_ibge_pevs/readme.md`: diretrizes de unidade (por produto), o motivo de a
  frente temporal de 2001 **não** se aplicar ao censo, validações, contraste PEVS x PAM x Censo,
  e valores-âncora para conferência.

## Critério de aceitação
- `unidade_medida` ingerida no raw **por produto** (linha de quantidade) e presente no silver
  das 7 tabelas com produção; as 6 tabelas de contagem/valor permanecem intactas.
- Quantidades em m³ (e quaisquer outras não-tonelada achadas) convertidas para toneladas de
  forma data-driven pela `unidade_medida`; `products_weight_ratio_fix` **não** utilizada.
- Modelos pydantic com unidades coerentes; `transform()` data-driven pela unidade.
- Diagnóstico, verificação numérica e readme entregues. `ruff` limpo.
