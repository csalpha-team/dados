# Tarefa: validar e padronizar unidades de medida no silver do `al_ibge_pam` (lavouras permanente e temporária)

## Contexto
Acabamos de implementar a validação/conversão de unidades de medida no silver do
`al_ibge_pevs` (extração vegetal). Agora replique o mesmo trabalho para o `al_ibge_pam`
(Produção Agrícola Municipal), nas duas tabelas: **lavoura permanente** e **lavoura temporária**.

O modelo pydantic do PAM anota `quantidade_produzida` como `ton` e `rendimento_medio_producao`
como `kg`, mas a tabela pode conter linhas que **não estão nessas unidades** — confirme e trate.

**Use como template a implementação já feita no PEVS** (mesma arquitetura, padrões e estilo):
- Raw: `dados/raw/al_ibge_pevs/extracao_vegetal.py` (ingestão de unidade via metadados)
- Helper de metadados: `get_classificacao_unidades` em `dados/raw/utils/ibge_api_crawler.py` (REUSE)
- Conversão dirigida por unidade: `pevs_volume_to_weight` em `dados/utils/agricultural_conversions.py`
- Constante de fator: `PEVS_DENSIDADE_TON_M3` em `dados/silver/constants/produtos.py`
- Documentação: `dados/silver/al_ibge_pevs/readme.md`
- Regras do projeto: `CLAUDE.md` e `REFACTORING.md` (camadas raw→silver→gold, schema pydantic
  obrigatório com `description`/`unit`, `get_logger`, `PostgresETL`).

## Fatos dos fluxos PAM (ancore aqui)
| | Lavoura permanente | Lavoura temporária |
|---|---|---|
| Raw | `dados/raw/al_ibge_pam/lavoura_permanente.py` | `dados/raw/al_ibge_pam/lavoura_temporaria.py` |
| Silver | `dados/silver/al_ibge_pam/lavoura_permanente.py` | `dados/silver/al_ibge_pam/lavoura_temporaria.py` |
| AGREGADO | `1613` | `1612` |
| CLASSIFICACAO / id_produto | `82` | `81` |
| Tabela (raw e silver) | `al_ibge_pam.lavoura_permanente` | `al_ibge_pam.lavoura_temporaria` |
| Dicionário de produtos | `dicionario_produtos_pam_permanente` | `dicionario_produtos_pam_temporaria` |
| Coluna de área específica | `area_destinada_colheita` | `area_plantada` |

- Modelos: `dados/silver/al_ibge_pam/models.py` (`_PamBase`, `AlIbgePamLavouraPermanente`,
  `AlIbgePamLavouraTemporaria`). Métricas: `quantidade_produzida`, `valor_producao`,
  `area_colhida`, `rendimento_medio_producao` (+ a área específica de cada tabela).
- O `COLUMNS_DDL` do raw **já tem** a coluna `unidade_medida`, mas hoje o `parse_pam_json`
  (`dados/raw/al_ibge_pam/utils.py`) preenche com a unidade da **variável** (genérica),
  não por produto. Metadados: `https://servicodados.ibge.gov.br/api/v3/agregados/{AGREGADO}/metadados`.

## ⚠️ Diferença essencial em relação ao PEVS
O problema de unidade no PAM tem **duas naturezas**:

1. **Por produto (estática)** — como no PEVS: a unidade correta por produto está nos metadados
   do agregado (classificação 82 / 81). Ingira no raw via `get_classificacao_unidades`.
2. **Temporal (por ano)** — específica do PAM: **antes de 2001**, várias frutas eram medidas
   em **"mil frutos"** (e banana em **"mil cachos"**), com rendimento em frutos/ha — de 2001
   em diante passaram a toneladas e kg/ha. Já existe a função
   **`products_weight_ratio_fix`** (`dados/utils/agricultural_conversions.py`) que faz essa
   conversão fruto→tonelada e recalcula o rendimento, **mas ela NÃO é chamada em nenhum fluxo
   silver** (está inerte). Referência IBGE:
   `https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf`.

O endpoint de metadados reflete só a unidade **atual** (pós-2001), então ele sozinho NÃO
resolve a frente temporal — as duas precisam ser tratadas.

## Tarefas

### 1. Diagnóstico (faça antes de codar; reporte os achados)
- Conecte no banco silver/raw (`PostgresETL`, vars `DB_RAW_ZONE`/`DB_SILVER_ZONE` do `.env`)
  e inspecione `unidade_medida` por produto nas duas tabelas raw. Está vazia/genérica por produto?
- Extraia o mapa `{id_produto: unidade}` dos metadados dos agregados 1613 (classif. 82) e
  1612 (classif. 81). Liste as unidades distintas e quais produtos fogem de `Toneladas`.
- Identifique os produtos sujeitos à mudança de unidade de 2001 (cruze com
  `products_weight_ratio_fix`/`DICIONARIO_DE_PROPORCOES`). Confirme na doc do IBGE.
- Verifique a unidade de `rendimento_medio_producao` (kg/ha atual vs frutos/ha pré-2001).

### 2. Implementação (espelhe o PEVS)
**Raw** (`lavoura_permanente.py` e `lavoura_temporaria.py`):
- Após o parse, preencha `unidade_medida` por produto (apenas linhas da variável de
  **quantidade produzida**) usando `get_classificacao_unidades(AGREGADO, ID_PRODUTO_CLASSIFICACAO)`,
  via join por `id_produto`. Produtos sem unidade → `None` (NULL), não a string "NaN".
- Não altere `parse_pam_json` (compartilhado) — faça o enriquecimento no fluxo, como no PEVS.

**Silver** (ambos os fluxos):
- Carregue `unidade_medida` no SQL de `extract()` (pegue a unidade da linha de quantidade).
- No `transform()`, **após** `fix_ibge_digits` e **antes** de `currency_fix`:
  - Aplique `products_weight_ratio_fix` (ligue a função hoje inerte) para converter as
    frutas pré-2001 de mil frutos/cachos → toneladas e recalcular o rendimento. Atenção:
    essa função é row-wise (`df.apply(..., axis=1)`) e usa `quantidade_produzida`,
    `area_colhida`, `produto`, `ano` — confirme que essas colunas existem e estão numéricas
    nesse ponto. Note que o `DICIONARIO_DE_PROPORCOES` usa nomes **originais** do IBGE
    (ex. "Banana (cacho)"); decida se aplica antes ou depois do `map` de padronização e
    ajuste as chaves de forma consistente.
  - Se o diagnóstico revelar produtos com unidade não-tonelada nos metadados (análogo ao m³
    do PEVS), trate de forma data-driven pela coluna `unidade_medida`, criando um helper
    no estilo de `pevs_volume_to_weight` e a constante de fator correspondente em
    `dados/silver/constants/produtos.py`.
- Adicione o campo `unidade_medida: str | None` aos modelos em
  `dados/silver/al_ibge_pam/models.py` (com `description` e `unit: "code"`), e atualize as
  `description`/`unit` de `quantidade_produzida` e `rendimento_medio_producao` para refletir
  o tratamento. `unidade_medida` deve refletir a unidade **efetiva** do valor armazenado.
- No `validate()`, coaja `unidade_medida` `NaN`/`''`/`'NaN'` → `None` (como no PEVS).

### 3. Execução e verificação
- Rode `uv run python -m dados.raw.al_ibge_pam.<tabela>` (re-crawl usa BigQuery via
  `bd.read_sql` + API IBGE) e depois `uv run python -m dados.silver.al_ibge_pam.<tabela>`.
- Verifique: distribuição de `unidade_medida` na silver; uma amostra de fruta **pré-2001**
  com a conversão aplicada (compare quantidade e rendimento antes/depois); produtos não
  afetados inalterados; sem PK duplicada; `ruff check` limpo.
- Se houver fluxo gold dependente, confirme a propagação.

### 4. Documentação
- Crie `dados/silver/al_ibge_pam/readme.md` no mesmo formato do
  `dados/silver/al_ibge_pevs/readme.md`: diretrizes de unidade (por produto + temporal),
  validações, contraste PEVS x PAM, e valores-âncora para conferência.

## Critério de aceitação
- `unidade_medida` ingerida no raw por produto e presente no silver das duas tabelas.
- Frutas pré-2001 convertidas para toneladas (e rendimento em kg/ha), via
  `products_weight_ratio_fix` agora efetivamente aplicada.
- Modelos pydantic com unidades coerentes; `transform()` data-driven pela unidade.
- Diagnóstico, verificação numérica e readme entregues. `ruff` limpo.
