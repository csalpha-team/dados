# Sobre o repositório

Este repositório implementa a camada de dados do projeto **Contas Alfa**: uma
arquitetura de zonas (raw → silver → gold) sobre PostgreSQL, em que cada zona
tem um banco dedicado, um contrato de entrada/saída próprio e um conjunto fixo
de utilitários compartilhados. O objetivo é entregar, no final do pipeline, um
conjunto de coeficientes e indexadores prontos para alimentar o
[algoritmo csalpha](https://github.com/csalpha-team/csalpha) (Layer 2).

A especificação executiva dos pipelines vive em
[`REFACTORING.md`](./REFACTORING.md); este documento descreve a **estrutura
atual do repositório por zona**.

---

## 1. Arquitetura de zonas

```
+----------+     +-----------+     +-------------+     +-----------+
|  FONTES  |     |   ZONA    |     |    ZONA     |     |   ZONA    |
| EXTERNAS | --> |   RAW     | --> |   SILVER    | --> |   GOLD    | --> export/
+----------+     +-----------+     +-------------+     +-----------+      algoritmo
```

| Zona   | Variável de ambiente | Diretório fonte     | Pydantic | Consumidores permitidos |
|--------|----------------------|---------------------|----------|--------------------------|
| raw    | `DB_RAW_ZONE`        | `dados/raw/`        | opcional | silver                   |
| silver | `DB_SILVER_ZONE`     | `dados/silver/`     | **obrigatório** | silver, gold       |
| gold   | `DB_AGREGATED_ZONE`  | `dados/gold/`       | **obrigatório** | export / algoritmo |

Regra de camadas estrita: **raw → silver → gold**. Não existe gold → gold,
nem gold → raw, nem silver → gold (silver não lê gold).

---

## 2. Estrutura comum a todas as zonas

Todo script de dataset segue o mesmo esqueleto

```python
def extract()  -> pd.DataFrame: ...   # leitura da fonte (API, BQ, raw, silver)
def validate(df) -> pd.DataFrame: ... # checagem de contrato (pydantic em silver/gold)
def transform(df) -> pd.DataFrame: ... # limpeza/cálculos por linha
def load(df)   -> None:           ... # escrita via PostgresETL

def flow() -> None:
    log.info("flow.start")
    df = extract();     log.info("extract.done",   rows=len(df))
    df = validate(df);  log.info("validate.done",  rows=len(df))
    df = transform(df); log.info("transform.done", rows=len(df))
    load(df);           log.info("load.done",      rows=len(df))
    log.info("flow.end", rows=len(df))
```

Executar um flow:

```bash
uv run python -m dados.<zona>.<dataset_id>.<tabela>
```

### Utilitários compartilhados

| Função / classe                 | Localização                                   | Quando usar                                              |
|---------------------------------|-----------------------------------------------|----------------------------------------------------------|
| `PostgresETL`                   | `dados/raw/utils/postgres_interactions.py`    | **Único** caminho para I/O em Postgres (todas as zonas). |
| `get_logger(dataset_id, zone)`  | `dados/utils/logging.py`                      | Logger loguru padrão; sink stdout + arquivo rotacionado em `logs/<zone>/<dataset_id>.log`. |
| `tmp_dir(dataset_id, kind)`     | `dados/utils/paths.py`                        | Resolve `tmp_data/<dataset_id>/{input,output}/`.        |
| `pydantic_to_postgres_columns`  | `dados/utils/pydantic_postgres.py`            | Deriva DDL Postgres a partir do schema pydantic.        |
| `async_crawler_ibge_municipio`  | `dados/raw/utils/ibge_api_crawler.py`         | Coletas na API Agregados do IBGE.                       |
| `fix_ibge_digits`, `fix_ibge_x_digit` | `dados/silver/utils.py`                 | Limpeza de códigos especiais do IBGE (`.`, `X`, `-`).   |

Regras duras herdadas por todos os scripts:

- Nada de `print` nem `logging` da stdlib — apenas `get_logger`.
- Nada de `psycopg2`/`sqlalchemy` direto — apenas `PostgresETL`.
- Arquivos temporários **somente** em `tmp_data/<dataset_id>/{input,output}/`.
- Eventos de log padronizados: `flow.start`, `extract.done`, `validate.done`,
  `transform.done`, `load.done`, `flow.end`, `*.error`.

---

## 3. Zona Raw — `dados/raw/`

Papel: **espelhar a fonte original**. Cada dataset entra aqui exatamente como
foi recebido (CSV, JSON da API, dump do BigQuery, planilha), tipicamente em
colunas `VARCHAR(255)`. Nada de tratamento, nada de agregação.

- **Entrada**: fonte externa (IBGE/Comex API, BigQuery, Receita Federal, drop
  de arquivo, etc.).
- **Saída**: uma tabela de aterrissagem por dataset no banco `$DB_RAW_ZONE`,
  schema = `dataset_id`.
- **Pydantic**: opcional.
- **Consumidores**: apenas silver.

Datasets atualmente implementados em `dados/raw/`:

```
al_ibge_censoagro/   al_ibge_pac/   al_ibge_paic/   al_ibge_pam/
al_ibge_pevs/        al_ibge_ppm/   br_csalpha_access/
br_csalpha_diretorios_brasil/   br_ibge_pas/   br_ibge_pia/   br_ibge_pof/
mip_csalfa/   pa_me_comex_stat/   pa_rf_rais/
```

Funções comuns vivem em `dados/raw/utils/` (cliente Postgres, crawler do IBGE)
e `dados/raw/constants/` (códigos geográficos da Amazônia Legal e do Pará).

---

## 4. Zona Silver — `dados/silver/`

Papel: **tipagem, validação e padronização**. A silver é onde os dados
ganham contrato. Cada tabela materializada tem um `BaseModel` pydantic
correspondente, e o dataframe é validado linha a linha antes da escrita.

### 4.1 Pydantic como contrato

Todo schema vive em `dados/silver/models/<dataset_id>.py`. Cada campo
declara três coisas — tipo Python, `description` e `unit` em
`json_schema_extra`:

```python
class AlIbgePamLavouraPermanente(_PamBase):
    area_destinada_colheita: Decimal | None = Field(
        description="Area destined for harvest (permanent crops)",
        json_schema_extra={"unit": "hectare"},
    )
```

Unidades padronizadas: `BRL`, `USD`, `kg`, `ton`, `L`, `head_count`,
`hectare`, `m2`, `ratio`, `percent`, `dimensionless`, `YYYY`, `YYYY-MM`,
`date`, `code` (lista completa em `REFACTORING.md` §4).

O linter `tests/test_pydantic_metadata.py` percorre todos os models de
silver/gold e falha se algum campo não tem `description` + `unit`.

### 4.2 Validações na fronteira

A validação acontece no passo `validate()`, **antes** da carga. Padrão
canônico:

```python
def validate(df: pd.DataFrame) -> pd.DataFrame:
    rows = df.to_dict("records")
    [AlIbgePamLavouraPermanente(**r) for r in rows]  # levanta na primeira linha ruim
    assert df["id_municipio"].is_unique, "PK duplicada"
    return df
```

Checks obrigatórios para entrar em silver:

- Schema pydantic existe e cobre todas as colunas materializadas.
- Cada campo tem `description` + `unit`.
- Validação linha a linha contra o schema antes do `load()`.
- Colunas de chave primária são únicas.

### 4.3 DDL derivada do schema

`pydantic_to_postgres_columns(Model)` traduz o `BaseModel` em
`{coluna: tipo Postgres}` (`int → BIGINT`, `Decimal → NUMERIC`,
`date → DATE`, `str → VARCHAR(255)`, etc.). Isso elimina os antigos dicts
paralelos de tipos: o pydantic é a **única** fonte de verdade.

Datasets atualmente em silver:

```
al_ibge_censoagro/   al_ibge_pam/   al_ibge_pevs/
br_ibge_pac/   br_ibge_pas/   br_ibge_pia/   br_ibge_pof/
```

Helpers de limpeza específicos da camada estão em `dados/silver/utils.py`
(códigos IBGE) e constantes (mapeamentos de produtos, colunas) em
`dados/silver/constants/`.

---

## 5. Zona Gold — `dados/gold/`

Papel: **camada que conversa diretamente com o algoritmo**. Cada dataset
gold prepara um artefato analítico — tipicamente um conjunto de
coeficientes ou indexadores — no formato que a Layer 2 do
[csalpha](https://github.com/csalpha-team/csalpha) consome.

- **Entrada**: apenas silver. Nunca raw, nunca outra gold.
- **Saída**: tabela em `$DB_AGREGATED_ZONE.<dataset_id>.<table>`, sempre
  com schema pydantic em `dados/gold/models/<dataset_id>.py`.
- **Consumidores**: a rotina de export (§6) e, por meio dela, o algoritmo.

Datasets atualmente em gold:

```
br_coeficientes_consumo/         # POF → coeficientes de consumo
br_coeficientes_exportacao/      # Comex → coeficientes de exportação
br_coeficientes_investimento/    # PAC/PAIC → coeficientes de investimento
br_coeficientes_renda/           # RAIS/PIA → produtividade e salário
br_despesas_familiares/          # POF → despesas familiares
br_servicos/                     # PAS/PIA → serviços/indústria/comércio
pa_coeficientes_custo/           # custos rurais — Pará
pa_indexadores_custo_producao_rural/
pa_indexadores_producao_rural/
pa_indexadores_salarios_producao_rural/
pa_indexadores_valor_producao_rural/
pa_servicos_industria_comercio/
```

Cada diretório de gold mantém, além do `flow.py`/scripts de tabela:

- `models/<dataset_id>.py` — schemas pydantic (mesma regra de
  `description` + `unit` da silver).
- `utils.py` — funções de cálculo específicas do coeficiente.
- `testing_utils.py` — fixtures e helpers para testes do dataset.
- arquivos de parâmetros (`*.json`) quando o cálculo depende de
  configuração externa (ex.:
  `pa_coeficientes_custo/parametros_coeficientes_custo.json`,
  `br_coeficientes_consumo/equivalencia_despesas.json`).

A validação na fronteira (pydantic linha a linha) é idêntica à da silver —
gold é o último ponto onde o contrato é checado antes do dado sair para o
algoritmo.

---

## 6. Estratégia de exportação — `dados/export/`

A integração com o algoritmo é feita por um único flow:
[`dados/export/dump_gold.py`](./dados/export/dump_gold.py). Ele lê tabelas
selecionadas da gold via `PostgresETL` e materializa os artefatos que a
Layer 2 espera, em formatos fixos:

| Artefato                          | Origem (gold)                                                      | Formato |
|-----------------------------------|--------------------------------------------------------------------|---------|
| `cost_coefficients.csv`           | `pa_coeficientes_custo.preparacao_camada_custo`                    | CSV     |
| `consumption_coefficients.csv`    | `br_coeficientes_consumo.preparacao_camada_consumo` (último ano)   | CSV wide|
| `investment_coefficients.json`    | `br_coeficientes_investimento.coeficientes_investimento`           | JSON    |
| `export_coefficients.json`        | `br_coeficientes_exportacao.preparacao_camada_exportacao`          | JSON por ano |
| `income_productivity.json`        | `br_coeficientes_renda.renda_produtividade`                        | JSON por ano |
| `income_salary.json`              | `br_coeficientes_renda.renda_salario`                              | JSON por ano |

Comportamento do flow:

1. Cada `export_*` lê a tabela gold correspondente, transforma para o
   formato esperado pelo algoritmo (CSV achatado, dicionário por ano,
   etc.) e grava em `gold_export/`.
2. Arquivos exógenos já presentes em `gold_export/` (os três
   `*_incidence.json` e o `l2_input_schemas_examples.md`) são **preservados** —
   o flow não os recalcula, apenas os empacota.
3. `bundle_zip()` compacta todo o conteúdo de `gold_export/` em
   `gold_export.zip` na raiz do repositório. Esse zip é o entregável que
   alimenta o algoritmo.

Executar:

```bash
uv run python -m dados.export.dump_gold_l2
```

Eventos de log emitidos: `export.<artefato>` para cada arquivo gerado,
`export.zip` ao final, e o par `flow.start` / `flow.end`.

---

## 7. Configuração e execução

As zonas e suas variáveis de ambiente são definidas em `.env` (template em
`.env.example`). Padrão:

```
DB_PREFIX=zona
DB_RAW_ZONE=${DB_PREFIX}_brutos
DB_SILVER_ZONE=${DB_PREFIX}_tratados
DB_AGREGATED_ZONE=${DB_PREFIX}_agregated
```

Subir o ambiente local (Postgres + PgAdmin) e instruções de execução: veja
[`CONTRIBUTING.md`](./CONTRIBUTING.md). 
