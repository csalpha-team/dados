# Lógica de tratamento e padronização

Análise de onde ocorrem os passos críticos de padronização no repositório e a qual script estão associados.

A padronização está distribuída por três camadas (raw → silver → gold), com **tipos distintos** de padronização e **fontes de verdade** diferentes. Não há um módulo central único; o repositório usa três famílias de dicionários + utilitários.

---

## 1. Padronização de identificadores geográficos (raw)

**Fonte:** `dados/dicionarios_institucionais.py` — `uf_id_sigla` (código IBGE → sigla UF).

**Onde é aplicada (camada raw, no momento da ingestão via API SIDRA):**
- `dados/raw/br_ibge_pia/tbl_1848.py:7,19`
- `dados/raw/br_ibge_pia/tbl_1849.py:7,20`
- `dados/raw/br_ibge_pia/tbl_1987.py:7,19`
- `dados/raw/br_ibge_pia/tbl_1988.py:7,19`
- `dados/raw/br_ibge_pof/tbl_2393.py:6`

**Mecanismo:** consumida por `dados/raw/br_ibge_pia/utils.py:68` (`download_json`), que itera sobre as UFs para parametrizar a URL da API IBGE. É um passo **de extração**, não de transformação.

---

## 2. Padronização de nomes de produtos (silver)

**Fonte:** `dados/silver/padronizacao_produtos.py` — seis dicionários, um por pesquisa/tabela:
- `dicionario_produtos_pevs` (Extração Vegetal)
- `dicionario_produtos_censo_6949_2233` (Censo Agro — extração vegetal/silvicultura)
- `dicionario_produtos_pam_permanente`, `dicionario_produtos_pam_temporaria` (PAM)
- `dicionario_produtos_censo_6955_2518`, `dicionario_produtos_censo_6957_2337` (Censo Agro — lavouras)

**Onde é aplicada (sempre na camada silver, via `data["produto"].map(...)`):**
- `silver/al_ibge_pam/lavoura_temporaria.py:67`
- `silver/al_ibge_pam/lavoura_permanente.py:102`
- `silver/al_ibge_pevs/extracao_vegetal.py:61`
- `silver/al_ibge_censoagro/2006_tbl_2337.py:63`, `2006_tbl_2384.py:65`, `2006_tbl_2233.py:66`, `2006_tbl_2518.py:77`
- `silver/al_ibge_censoagro/2017_tbl_6949.py:68`, `2017_tbl_6955.py:80`, `2017_tbl_6957.py:69`

**Convenção do projeto** (documentada como comentário no topo de `padronizacao_produtos.py`): padrão `{produto}-{informação adicional}`; quando há divergência entre PAM e Censo Agro, o **Censo é a referência**.

---

## 3. Padronização de valores/colunas (silver) — utilitários

**Fonte:** `dados/silver/utils.py`. Estes são os passos **críticos** que transformam dados brutos do IBGE em valores numéricos consistentes:

| Função | Linha | O que padroniza |
|---|---|---|
| `fix_ibge_digits` | `utils.py:5` | Substitui marcadores especiais do IBGE (`-`, `..`, `...`, `X`) por 0 ou imputação |
| `fix_ibge_x_digit` | `utils.py:83` | Imputação por razão unitária para valores `X` (suprimidos por sigilo) |
| `currency_fix` | `utils.py:205` | Normaliza valor da produção entre moedas históricas (Cruzeiro → Real) |
| `products_weight_ratio_fix` | `utils.py:227` | Converte unidades (frutos/cachos → toneladas) para harmonizar séries pré/pós-2001 |
| `check_duplicates` | `utils.py:283` | Falha o pipeline se houver duplicatas em chaves naturais |
| `calcula_autoconsumo_comercio` | `utils.py:308` | Pivota Total/Autoconsumo e calcula Comércio |

**Renomeação de colunas:** feita inline em cada script silver via dicionário local + `df.rename`. Ex.: `silver/al_ibge_pam/lavoura_temporaria.py:55-64` (`"Área colhida" → "area_colhida"` etc.). **Não há fonte central** para nomes de colunas — cada silver script define o seu.

---

## 4. Padronização territorial fina (gold)

**Fonte:** `dados/gold/pa_indexadores_producao_rural/utils.py` — `dicionario_regioes_integracao` (`id_municipio` → Região de Integração do Pará).

**Onde é aplicada:** scripts gold em `gold/pa_indexadores_producao_rural/*.py`, ex.: `pam_lavoura_temporaria.py:50` via `data['id_municipio'].map(dicionario_regioes_integracao)`. Faz também **join com diretório do BD** (`basedosdados.br_bd_diretorios_brasil.municipio`) para trazer `nome` e `sigla_uf`.

---

## 5. Diretórios externos (raw, materializados)

`dados/raw/br_csalpha_diretorios_brasil/` carrega tabelas de referência usadas como **dicionários relacionais** em vez de literais Python:
- `cnae_2.py` — subset CNAE filtrado do BD
- `nomenclatura_comum_mercosul.py` — NCM
- `prodlist_industria.py`, `prodlist_pesca.py` — Prodlist IBGE (Excel local)

---

## Resumo crítico

| Tipo de padronização | Camada | Fonte de verdade | Como é aplicada |
|---|---|---|---|
| Geográfica (UF id↔sigla) | raw (extração) | `dados/dicionarios_institucionais.py` | Parametriza URLs SIDRA em `raw/br_ibge_pia/utils.py:download_json` |
| Nomes de produto | silver | `silver/padronizacao_produtos.py` (6 dicts) | `df["produto"].map(...)` em cada script silver |
| Marcadores IBGE / moeda / unidades | silver | `silver/utils.py` | Chamadas explícitas em cada script silver |
| Renomeação de colunas (`Área plantada` → `area_plantada`) | silver | **dicionário local** em cada script | Inline, sem fonte central — risco de divergência |
| Regiões de integração (PA) | gold | `gold/pa_indexadores_producao_rural/utils.py` | `df["id_municipio"].map(...)` em scripts gold do Pará |
| Diretórios oficiais (CNAE, NCM, Prodlist, município) | raw / consultas BD | Tabelas materializadas + BD externo | SQL join (`bd.read_sql`, `PostgresETL`) |

---

## Pontos de atenção

1. **Renomeação de colunas descentralizada** — `cols = {...}` é repetido em quase todo script silver. Candidato natural a um dicionário central (analogamente ao `padronizacao_produtos.py`).
2. **`dicionarios_institucionais.py` subutilizado** — só serve para iterar UFs na extração, não para validar/converter siglas no resto do pipeline.
3. **Divergência com a regra dbt/Postgres** — silver e gold ainda são scripts Python que escrevem direto no Postgres via `PostgresETL`. Para alinhar com a regra de boundary dbt (raw→silver→gold como modelos `.sql` + `schema.yml`), estes passos de padronização precisariam migrar para modelos dbt.
