# Migração — Regiões de Integração (silver `regioes_integracao`)

## Contexto

O enriquecimento de região de integração na zona **gold** passou a usar a tabela
silver `br_csalpha_diretorios_brasil.regioes_integracao` como fonte única, em vez
do dict hardcoded em `dados/gold/pa_indexadores_producao_rural/utils.py`.

A mudança está em `dados/gold/pa_indexadores_producao_rural/_common.py`
(`enrich_with_regiao` agora faz `LEFT JOIN` por `id_municipio` com a tabela
silver, trazendo `nome`, `sigla_uf` e `nome_regiao_integracao`).

**Por que re-rodar:** o dict antigo tinha **6 `id_municipio` incorretos**
(id ↔ nome trocados em relação ao código IBGE). As tabelas gold já materializadas
carregam esses erros e só são corrigidas re-executando os fluxos.

| `id_municipio` | região/nome correto (novo) | estava errado como |
|---|---|---|
| 1508407 | Xinguara — Araguaia | Viseu / Rio Caeté |
| 1508308 | Viseu — Rio Caeté | Xinguara / Araguaia |
| 1505007 | Nova Timboteua — Rio Caeté | Óbidos / Baixo Amazonas |
| 1505304 | Oriximiná — Baixo Amazonas | Nova Timboteua / Rio Caeté |
| 1504000 | Limoeiro do Ajuru — Tocantins | Magalhães Barata / Guamá |
| 1504109 | Magalhães Barata — Guamá | Limoeiro do Ajuru / Tocantins |

## Pré-requisito

A tabela silver precisa estar carregada antes de rodar qualquer gold abaixo:

```bash
uv run python -m dados.raw.br_csalpha_diretorios_brasil.regioes_integracao
uv run python -m dados.silver.br_csalpha_diretorios_brasil.regioes_integracao
```

## Golds a re-rodar

Todos os fluxos que consomem `enrich_with_regiao`. Marcar conforme executados.

### `pa_indexadores_producao_rural`

- [x] `pevs_extracao_vegetal` — **já re-rodado e validado** (252.720 linhas, 0 região nula)
- [x] `pam_lavoura_permanente` — re-rodado e validado (286.416 linhas)
- [x] `pam_lavoura_temporaria` — re-rodado e validado (249.696 linhas)
- [x] `censo_2006_extracao_vegetal` — re-rodado e validado (11.520 linhas)
- [x] `censo_2017_extracao_vegetal` — re-rodado e validado (14.976 linhas)
- [x] `censo_2006_lavoura_permanente` — re-rodado e validado (18.720 linhas)
- [x] `censo_2017_lavoura_permanente` — re-rodado e validado (20.448 linhas)
- [x] `censo_2006_lavoura_temporaria` — re-rodado e validado (15.264 linhas)
- [x] `censo_2017_lavoura_temporaria` — re-rodado e validado (15.840 linhas)
- [x] `censo_2006_lavoura_temporaria_2284` — re-rodado e validado (14.976 linhas)
- [x] `censo_2006_2017_despesas` — re-rodado e validado (11.232 linhas)

```bash
uv run python -m dados.gold.pa_indexadores_producao_rural.pam_lavoura_permanente
uv run python -m dados.gold.pa_indexadores_producao_rural.pam_lavoura_temporaria
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2006_extracao_vegetal
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2017_extracao_vegetal
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_permanente
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2017_lavoura_permanente
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_temporaria
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2017_lavoura_temporaria
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2006_lavoura_temporaria_2284
uv run python -m dados.gold.pa_indexadores_producao_rural.censo_2006_2017_despesas
```

### `pa_indexadores_custo_producao_rural`

- [x] `censo_2006_2017_despesas` — re-rodado e validado (11.232 linhas)

```bash
uv run python -m dados.gold.pa_indexadores_custo_producao_rural.censo_2006_2017_despesas
```

### `pa_indexadores_valor_producao_rural`

- [x] `censo_2006_2017_valor_producao` — re-rodado e validado (7.776 linhas)
- [x] `censo_2017_pessoal_ocupado_producao_rural` — re-rodado e validado (288 linhas)

```bash
uv run python -m dados.gold.pa_indexadores_valor_producao_rural.censo_2006_2017_valor_producao
uv run python -m dados.gold.pa_indexadores_valor_producao_rural.censo_2017_pessoal_ocupado_producao_rural
```

### `pa_coeficientes_custo`

- [x] `preparacao_camada_custo` — re-rodado e validado (300 linhas, agregado por RI)

```bash
uv run python -m dados.gold.pa_coeficientes_custo.preparacao_camada_custo
```

## Validação pós-execução (por tabela)

> **Atenção:** o check só-de-nulo abaixo é **necessário mas não suficiente**. O
> erro do dict antigo era um *swap* região↔região (id↔nome trocados), não uma
> ausência — as tabelas materializadas com o dict tinham **0 nulos** e mesmo
> assim carregavam a região errada nos 6 ids. Por isso conferir **também** os 6
> ids explicitamente (Check B).

**Check A — sem região nula para o Pará** (esperado: 0):

```sql
SELECT COUNT(*) AS n_null_regiao
FROM <schema>.<tabela>
WHERE id_municipio LIKE '15%' AND nome_regiao_integracao IS NULL;
-- esperado: 0
```

**Check B — os 6 ids corrigidos com a região certa** (esperado: 0 divergências):

```sql
SELECT COUNT(*) AS n_regiao_errada
FROM <schema>.<tabela> t
JOIN (VALUES
  ('1504000','Tocantins'), ('1504109','Guamá'), ('1505007','Rio Caeté'),
  ('1505304','Baixo Amazonas'), ('1508308','Rio Caeté'), ('1508407','Araguaia')
) e(id, reg) ON t.id_municipio = e.id
WHERE t.nome_regiao_integracao IS DISTINCT FROM e.reg;
-- esperado: 0
```

**Resultado (2026-06-04):** todas as 14 tabelas passaram — `n_null_regiao = 0`
e `n_regiao_errada = 0`. `preparacao_camada_custo` é agregada por RI (sem
`id_municipio`): validada via 0 nulos e nomes de RI válidos; o fix dos 6 ids
propaga porque o `enrich_with_regiao` ocorre antes da agregação. O export L1
(`gold_export_l1/vetores_producao_rural.json`) foi regerado em seguida.

## Pendência

~~`dados/gold/pa_indexadores_producao_rural/utils.py` (dict com os 6 ids errados)
ficou **sem uso** após a migração — candidato a remoção para evitar reuso acidental.~~

**Resolvido (2026-06-04):** arquivo removido (`git rm`). Confirmado sem nenhum
import no repo. A referência remanescente na docstring de `_common.py` foi
atualizada para apontar a tabela silver `regioes_integracao`.
