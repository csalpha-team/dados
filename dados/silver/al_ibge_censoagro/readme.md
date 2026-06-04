# Silver — `al_ibge_censoagro` (Censo Agropecuário 2006 e 2017)

Especificações de padronização e validação de **unidade de medida** aplicadas na camada
**silver** do Censo Agropecuário (Amazônia Legal), nas tabelas de **produção física**:
extração vegetal, lavoura permanente e lavoura temporária.

Tabelas no escopo (uma por agregado/ano):

| Tema | Classif. produto | 2006 (AGREGADO) | 2017 (AGREGADO) |
|---|---|---|---|
| Extração vegetal | `229` | `tbl_2233_2006` (2233) | `tbl_6949_2017` (6949) |
| Lavoura permanente | `227` | `tbl_2518_2006` (2518) | `tbl_6955_2017` (6955) |
| Lavoura temporária | `226` | `tbl_2284_2006` (2284), `tbl_2337_2006` (2237) | `tbl_6957_2017` (6957) |

As demais tabelas do dataset (`tbl_1909/1931/2782/2006`, `tbl_6885/6898/6899/2017`) só têm
contagem de estabelecimentos e valores em BRL — **não têm unidade física** e ficam fora
deste tratamento.

---

## 1. Diretrizes de padronização de unidade de medida

A padronização ocorre na **silver**, ao ler do **raw**.

- A unidade de medida do censo **não vem na série**: a variável de quantidade traz uma
  unidade genérica (`'Não se aplica'`, `'Unidades'`…). A unidade correta é definida
  **por produto (categoria)** nos metadados do agregado
  (`servicodados.ibge.gov.br/api/v3/agregados/{AGREGADO}/metadados` →
  `classificacoes[229|227|226].categorias[].unidade`).
- Essa unidade é **ingerida no raw** como coluna `unidade_medida`, por `id_produto`, apenas
  nas linhas da variável de **quantidade produzida**. Ver
  `enrich_unidade_medida_por_produto` em `dados/raw/al_ibge_censoagro/utils.py` (reusa
  `get_classificacao_unidades` de `dados/raw/utils/ibge_api_crawler.py`).
- Unidades nativas encontradas nos metadados (idênticas entre 2006 e 2017):

  | Unidade nativa | Onde | Tratamento na silver |
  |---|---|---|
  | `Toneladas` | maioria dos produtos | mantida |
  | `Mil metros cúbicos` | extração vegetal: lenha, madeira em toras (papel / outra) | → toneladas |
  | `Mil frutos` | permanente: coco-da-baía, graviola, jaca · temporária: abacaxi | → toneladas |
  | `Mil unidades` | permanente: 8 tipos de mudas | **mantida** (contagem) |
  | `NULL` | produto `Total` (agregado) | sem unidade |

### Conversão para unidade comum (toneladas)

Dirigida pela coluna `unidade_medida` (não por lista de produtos chumbada):
`censo_quantity_to_weight` em `dados/utils/agricultural_conversions.py`. Converte as colunas
`quantidade_produzida` e `quantidade_vendida` (e, por consequência, as derivadas
`autoconsumo_*`/`comercio_*`, calculadas **depois**). Após converter, a `unidade_medida`
efetiva passa a `Toneladas`.

- **`Mil metros cúbicos` → toneladas**: `× CENSO_FATOR_MIL_M3_TON` = `1000 × 0,5 = 500`
  (densidade básica média de madeira `PEVS_DENSIDADE_TON_M3`).
- **`Mil frutos` → toneladas**: `× kg_por_fruto` (o valor está em milhares de frutos, então
  `t = quantidade × kg_por_fruto`). Fatores em `CENSO_KG_POR_FRUTO`
  (`dados/silver/constants/produtos.py`), médias nacionais aproximadas:

  | Fruto | kg/fruto |
  |---|---|
  | abacaxi | 1,5 |
  | coco da baía | 1,3 |
  | graviola | 2,0 |
  | jaca | 8,0 |

- **`Mil unidades` (mudas)** é **contagem** e **não** é convertida; permanece com
  `unidade_medida = 'Mil unidades'` (análogo a `'Mil árvores'` na PEVS).
- Resultado: `quantidade_produzida`/`quantidade_vendida` ficam homogêneas em **toneladas**,
  exceto as mudas. A coluna `unidade_medida` documenta a unidade **efetiva do valor
  armazenado**.

> Nota: a conversão volume→massa (0,5 t/m³) e fruto→massa são aproximadas (variam por
> espécie/UF); são os fatores genéricos adotados no projeto.

---

## 2. Por que a frente temporal de 2001 do PAM **não** se aplica

No PAM, frutas eram medidas em "mil frutos"/"mil cachos" **antes de 2001** e passaram a
toneladas depois (tratado por `products_weight_ratio_fix`). O **Censo Agropecuário é pontual**:
só há **2006 e 2017**, ambos pós-2001. Portanto a frente temporal não existe aqui e
`products_weight_ratio_fix` **não** é usada. Resta apenas a unidade **estática por produto**
(seção 1) — inclusive frutos que o IBGE ainda reporta em `Mil frutos` no censo.

---

## 3. Validação após transformações na silver

Ordem em `transform()`:

1. Padronização de nomes de produto (`dicionario_produtos_censo_*`) e de `tipo_agricultura`.
2. Tratamento de sentinelas do IBGE (`fix_ibge_digits`).
3. **Conversão de unidade** (`censo_quantity_to_weight`, seção 1).
4. Apenas tabelas de destinação (2233, 2284, 2518): `calcula_autoconsumo_comercio` deriva
   `autoconsumo_*`/`comercio_*` a partir das quantidades **já convertidas**
   (`unidade_medida` é carregada no índice do pivot para sobreviver à reshape).

Em `validate()`:

- Falha se vazio; falha em **PK duplicada** (`ano`, `id_municipio`, `produto`, `tipo_agricultura`);
- métricas → `Decimal` (NA → `None`); `unidade_medida` **excluída** da coerção decimal;
- `unidade_medida`: `NaN`/`''`/`'NaN'` → `None`;
- cada linha validada contra o schema pydantic da tabela.

---

## 4. Censos × PAM × PEVS

- **PEVS**: unidade por produto; madeira em `Metros cúbicos` → t (×0,5).
- **PAM**: alteração histórica de 2001 (frutas mil frutos/cachos → t), via
  `products_weight_ratio_fix`.
- **Censo** (este dataset): unidade por produto (estática), **sem** frente temporal;
  `Mil metros cúbicos` (×500) e `Mil frutos` (×kg/fruto) → t; mudas em `Mil unidades` mantidas.
- A padronização de nomes de produto usa o **Censo como referência**
  (`dados/silver/constants/produtos.py`).

---

## 5. Valores Âncora

Conferências de conversão (raw → silver), em linhas com valor limpo:

| Tabela | Produto | Raw (unidade nativa) | Fator | Silver (toneladas) |
|---|---|---|---|---|
| `tbl_6949_2017` | lenha (mun 2102804) | 63 (Mil m³) | ×500 | 31.500 |
| `tbl_2233_2006` | lenha (mun 1501782) | 165 (Mil m³) | ×500 | 82.500 |
| `tbl_6957_2017` | abacaxi (mun 1718204) | 5.822 (Mil frutos) | ×1,5 | 8.733 |
| `tbl_6955_2017` | coco da baía (mun 1504703) | 110.140 (Mil frutos) | ×1,3 | 143.182 |

Distribuição de `unidade_medida` na silver (linhas):

| Tabela | Toneladas | Mil unidades | NULL (Total) |
|---|---|---|---|
| `tbl_2233_2006` | 11.520 | — | — |
| `tbl_2284_2006` | 14.976 | — | — |
| `tbl_2337_2006` | 80.392 | — | 1.546 |
| `tbl_2518_2006` | 16.416 | 2.304 | — |
| `tbl_6949_2017` | 78.846 | — | 1.546 |
| `tbl_6955_2017` | 95.852 | 12.368 | 1.546 |
| `tbl_6957_2017` | 83.484 | — | 1.546 |
