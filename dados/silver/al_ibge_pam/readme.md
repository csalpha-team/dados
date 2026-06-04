# Silver — `al_ibge_pam` (Produção Agrícola Municipal)

Especificações de padronização e validação aplicadas na camada **silver** do conjunto
`al_ibge_pam`, tabelas `lavoura_permanente` e `lavoura_temporaria`.

Fonte: IBGE / Agregados — **1613** / classificação **82** (lavoura permanente) e
**1612** / classificação **81** (lavoura temporária). Variáveis: `Quantidade produzida`,
`Valor da produção`, `Área colhida`, `Área destinada à colheita` (permanente) /
`Área plantada` (temporária) e `Rendimento médio da produção`.

---

## 1. Diretrizes de unidade de medida

O problema de unidade na PAM tem **duas naturezas** — uma é trivial (a unidade é única),
a outra é o cerne do tratamento (mudança histórica de unidade em 2001).

### 1.1 Por produto (estática) — uniforme em `Toneladas`

- Diferente da **PEVS**, na PAM a unidade **não** é definida por produto. Nos metadados do
  agregado (`servicodados.ibge.gov.br/api/v3/agregados/1613/metadados`) as **categorias da
  classificação não têm unidade** (`categorias[].unidade = null`); a unidade é definida na
  **variável**. A variável `Quantidade produzida` (id 214) é **`Toneladas` para todos os
  produtos**.
- Por isso `get_classificacao_unidades` (usado na PEVS) **não se aplica** à PAM: retornaria
  vazio. A `unidade_medida` já é corretamente preenchida no **raw** pela unidade da variável
  (`parse_pam_json`), e é carregada na silver pela linha de `Quantidade produzida`.
- Como a unidade de quantidade é homogênea, **não há** conversão dirigida por unidade
  análoga ao `pevs_volume_to_weight` (m³→t) — não existe produto em unidade não-tonelada.

### 1.2 Temporal (por ano) — frutas pré-2001

- **Antes de 2001** o IBGE informava abacate, banana, caqui, figo, goiaba, laranja, limão,
  maçã, mamão, manga, maracujá, marmelo, pera, pêssego, tangerina (lavoura permanente) e
  melancia, melão (lavoura temporária) em **mil frutos** — **banana em mil cachos** — com
  rendimento em **frutos/ha** (cachos/ha para banana). De **2001** em diante passaram a
  **toneladas** e **kg/ha**.
- A silver aplica **`products_weight_ratio_fix`** (`dados/utils/agricultural_conversions.py`,
  via `dados/silver/utils.py`): para `ano < 2001` e produtos do `DICIONARIO_DE_PROPORCOES`,
  multiplica `quantidade_produzida` pelo fator fruto→tonelada e **recalcula**
  `rendimento_medio_producao = quantidade_t / area_colhida * 1000` (kg/ha). Demais
  produtos/anos passam intactos.
- Referência IBGE:
  `https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf`.
- Resultado: `quantidade_produzida` fica homogênea em **toneladas** e
  `rendimento_medio_producao` em **kg/ha** em toda a série; `unidade_medida` documenta a
  unidade **efetiva do valor armazenado** (sempre `Toneladas` quando há quantidade).

> O `DICIONARIO_DE_PROPORCOES` usa os **nomes originais do IBGE** (ex. `Banana (cacho)`,
> `Laranja`). Por isso `products_weight_ratio_fix` roda **antes** do `map` de padronização
> de nomes, quando `produto` ainda traz o nome original.

---

## 2. Validação após transformações na silver

Ordem das transformações em `transform()`
(`dados/silver/al_ibge_pam/lavoura_{permanente,temporaria}.py`):

1. **Tratamento de sentinelas do IBGE** (`fix_ibge_digits`): `..`, `...`, `-` → `0`;
   `X` (valor inibido) → imputado pela média da razão por grupo/UF (ratio imputation).
2. **Coerção numérica** de `quantidade_produzida` e `area_colhida` (entradas do passo 3).
3. **Conversão temporal das frutíferas** (`products_weight_ratio_fix`), item 1.2 acima —
   sobre os **nomes originais** do IBGE.
4. **Padronização de nomes de produto** via `dicionario_produtos_pam_{permanente,temporaria}`.
5. **Normalização monetária** de `valor_producao` (`currency_fix`): moedas históricas
   (Cruzeiros/Cruzados/…) são convertidas para a base **Mil Reais**. O valor é mantido nessa
   escala — `unit: "1000xBRL"` (1000×BRL), igual a PEVS. **Não** há conversão para BRL puro;
   os algoritmos downstream assumem 1000xBRL.

Validações em `validate()`:

- Falha se o DataFrame estiver vazio;
- Falha em **PK duplicada** (`id_municipio`, `ano`, `produto`);
- Métricas → `Decimal` (NA → `None`);
- `unidade_medida`: `NaN`/vazio/`'NaN'` → `None`;
- Cada linha é validada contra `AlIbgePamLavouraPermanente` / `AlIbgePamLavouraTemporaria`.

---

## 3. PEVS × PAM (contraste de unidade)

| | **PEVS** (`al_ibge_pevs`) | **PAM** (`al_ibge_pam`) |
|---|---|---|
| Onde está a unidade | Por **produto** (categoria) nos metadados | Por **variável** (categorias sem unidade) |
| Variação por produto | Sim (`Toneladas`, `Metros cúbicos`, `Mil árvores`) | Não — quantidade sempre `Toneladas` |
| Conversão dirigida por unidade | `pevs_volume_to_weight` (m³ → t, fator 0,5 t/m³) | Não se aplica |
| Mudança histórica de unidade | Não | **Sim** — frutas pré-2001 (`products_weight_ratio_fix`) |
| `get_classificacao_unidades` | Usado no raw | Não se aplica (retorna vazio) |

Padronização de nomes de produto entre fontes usa o **Censo Agropecuário como referência**
(`dados/silver/constants/produtos.py`).

---

## 4. Valores Âncora

Conferência rápida da conversão fruto→tonelada (lavoura permanente, município `1503705`,
`Banana (cacho)`, fator 10,20):

| ano | `quantidade_produzida` raw | silver (t) | `rendimento` raw | silver (kg/ha) | `area_colhida` |
|---|---|---|---|---|---|
| 2000 (pré) | 3.282 (mil cachos) | 33.476,4 | 1.665 (cachos/ha) | 16.993,1 | 1.970 |
| 2001 (pós) | 24.625 (t) | 24.625,0 | 12.500 (kg/ha) | 12.500 | 1.970 |
| 2010 (pós) | 18.750 (t) | 18.750,0 | 12.500 (kg/ha) | 12.500 | 1.500 |

`unidade_medida` na silver: `Toneladas` para toda linha com quantidade; `None` apenas onde
a origem não traz unidade. Produtos fora do `DICIONARIO_DE_PROPORCOES` e anos ≥ 2001
permanecem inalterados.
