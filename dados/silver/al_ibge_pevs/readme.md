# Silver — `al_ibge_pevs` (Produção da Extração Vegetal)

Especificações de padronização e validação aplicadas na camada **silver** do conjunto
`al_ibge_pevs`, tabela `produtos_extracao_vegetal`.

Fonte: IBGE / Agregados, tabela **289**, classificação **193** (Tipo de produto extrativo),
variáveis **144** (quantidade produzida) e **145** (valor da produção).

---

## 1. Diretrizes de padronização de unidade de medida

A padronização de unidade de medida ocorre na **silver**, ao ler do **raw**.

- A unidade de medida da PEVS **não vem nos dados da série** — a variável 144 traz apenas
  o texto *"Vide categorias da classificação 'Tipo de produto extrativo'"*. A unidade é
  definida **por produto** nos metadados do agregado
  (`servicodados.ibge.gov.br/api/v3/agregados/289/metadados` →
  `classificacoes[193].categorias[].unidade`).
- Essa unidade é **ingerida no raw** como coluna `unidade_medida`, fazendo join por
  `id_produto` (= id da categoria nos metadados). Ver
  `get_classificacao_unidades` em `dados/raw/utils/ibge_api_crawler.py`.
- Unidades nativas na PEVS (53 produtos + Total):

  | Unidade nativa | Nº de produtos | Exemplos |
  |---|---|---|
  | `Toneladas` | 48 | açaí, castanha, borrachas, ceras, fibras, oleaginosos, carvão vegetal… |
  | `Metros cúbicos` | 4 | `lenha`, `madeira-tora`, `pinheiro brasileiro-nó de pinho`, `pinheiro brasileiro-madeira em tora` |
  | `Mil árvores` | 1 | `pinheiro brasileiro-árvores abatidas` (contagem) |
  | `NULL` | 1 | `total` (agregado de unidades distintas — sem unidade) |

### Conversão para unidade comum (toneladas)

- Os produtos em **`Metros cúbicos`** são convertidos para **toneladas** usando a densidade
  básica média de madeira **`PEVS_DENSIDADE_TON_M3 = 0,5 t/m³`**
  (`dados/silver/constants/produtos.py`).
- A conversão é **dirigida pela coluna `unidade_medida`** (não por lista de produtos
  chumbada): ver `pevs_volume_to_weight` em `dados/utils/agricultural_conversions.py`.
  Após converter, a `unidade_medida` efetiva passa a `Toneladas`.
- `Mil árvores` (`pinheiro brasileiro-árvores abatidas`) é **contagem** e **não é convertida**;
  permanece com `unidade_medida = 'Mil árvores'`. Na Amazônia Legal esse produto é sempre
  zero (Araucária é espécie do Sul).
- Resultado: `quantidade_produzida` fica homogênea em **toneladas**, exceto árvores abatidas.
  A coluna `unidade_medida` documenta a unidade **efetiva do valor armazenado**
  (`Toneladas`, `Mil árvores` ou `NULL` para o total).

> Nota: a conversão volume→massa é aproximada (a densidade varia por espécie/umidade);
> 0,5 t/m³ é o fator genérico de inventário florestal adotado para todos os madeireiros.

---

## 2. Validação de quantidades e valores após transformações na silver

Ordem das transformações em `transform()`
(`dados/silver/al_ibge_pevs/extracao_vegetal.py`):

1. **Padronização de nomes de produto** via `dicionario_produtos_pevs`.
2. **Tratamento de sentinelas do IBGE** (`fix_ibge_digits`):
   - `..`, `...`, `-` → `0`;
   - `X` (valor inibido) → imputado pela **média da razão por grupo/UF** (ratio imputation).
3. **Conversão de unidade de medida** de quantidade (m³ → t), item 1 acima.
4. **Normalização monetária** de `valor_producao` (`currency_fix`): deflaciona moedas
   históricas (Cruzeiros, Cruzados, Cruzados Novos, Cruzeiros Reais) para a base atual.

Validações em `validate()`:

- Falha se o DataFrame estiver vazio;
- Falha em **PK duplicada** (`id_municipio`, `ano`, `produto`);
- `quantidade_produzida` / `valor_producao` → `Decimal` (NA → `None`);
- `unidade_medida`: `NaN`/vazio → `None`;
- Cada linha é validada contra o schema pydantic `AlIbgePevsExtracaoVegetal`.

---

## 3. Censos x PAM/PEVS

- **PEVS** (este dataset): unidade definida **por produto** nos metadados; quantidade
  madeireira em m³ convertida para toneladas (acima).
- **PAM**: alteração histórica de unidade em 2001 (frutas em "mil frutos"/"mil cachos"
  antes de 2001 → toneladas), tratada por `products_weight_ratio_fix`
  (`dados/utils/agricultural_conversions.py`).
- A padronização dos nomes de produto entre as fontes usa o **Censo Agropecuário como
  referência** (ver `dados/silver/constants/produtos.py`).

---

## 4. Valores Âncora

Referências para conferência rápida da conversão m³ → t (×0,5), valores máximos observados:

| Produto | Raw (m³) | Silver (t) = ÷2 |
|---|---|---|
| `lenha` | 1.001.980 | 500.990,0 |
| `madeira-tora` | 30.723.421 | 15.361.710,5 |

Distribuição de `unidade_medida` na silver: `Toneladas` ≈ 1.567.644 · `Mil árvores` 30.147 ·
`NULL` (total) 30.147.
