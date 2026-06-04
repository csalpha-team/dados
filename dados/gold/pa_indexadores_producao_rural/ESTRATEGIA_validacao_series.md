# Estratégia de Validação de Séries — `pa_indexadores_producao_rural`

## Contexto

Os commits recentes (`0ec8ca0` "fix pevs measurement units", além do trabalho de
`PROMPT_validacao_unidades.md` na zona silver) expuseram **erros de unidade de
medida** em PAM/PEVS/Censo — m³ vs tonelada, "mil frutos"/"mil cachos" vs
tonelada, conversões de frutas pré-2001 — que aparecem como saltos espúrios nas
séries de produção. Precisamos de uma forma sistemática de:

- **(A)** sinalizar **variações altas** na série anual PAM/PEVS que provavelmente
  sejam erros de dado/unidade; e
- **(B)** **confrontar os dois pontos censitários (2006, 2017) contra a série
  PAM/PEVS** para o mesmo produto, já que o Censo fornece duas âncoras
  independentes com as quais a série de pesquisa deve ser consistente.

Este documento é o **catálogo de métricas / spec de desenho** (matemática,
fórmulas, limiares, layout do relatório). A implementação é uma etapa posterior.

O validador será um **relatório efêmero**: lê as tabelas gold de
`pa_indexadores_producao_rural`, emite CSV + markdown; **não** cria nova tabela
persistida (respeita a regra de camadas — nenhuma tabela gold derivada de gold).

As séries são analisadas em **duas granularidades**: `id_municipio` e
`nome_regiao_integracao` (soma por RI). RI é estatisticamente mais robusta;
município captura erros localizados.

### Fatos dos dados (confirmados)

- **PAM/PEVS**: série anual, grão `(ano, id_municipio, produto)`, décadas de
  cobertura. Métricas `quantidade_produzida` (ton), `valor_producao` (1000xBRL — IBGE "Mil Reais"). PAM
  adiciona `area_colhida` / `rendimento_medio_producao`.
- **Censo**: apenas `ano ∈ {2006, 2017}`, grão
  `(ano, id_municipio, produto, tipo_agricultura)`. Somar sobre
  `tipo_agricultura` para comparar com PAM.
- `produto` é texto livre, padronizado por dicionário **por fonte** — não há
  garantia de nomes idênticos entre PAM/PEVS e Censo → um crosswalk de produtos é
  pré-requisito da Parte B.
- Valores já são deflacionados na silver (`currency_fix`), então comparações de
  valor/preço entre anos são válidas em termos reais.

---

## Parte A — Detecção de anomalias temporais (PAM / PEVS)

Objetivo: uma métrica que capture variações interanuais altas em
`quantidade_produzida` e `valor_producao`, separe **erros prováveis** de
**mudança estrutural genuína** e aponte **qual** série está errada.

### A0. Pré-processamento

- Construir painéis por série com chave `(grão, produto)`, grão ∈ {município, RI}.
  Série RI = soma de quantidade e valor entre municípios.
- Trabalhar em **espaço log** para efeitos multiplicativos (um erro de unidade é
  um *fator*, não um deslocamento aditivo). Tratar anos zero/nulo explicitamente
  (muitas séries municipais são esparsas): tratar 0/NULL como lacuna, calcular
  retornos só entre anos consecutivos não-nulos, e registrar o comprimento da
  sequência para excluir séries de ponto único das estatísticas temporais.

### A1. Métrica núcleo — z-score modificado robusto sobre log-retornos

Para a métrica `x` (quantidade, valor), por série:

- Log-retorno: `r_t = ln(x_t) − ln(x_{t−1})`.
- Centro/escala robustos sobre a série: `med = mediana(r)`,
  `MAD = mediana(|r − med|)`.
- **Z-score modificado**: `M_t = 0.6745 · (r_t − med) / MAD`.
  **Sinalizar `|M_t| > 3.5`** (Iglewicz–Hoaglin).
- Por que MAD, não média/desvio: séries são curtas e contaminadas por erro; o
  desvio-padrão é ele próprio inflado pelos outliers que buscamos.
- Interpretação de `r`: um erro de unidade ×1000 → `r ≈ 6.9`; um erro de dígito
  (×10) → `r ≈ 2.3`. A métrica é independente de unidade e se auto-calibra por
  série.

### A2. Classificador pico-vs-degrau (teste de reversão)

Distingue um **pico** transitório (um ano ruim que reverte — quase certamente
erro) de uma **mudança de nível** real.

- Para um ano sinalizado `t`, inspecionar `r_t` e `r_{t+1}`.
- Coeficiente de reversão:
  `ρ_t = −(r_t · r_{t+1}) / (|r_t|·|r_{t+1}| + ε)` ∈ [−1, 1].
  - `ρ_t → +1` (sobe-desce ou desce-sobe): reversão limpa → **pico → alta
    probabilidade de erro**.
  - `ρ_t ≈ 0` (salto sem retorno): **degrau → possivelmente real** (novo
    município reportante, mudança de metodologia) → menor severidade, encaminhar
    para revisão manual.

### A3. Consistência interna — valor unitário implícito (preço)

O indício isolado mais forte de *qual* série está errada. Defina
`p_t = valor_producao / quantidade_produzida` (1000xBRL/ton — preço em milhares de R$ por tonelada).

- Quantidade salta e valor não → `p` move pelo mesmo fator → erro de unidade na
  **quantidade**.
- Valor salta e quantidade não → erro no **valor**.
- Ambos movem juntos → `p` estável → plausivelmente choque real (ou bug de
  escala compartilhado).
- Métrica: z-score robusto de `ln(p_t)` **temporalmente** (dentro da série) E
  **transversalmente** (entre municípios para o mesmo produto-ano; preços
  deflacionados devem ficar bem agrupados). Sinalizar `|z| > 3.5` em qualquer
  eixo.
- **Tabela de triangulação**: para cada ano sinalizado, registrar qual de
  {salto-qtd, salto-valor, salto-preço} disparou → inferir a causa-raiz
  automaticamente.

### A4. Plausibilidade física PAM (identidade rendimento × área)

Somente PAM — captura exatamente a classe de bug "mil frutos" pré-2001.

- Identidade:
  `quantidade_produzida ≈ area_colhida × rendimento_medio_producao` (reconciliar
  kg/ha vs ton). Sinalizar linhas onde a identidade quebra além de tolerância
  (ex.: >1%).
- Limites de rendimento: `rendimento_medio_producao` fora da plausibilidade
  agronômica por cultura (o bug de unidade de fruta inflou rendimentos ~10×).

### A5. Score de severidade (triagem, não enxurrada binária)

Composto contínuo e explicável, para que revisores priorizem em vez de afogar em
flags:

```
severity = w1·|M_qtd| + w2·|M_val| + w3·max(ρ,0)·max(|M_qtd|,|M_val|)
           + w4·|z_preço| + w5·quebra_identidade
```

Emitir a lista ranqueada **mantendo todos os componentes brutos** para
explicabilidade. Iniciar pesos em ~1 e calibrar contra casos conhecidos (exemplo
da banana abaixo).

### A6. Métodos mais pesados (documentados, fora da v1)

Filtro de Hampel (mediana móvel + MAD) para séries agregadas longas; outliers de
resíduo via STL robusto (anual → só tendência+resíduo, ganho marginal); detecção
de changepoint (PELT via `ruptures`) para separar formalmente mudanças de regime
de picos. Adicionam dependências `scipy`/`statsmodels`/`ruptures` — adiar até o
núcleo baseado em MAD se mostrar insuficiente.

---

## Parte B — Validação cruzada Censo ↔ pesquisa (Censo 2006/2017 vs PAM/PEVS)

Objetivo: para o mesmo produto, usar as duas âncoras censitárias para validar a
série de pesquisa.

### B0. Pré-requisitos

- **Crosswalk de produtos**: reconciliar `produto` entre fontes. Produtos
  presentes em uma fonte e ausentes em outra são em si um achado (lacuna de
  mapeamento).
- **Alinhamento de grão**: somar Censo sobre `tipo_agricultura` (familiar + não
  familiar) → total; comparar igual-com-igual
  (`quantidade_produzida` ↔ `quantidade_produzida`,
  `valor_producao` ↔ `valor_producao`).
- **Ressalva metodológica**: Censo (censo de estabelecimentos, ano agrícola de
  referência) ≠ PAM (estimativa municipal anual). Esperar mesma ordem de grandeza
  e movimento correlacionado — **não** igualdade. Todas as métricas da Parte B
  são, portanto, checagens *relativas/de forma*, não de igualdade.

### B1. Checagem de razão nas âncoras

Por `(grão, produto)`: `razao_y = Censo_y / PAM_y` para `y ∈ {2006, 2017}`.

- Esperar razões dentro de uma banda plausível e aproximadamente estáveis.
  Construir a **razão mediana entre produtos por ano** como baseline esperado;
  sinalizar produtos cujo `ln(razao)` desvia fortemente (z-score MAD entre
  produtos).
- **Divergência entre âncoras**: `|ln(razao_2017) − ln(razao_2006)|` grande → uma
  das âncoras ou a série está errada.

### B2. Checagem de envelope / bracketing

O período de referência do Censo pode não alinhar com um único ano-calendário.
Comparar `Censo_2006` a uma **janela PAM local** (mediana de PAM 2005–2007) e
`Censo_2017` a 2016–2018. Desvio `= ln(Censo / mediana_PAM_local)`; z robusto
entre produtos. Robusto a ruído de ano único da pesquisa.

### B3. Consistência de inclinação entre as duas âncoras  ← a métrica pedida

Crescimento de longo prazo por fonte:

- `g_censo = ln(Censo_2017 / Censo_2006)`
- `g_pam   = ln(PAM_2017 / PAM_2006)`
- **Discrepância `D = g_censo − g_pam`.** Se ambas as fontes são válidas, a
  tendência 2006→2017 deve concordar em sinal e magnitude aproximada. `|D|` grande
  sinaliza inconsistência — os dois pontos censitários efetivamente **validam a
  inclinação da série PAM** entre as âncoras.

### B4. Coerência de preço entre fontes

`p_censo = Censo_valor/Censo_qtd` vs `p_pam = PAM_valor/PAM_qtd` no mesmo ano
(ambos 1000xBRL/ton). Devem coincidir independentemente de diferenças de
nível/metodologia (preço é razão). Divergência → problema de unidade isolado em
uma fonte. Forte e independente de B1–B3.

### B5. Sanidade por correlação de postos

Dentro de `(grão, ano ∈ {2006,2017})`, correlação de Spearman dos produtos
ordenados por quantidade entre Censo e PAM. Correlação baixa → desalinhamento
**sistemático** de mapeamento/agregação (ex.: erros de crosswalk) em vez de
anomalia produto-a-produto.

---

## Layout do relatório (para a etapa de implementação)

Script efêmero (ex.: `dados/gold/pa_indexadores_producao_rural/validacao_series.py`,
executável via `uv run python -m ...`) que:

- lê as tabelas gold com `PostgresETL.download_data`
  (`dados/raw/utils/postgres_interactions.py`);
- reutiliza `_common.fetch_regioes_integracao` / `enrich_with_regiao` para o grão
  RI;
- registra logs via `get_logger` (`dados/utils/logging.py`);
- emite:
  - `relatorio_series_pam_pevs.csv` — uma linha por
    `(grão, produto, ano, métrica)` sinalizado, com
    `M_qtd, M_val, ρ, z_preço, quebra_identidade, severity`, ranqueado desc;
  - `relatorio_censo_vs_series.csv` — uma linha por `(grão, produto)` com
    `razao_2006, razao_2017, divergencia_ancoras, D (inclinação),
    coerencia_preco, spearman`;
  - um resumo markdown curto (top-N por fonte).
- `pandas` + `numpy` puros na v1 (sem novas dependências pesadas).

---

## Calibração contra casos conhecidos (verificação)

- **Banana pré-2001 "mil cachos"** (município `1503705`, fator 10,20 kg/fruto,
  de `silver/al_ibge_pam/readme.md`): deve acender A1/A2/A4 quando rodado sobre
  dados **pré-correção** e ficar limpo nos dados pós-correção.
- **PEVS madeira-em-tora m³→ton** (âncora em `silver/al_ibge_pevs/readme.md`):
  idem.
- **Spot-check** das linhas de maior severidade manualmente contra o IBGE SIDRA.
- Nenhuma tabela gold-derivada-de-gold é criada → regra de camadas respeitada.

## Itens em aberto (confirmar na implementação)

- Se `numpy` já está disponível (senão, adicionar ao `pyproject.toml`).
- Pesos finais de severidade e a banda transversal de preço (calibrar nos casos
  conhecidos).
- Se o crosswalk de produtos já existe em `dados/silver/constants/produtos.py` ou
  precisa ser derivado para a Parte B.
