# Coeficientes de custo

Esta camada usa o arquivo `parametros_coeficientes_custo.json` para definir
como os itens de despesa do Censo Agropecuario devem ser convertidos nas chaves
de coeficientes usadas pelo modelo. O arquivo nao calcula nada por si so. Ele
funciona como um dicionario de correspondencia entre:

- os grupos de parametros herdados da logica anterior da CSAlpha;
- a lista de itens de custo efetivamente disponivel no Censo Agropecuario.

Em termos praticos, esse JSON registra o "de para" entre os nomes que o modelo
espera consumir e os nomes de despesa encontrados na base de origem.

## Origem das relacoes entre grupos de parametros

As relacoes presentes em `parametros_coeficientes_custo.json` nao surgem de uma
classificacao automatica feita por este ETL. Elas foram definidas a partir da
compatibilizacao entre:

1. os itens de custo usados na logica anterior da CSAlpha;
2. os itens de custo disponibilizados pelo Censo Agropecuario.

Essa compatibilizacao e um agrupamento de insumos. Ela informa quais despesas
observadas no censo devem alimentar cada grupo de parametro do modelo. Alguns
exemplos centrais:

- `InsumoEnergia` <- `Energia elétrica`
- `InsumosCombustível` <- `Combustíveis e lubrificantes`
- `InsumosMecânicos` <- `Aluguel de máquina` e `Compra de máquinas e veículos`

O mesmo principio vale para os demais grupos do arquivo: cada `expense_type`
representa um item do censo, e cada `coeff_key` representa a chave de
coeficiente que sera publicada para consumo pelo modelo.

## Por que esse mapeamento fica no JSON

O mapeamento esta versionado em JSON por desenho. A camada precisa de uma
fonte simples e auditavel para manter explicita a correspondencia entre a
taxonomia antiga e a taxonomia do censo. Isso evita espalhar regras de
renomeacao no codigo e facilita revisar alteracoes quando:

- um item do censo mudar de nome;
- um grupo de parametro ganhar ou perder itens;
- a compatibilizacao entre a logica anterior e a base de origem for revista.

Quando houver revisao metodologica, a mudanca deve ser feita neste arquivo,
preservando a coerencia com as chaves `coeff_keys` consumidas nas etapas
seguintes.

## Como o ETL usa esse arquivo

O fluxo da camada em `preparacao_camada_custo.py` e o seguinte:

1. baixar os dados de despesas do Censo Agropecuario;
2. carregar `parametros_coeficientes_custo.json`;
3. transformar cada relacao `expense_types -> coeff_keys` em um mapa de expansao;
4. calcular o coeficiente de cada tipo de despesa em relacao a `Total`;
5. replicar o valor calculado para cada `coeff_key` associado ao item de despesa;
6. agregar os resultados por regiao de integracao e manter o ano mais recente.

O ponto importante e que o JSON nao guarda coeficientes prontos. Ele guarda a
regra de correspondencia usada para transformar despesas observadas no censo em
`tipo_coeff`.

## Leitura dos grupos de parametros

O bloco `grupos_parametros` esta dividido em dois grupos maiores:

- `Custos`: itens correntes de despesa usados para formar coeficientes de custo;
- `Investimento`: itens tratados nesta mesma camada que entram como investimento
  especifico a partir da base de despesas.

Dentro de cada grupo, cada objeto associa uma ou mais chaves do modelo a um ou
mais itens do censo.

## Relacoes atualmente definidas

### Custos

| `coeff_keys` | `expense_types` no Censo Agropecuario | Leitura da relacao |
| --- | --- | --- |
| `CombustíveisBenefEstad`, `CombustíveisBenefLoc` | `Combustíveis` | O item de combustíveis do censo alimenta duas chaves de coeficiente consumidas em outras etapas. |
| `InsumoEnergia` | `Energia elétrica` | O grupo de energia da lógica anterior e representado pelo item de energia elétrica do censo. |
| `InsumosCombustível` | `Combustíveis e lubrificantes` | O grupo de combustíveis e lubrificantes do censo e tratado como insumo de combustível no modelo. |
| `InsumosMecânicos` | `Aluguel de máquina`, `Compra de máquinas e veículos` | O grupo de insumos mecânicos agrega despesas com uso e aquisição de bens mecanizados. |
| `InsumosMineral` | `Corretivos do solo`, `Sal e rações (industrializados ou não-industrializados)`, `Sal, ração e outros suplementos` | O grupo agrega itens minerais e suplementos associados a esse bloco de custo na lógica anterior. |
| `InsumosOrgânicos` | `Sementes e mudas`, `Compra de matéria-prima para agroindústria` | O grupo agrega insumos biológicos e matérias-primas compatibilizados com a taxonomia anterior. |
| `InsumosQuímicos` | `Adubos`, `Agrotóxicos`, `Medicamentos para animais` | O grupo reúne insumos químicos e veterinários tratados conjuntamente no modelo. |
| `EmbalagemBenefEstad`, `EmbalagemBenefLoc`, `EmbalagemTransfEstad`, `EmbalagemTransfLoc` | `Sacarias e embalagens` | Um único item do censo alimenta quatro chaves ligadas a beneficiamento e transformação. |
| `CustosVariáveisDiversosBenefEstad`, `CustosVariáveisDiversosBenefLoc`, `CustosVariáveisDiversosTransfEstad`, `CustosVariáveisDiversosTransfLoc` | `Outras despesas` | O grupo absorve despesas residuais que não entram nas classes anteriores. |
| `ServiçosConstCivilBenefEstad`, `ServiçosConstCivilBenefLoc`, `ServiçosConstCivilTransfEstad`, `ServiçosConstCivilTransfLocal` | `Serviços de empreitada` | O item do censo é propagado para as chaves de serviços ligadas a beneficiamento e transformação. |
| `TransporteBenefEstad`, `TransporteBenefLoc`, `TransporteTransfEstad`, `TransporteTransfLoc` | `Transporte da produção` | O item de transporte do censo é distribuído entre as chaves de transporte usadas pelo modelo. |

### Investimento

| `coeff_keys` | `expense_types` no Censo Agropecuario | Leitura da relacao |
| --- | --- | --- |
| `InvestPlantio` | `Novas culturas permanentes e silvicultura`, `Formação de pastagens` | O grupo de investimento em plantio agrega os itens do censo ligados à implantação e formação de novas áreas. |

## Observacoes de manutencao

- A ordem das entradas no JSON nao muda o calculo, mas manter agrupamentos
  coerentes facilita revisao.
- Uma mesma entrada de despesa pode alimentar mais de uma `coeff_key` quando o
  modelo precisa replicar esse valor para chaves diferentes.
- Se um item do censo for renomeado ou substituido, o ajuste deve ser feito no
  campo `expense_types`, nao no codigo do ETL, salvo quando houver mudanca de
  logica.
- As chaves `coeff_keys` formam um contrato com as camadas consumidoras; por
  isso, renomeacoes devem ser tratadas com cuidado.
