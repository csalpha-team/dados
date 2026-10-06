# IPCA mensal — camada raw

Ingere a série mensal `433` do Sistema Gerenciador de Séries Temporais (SGS)
do Banco Central do Brasil. A série representa a variação percentual mensal do
IPCA e é obtida pela interface JSON oficial BCData.

A resposta validada é armazenada em
`tmp_data/br_bcb_sgs_ipca/sgs_433.json`. Se a API estiver indisponível, o fluxo
aceita esse cache somente após repetir as validações de esquema, datas,
duplicidade e continuidade mensal. Valores ou meses ausentes nunca são
substituídos por zero.
