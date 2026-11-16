# -*- coding: utf-8 -*-

# Dicionários para padronizar os produtos do IBGE para o padrão do projeto
# NOTE: É comum que o nome dos produtos seja acompanhado de
# NOTE: informações adicionais entre parênteses, como "semente", "fruto", etc.
#! Defini o padrão  {produto}-{informação adicional}

# NOTE: Quando existem produtos com especificações diferentes
# ex. PAM : Erva-mate (folha verde)
# ex. Censo Agro 2006 - 2017 : Erva-mate
# A padronização do nome foi feita considerando o censo como referência

# NOTE: Regra para produto novo em PAM/PEVS (safra 2025 trouxe 24 deles):
# se o censo já coleta a mesma espécie/parte, reusa-se EXATAMENTE o nome
# padronizado do dicionário do censo correspondente — é isso que permite o
# casamento censo x série anual no gold. Só quando o censo não coleta o produto
# é que se cria um nome novo, no padrão {produto}-{informação adicional}.
# Sem par no censo hoje: "cajá-fruto" (PEVS) e as olerícolas da PAM temporária
# (alface, cenoura, chuchu, inhame, milho verde, morango, pimentão, repolho),
# que o Censo Agro apura em tabela própria, fora de 6957/2337.


### EXTRAÇÃO VEGETAL

# Densidade básica média de madeira (t/m³), usada para converter produtos da PEVS
# medidos em m³ (lenha, madeira em tora, nó-de-pinho) para toneladas no silver.
PEVS_DENSIDADE_TON_M3 = 0.5

# --- Censo Agropecuário (2006/2017): unidades nativas que não são Toneladas ---
# A quantidade dos produtos madeireiros da extração vegetal (lenha, madeira em
# toras) vem em "Mil metros cúbicos". Fator p/ toneladas = 1000 m³ × densidade.
CENSO_FATOR_MIL_M3_TON = 1000 * PEVS_DENSIDADE_TON_M3  # 500 t por mil m³

# Frutos medidos em "Mil frutos" no censo (coco-da-baía, graviola, jaca na lavoura
# permanente; abacaxi na temporária). Peso médio por fruto (kg). Como o valor já
# está em milhares de frutos, toneladas = quantidade × kg_por_fruto. Médias
# nacionais aproximadas (o IBGE calcula pesos médios por UF); chaveado pelo nome
# padronizado do produto (pós-`dicionario_produtos_censo_*`).
CENSO_KG_POR_FRUTO = {
    "abacaxi": 1.5,
    "coco da baía": 1.3,
    "graviola": 2.0,
    "jaca": 8.0,
}

# NOTE: A PEVS é chaveada por ``id_produto`` (id da categoria da classificação
# 193 do agregado 289), e não pelo rótulo. O IBGE renumera os rótulos quando
# insere produtos novos — em 2025, por exemplo, "Castanha-de-caju" passou de
# "1.2" para "1.5" —, o que quebrava silenciosamente o mapeamento por nome.
# Os ids são estáveis. O comentário ao lado de cada entrada traz o rótulo
# vigente, apenas para leitura humana.
dicionario_produtos_pevs = {
    "0": "total",  # Total
    "3402": "1-alimentícios",  # 1 - Alimentícios
    "3403": "açaí-fruto",  # 1.1 - Açaí (fruto)
    "83410": "baru-amêndoa",  # 1.2 - Baru ou Cumaru (amêndoa)
    "83411": "buriti-coco",  # 1.3 - Buriti (fruto)
    "83412": "cajá-fruto",  # 1.4 - Cajá (fruto)
    "3404": "castanha de caju",  # 1.5 - Castanha-de-caju
    "3405": "castanha do pará",  # 1.6 - Castanha-do-pará
    "83413": "cupuaçu",  # 1.7 - Cupuaçu (fruto)
    "3406": "erva-mate",  # 1.8 - Erva-mate
    "83414": "juçara-fruto",  # 1.9 - Juçara (fruto)
    "83415": "macaúba-fruto",  # 1.10 - Macaúba (fruto)
    "3407": "mangaba-fruto",  # 1.11 - Mangaba (fruto)
    "3408": "palmito",  # 1.12 - Palmito
    "39409": "pequi-fruto",  # 1.13 - Pequi (fruto)
    "3409": "pinhão",  # 1.14 - Pinhão
    "83416": "pupunha-coco",  # 1.15 - Pupunha (fruto)
    "83417": "tucumã",  # 1.16 - Tucumã ou Tucum (fruto)
    "3410": "umbu",  # 1.17 - Umbu (fruto)
    "11296": "outros alimentícios",  # 1.18 - Outros
    "3411": "2-aromáticos, medicinais e corantes",  # 2 - Aromáticos, medicinais, tóxicos e corantes
    "3412": "ipecacuanha-raiz",  # 2.1 - Ipecacuanha ou poaia (raiz)
    "3413": "jaborandi",  # 2.2 - Jaborandi (folha)
    "3414": "urucum",  # 2.3 - Urucum (semente)
    "3415": "outros aromáticos, medicinais e corantes",  # 2.4 - Outros
    "3416": "3-borrachas",  # 3 - Borrachas
    "3417": "caucho",  # 3.1 - Caucho
    "3418": "borracha-latéx coagulado",  # 3.2 - Hevea (látex coagulado)
    "3419": "borracha-latéx líquido",  # 3.3 - Hevea (látex líquido)
    "40524": "mangabeira",  # 3.4 - Mangabeira
    "3420": "4-ceras",  # 4 - Ceras
    "3421": "carnaúba-cera",  # 4.1 - Carnaúba (cera)
    "3422": "carnaúba-pó",  # 4.2 - Carnaúba (pó)
    "110011": "outras ceras",  # 4.3 - Outras
    "3423": "5-fibras",  # 5 - Fibras
    "3424": "buriti",  # 5.1 - Buriti (fibra)
    "3425": "carnaúba",  # 5.2 - Carnaúba (fibra)
    "3426": "piaçava",  # 5.3 - Piaçava
    "3427": "outras fibras",  # 5.4 - Outras
    "3428": "6-gomas não elásticas",  # 6 - Gomas não elásticas
    "3429": "balata",  # 6.1 - Balata
    "3430": "maçaranduba",  # 6.2 - Maçaranduba
    "3431": "sorva",  # 6.3 - Sorva
    "3433": "carvão vegetal",  # 7.1 - Carvão vegetal
    "3434": "lenha",  # 7.2 - Lenha
    "3435": "madeira-tora",  # 7.3 - Madeira em tora
    "3438": "8-oleaginosos",  # 8 - Oleaginosos
    "83418": "andiroba-semente",  # 8.1 - Andiroba (amêndoa)
    "3439": "babaçu-amêndoa",  # 8.2 - Babaçu (amêndoa)
    "3440": "copaíba-óleo",  # 8.3 - Copaíba (óleo)
    "3441": "cumaru-amêndoa",  # 8.4 - Cumaru (amêndoa)
    "3442": "licuri-coquilho",  # 8.5 - Licuri (coquilho)
    "83419": "murumuru-semente",  # 8.6 - Murumuru
    "3443": "oiticica",  # 8.7 - Oiticica (semente)
    "3444": "pequi-amêndoa",  # 8.8 - Pequi (amêndoa)
    "3445": "tucum-amêndoa",  # 8.9 - Tucum (amêndoa)
    "3446": "outros oleaginosos",  # 8.10 - Outros
    "3448": "pinheiro brasileiro-nó de pinho",  # 9.1
    "3449": "pinheiro brasileiro-árvores abatidas",  # 9.2
    "3450": "pinheiro brasileiro-madeira em tora",  # 9.3
    "3451": "10-tanantes",  # 10 - Tanantes
    "3452": "angico-casca",  # 10.1 - Angico (casca)
    "3453": "barbatimão-casca",  # 10.2 - Barbatimão (casca)
    "3454": "outros tanantes",  # 10.3 - Outros
}

dicionario_produtos_censo_6949_2233 = {
    "Andiroba (semente)": "andiroba-semente",
    "Araticum (fruto)": "araticum-fruto",
    "Açaí (fruto)": "açaí-fruto",
    "Babaçu (amêndoa)": "babaçu-amêndoa",
    "Babaçu (coco)": "babaçu-coco",
    "Bacaba (fruto)": "bacaba-fruto",
    "Bacuri": "bacuri",
    "Baru (amêndoa)": "baru-amêndoa",
    "Borracha (látex coagulado)": "borracha-latéx coagulado",
    "Borracha (látex líquido)": "borracha-latéx líquido",
    "Buriti (coco)": "buriti-coco",
    "Buriti (palha)": "buriti-palha",
    "Butiá (fibra)": "butiá-fibra",
    "Cacau (amêndoa)": "cacau-amêndoa",
    "Cagaita (fruto)": "cagaita-fruto",
    "Cajarana": "cajarana",
    "Camu-camu (fruto)": "camu camu-fruto",
    "Carnaúba (cera)": "carnaúba-cera",
    "Carnaúba (pó de palha) - Kg": "carnaúba-pó de palha",
    "Casca de angico": "angico-casca",
    "Castanha-do-Brasil (castanha-do-Pará)": "castanha do pará",
    "Caucho (goma elástica)": "caucho-goma elástica",
    "Copaíba (óleo)": "copaíba-óleo",
    "Cumaru (semente)": "cumaru-semente",
    "Cupuaçu": "cupuaçu",
    "Erva-mate": "erva-mate",
    "Imbú ou umbú": "umbu",
    "Ipecacuanha (raiz)": "ipecacuanha-raiz",
    "Jaborandi (folha)": "jaborandi-folha",
    "Jambu (folha)": "jambu-folha",
    "Juçara (fruto)": "juçara-fruto",
    "Lenha": "lenha",
    "Licuri (cera)": "licuri-cera",
    "Licuri (coquilho)": "licuri-coquilho",
    "Macaúba (fruto)": "macaúba-fruto",
    "Madeira em toras outra finalidade": "madeira em toras-outra finalidade",
    "Madeira em toras para papel": "madeira em toras-para papel",
    "Mangaba (fruto)": "mangaba-fruto",
    "Maniçoba (goma elástica)": "maniçoba-goma elástica",
    "Maçaranduba (goma não elástica)": "maçaranduba",
    "Murici": "murici",
    "Murumuru (semente)": "murumuru-semente",
    "Oiticica (semente)": "oiticica-semente",
    "Outros produtos": "outros produtos",
    "Palmito": "palmito",
    "Pequi": "pequi",
    "Piaçava (fibra)": "piaçava-fibra",
    "Pinhão": "pinhão",
    "Pupunha (coco)": "pupunha-coco",
    "Sorva (goma não elástica)": "sorva",
    "Total": "total",
    "Tucumã": "tucumã",
    "Ucuuba (amêndoa)": "ucuuba-amêndoa",
}

### lAVOURA PERMANENTE

dicionario_produtos_pam_permanente = {
    "Abacate": "abacate",
    "Acerola": "acerola",
    "Açaí": "açaí-fruto",
    "Algodão arbóreo (em caroço)": "algodão arbóreo",
    "Azeitona": "azeitona",
    "Banana (cacho)": "banana-cacho",
    "Borracha (látex coagulado)": "borracha-latéx coagulado",
    "Borracha (látex líquido)": "borracha-latéx líquido",
    "Cacau (em amêndoa)": "cacau-amêndoa",
    "Café (em grão) Arábica": "café arábica-grão",
    "Café (em grão) Canephora": "café canephora-grão",
    "Café (em grão) Total": "café total-grão",
    "Caju": "caju",
    "Caqui": "caqui",
    "Castanha de caju": "castanha de caju",
    "Chá-da-índia (folha verde)": "chá da índia",
    "Coco-da-baía*": "coco da baía",
    "Cupuaçu": "cupuaçu",
    "Dendê (cacho de coco)": "dendê-coco",
    "Erva-mate (folha verde)": "erva mate",
    "Figo": "figo",
    "Goiaba": "goiaba",
    "Graviola": "graviola",
    "Guaraná (semente)": "guaraná",
    "Laranja": "laranja",
    "Limão": "limão",
    "Maçã": "maçã",
    "Mamão": "mamão",
    "Manga": "manga",
    "Maracujá": "maracujá",
    "Marmelo": "marmelo",
    "Noz (fruto seco)": "noz",
    "Palmito": "palmito",
    "Pera": "pera",
    "Pêssego": "pêssego",
    "Pimenta-do-reino": "pimenta do reino",
    "Sisal ou agave (fibra)": "agave, sisal-fibra",
    "Tangerina": "tangerina",
    "Total": "total",
    "Tungue (fruto seco)": "tungue-fruto seco",
    "Urucum (semente)": "urucum-semente",
    "Uva": "uva",
}

dicionario_produtos_censo_6955_2518 = {
    "Abacate": "abacate",
    "Açaí (fruto)": "açaí-fruto",
    "Acerola": "acerola",
    "Agave, sisal (fibra)": "agave, sisal-fibra",
    "Agave, sisal (folha)": "agave, sisal-folha",
    "Algodão arbóreo": "algodão arbóreo",
    "Ameixa": "ameixa",
    "Amora (folha)": "amora-folha",
    "Amora (fruto)": "amora-fruto",
    "Atemoia": "atemoia",
    "Azeitona (oliveira)": "azeitona",
    "Banana": "banana",
    "Borracha (látex coagulado)": "borracha-latéx coagulado",
    "Borracha (látex líquido)": "borracha-latéx líquido",
    "Cacau (amêndoa)": "cacau-amêndoa",
    "Café arábica em grão (verde)": "café arábica-grão",
    "Café canephora (robusta, conilon) em grão (verde)": "café canephora-grão",
    "Caju (castanha)": "caju-castanha",
    "Caju (fruto)": "caju-fruto",
    "Camu-camu": "camu-camu",
    "Caqui": "caqui",
    "Carambola": "carambola",
    "Chá-da-Índia": "chá da índia",
    "Coco-da-baía": "coco da baía",
    "Cravo-da-Índia": "cravo da índia",
    "Cupuaçu": "cupuaçu",
    "Dendê (coco)": "dendê-coco",
    "Erva-mate": "erva-mate",
    "Figo": "figo",
    "Fruta-de-conde": "fruta-de-conde",
    "Goiaba": "goiaba",
    "Graviola": "graviola",
    "Guaraná": "guaraná",
    "Jabuticaba": "jabuticaba",
    "Jaca": "jaca",
    "Jambo": "jambo",
    "kiwi": "kiwi",
    "Laranja": "laranja",
    "Lichia": "lichia",
    "Lima": "lima",
    "Limão": "limão",
    "Louro (folha)": "louro-folha",
    "Maçã": "maçã",
    "Mamão": "mamão",
    "Manga": "manga",
    "Maracujá": "maracujá",
    "Mudas de cacau": "cacau-mudas",
    "Mudas de café": "café-mudas",
    "Mudas de caju": "caju-mudas",
    "Mudas de coco-da-baía": "coco da baía-mudas",
    "Mudas de frutas cítricas (laranja, limão, tangerina, etc.)": "frutas cítricas-mudas",
    "Mudas de mamão": "mamão-mudas",
    "Mudas de outros produtos da lavoura permanente": "outros produtos lavoura permanente-mudas",
    "Mudas de uva": "uva-mudas",
    "Nectarina": "nectarina",
    "Nêspera": "nêspera",
    "Noz (europeia, pecã)": "noz",
    "Outros produtos": "outros-produtos",
    "Palmito": "palmito",
    "Pera": "pera",
    "Pêssego": "pêssego",
    "Pimenta-do-reino": "pimenta-do-reino",
    "Pitaia": "pitaia",
    "Pitanga": "pitanga",
    "Pupunha (cacho frutos)": "pupunha-cacho",
    "Romã": "romã",
    "Tangerina, bergamota, mexerica": "tangerina",
    "Total": "total",
    "Urucum (semente)": "urucum-semente",
    "Uva (mesa)": "uva-mesa",
    "Uva (vinho ou suco)": "uva-vinho",
}


### lAVOURA TEMPORÁRIA
dicionario_produtos_pam_temporaria = {
    "Abacaxi*": "abacaxi",
    "Abóbora": "abóbora",
    "Alface": "alface",
    "Alfafa fenada": "alfafa-fenada",
    "Algodão herbáceo (em caroço)": "algodão herbáceo",
    "Alho": "alho",
    "Amendoim (em casca)": "amendoim-casca",
    "Arroz (em casca)": "arroz-casca",
    "Aveia (em grão)": "aveia-grão",
    "Batata-doce": "batata-doce",
    "Batata-inglesa": "batata inglesa",
    "Cana-de-açúcar": "cana de açúcar",
    "Cana para forragem": "cana forrageira",
    "Canola": "colza-canola",
    "Cebola": "cebola",
    "Cenoura": "cenoura",
    "Centeio (em grão)": "centeio-grão",
    "Cevada (em grão)": "cevada-grão",
    "Chuchu": "chuchu",
    "Ervilha (em grão)": "ervilha-grão",
    "Fava (em grão)": "fava-grão",
    "Feijão (em grão)": "feijão-grão",
    "Fumo (em folha)": "fumo-folha",
    "Gergelim": "gergelim-semente",
    "Girassol (em grão)": "girassol-grão",
    "Inhame": "inhame",
    "Juta (fibra)": "juta-fibra",
    "Linho (semente)": "linho-semente",
    "Malva (fibra)": "malva-fibra",
    "Mamona (baga)": "mamona-baga",
    "Mandioca": "mandioca",
    "Melancia": "melancia",
    "Melão": "melão",
    "Milho (em grão)": "milho-grão",
    "Milho verde": "milho verde",
    "Morango": "morango",
    "Pimentão": "pimentão",
    "Rami (fibra)": "rami-fibra",
    "Repolho": "repolho",
    "Soja (em grão)": "soja-grão",
    "Sorgo (em grão)": "sorgo-grão",
    "Tomate": "tomate",
    "Total": "total",
    "Trigo (em grão)": "trigo-grão",
    "Triticale (em grão)": "triticale-grão",
}


dicionario_produtos_censo_6957_2337 = {
    "Abacaxi": "abacaxi",
    "Abóbora, moranga, jerimum": "abóbora",
    "Algodão herbáceo": "algodão herbáceo",
    "Alho": "alho",
    "Amendoim em casca": "amendoim-casca",
    "Arroz em casca": "arroz-casca",
    "Aveia branca em grão": "aveia-grão",
    "Batata-inglesa": "batata inglesa",
    "Cana-de-açúcar": "cana de açúcar",
    "Cana forrageira": "cana forrageira",
    "Cebola": "cebola",
    "Centeio em grão": "centeio-grão",
    "Cevada em casca": "cevada-casca",
    "Colza (canola)": "colza-canola",
    "Ervilha em grão": "ervilha-grão",
    "Fava em grão": "fava-grão",
    "Feijão de cor em grão": "feijão de cor-grão",
    "Feijão fradinho em grão": "feijão fradinho-grão",
    "Feijão preto em grão": "feijão preto-grão",
    "Feijão verde": "feijão verde",
    "Forrageiras para corte": "forrageiras para corte",
    "Fumo em folha seca": "fumo-folha",
    "Gergelim (semente)": "gergelim-semente",
    "Girassol (semente)": "girassol-semente",
    "Juta (fibra)": "juta-fibra",
    "Linho (fibra)": "linho-fibra",
    "Malva (fibra)": "malva-fibra",
    "Mamona": "mamona",
    "Mandioca (aipim, macaxeira)": "mandioca",
    "Melancia": "melancia",
    "Melão": "melão",
    "Milho em grão": "milho-grão",
    "Milho forrageiro": "milho-forrageiro",
    "Outros produtos": "outros produtos",
    "Palma forrageira": "palma-forrageira",
    "Rami (fibra)": "rami-fibra",
    "Sementes de algodão (produzidas para plantio)": "algodão-sementes",
    "Sementes de arroz (produzidas para plantio)": "arroz-sementes",
    "Sementes de batata-inglesa (produzidas para plantio)": "batata-sementes",
    "Sementes de feijão (produzidas para plantio)": "feijão-sementes",
    "Sementes de forrageiras (produzidas para plantio)": "forrageiras-sementes",
    "Sementes de milho (produzidas para plantio)": "milho-sementes",
    "Sementes de soja (produzidas para plantio)": "soja-sementes",
    "Sementes de trigo (produzidas para plantio)": "trigo-sementes",
    "Sementes e outras formas de propagação de outros produtos (produzidas para plantio)": "outros-sementes",
    "Soja em grão": "soja-grão",
    "Sorgo em grão": "sorgo-grão",
    "Sorgo forrageiro": "sorgo-forrageiro",
    "Sorgo vassoura": "sorgo-vassoura",
    "Toletes de cana-de-açúcar (produzidas para plantio)": "cana-toletes",
    "Tomate rasteiro (industrial)": "tomate rasteiro-industrial",
    "Total": "total",
    "Trigo em grão": "trigo-grão",
    "Trigo preto em grão": "trigo-preto",
    "Triticale em grão": "triticale-grão",
}
