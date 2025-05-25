from dados.raw.utils.postgres_interactions import (
    PostgresETL,
)
from dados.silver.utils import (
    currency_fix,
    fix_ibge_digits,
    check_duplicates,
)

#1. para pesquisas conjunturais


#1. 2233 censo 2006 - extracao vegetal
#2. quantidade estabelecimentos, valor producao....

#1. X .. -....
#1. tudo que nao é X, é convertido 0
#2. identifica onde, municipio, tipo_agricultura, produto, ano tem valores X
#3. se é conjuntural
#3.1 calcula média dos valores de cada municipio pra essa mesma agregacao - ano passado ano fututuo media, 
# o valor mínimo de dados preenchidos na série determina a possibilidade de execução/ verificar se é possível ancorar crescimento por taxa média
# o valor precisa ser comparado com as estatisticas do agredado (dado a nível de UF)
# situacoes de quebra longas podem nao corresponder as series reais do agregadao/ 
#3.2 estrutural
#se existe X
#1. calcula a

#conjuntural - nao tem media legitima
#estrutural - tem média legitima encontrada a partir do valor e frequencia (erro controlado p/ variancia)

#sobre as diferenças estruturais
#se considera que ambos agentes estão submetidos a mesma dinâmica do território/


#Primeiro ver a 


from dotenv import load_dotenv
import os
import pandas as pd
from dados.silver.padronizacao_produtos import (
    dicionario_produtos_pam_permanente
)







load_dotenv()

#TODO: Refatorar lógica do código para processar a pipe inteira em chunks 

query = """
select
nome_variavel,
produto,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_pam.lavoura_permanente
where id_municipio like '15%';
"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_RAW_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pam') as db:
    
    data = db.download_data(query)
    

# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "produto", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:3],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

cols = {
    "Área colhida": "area_colhida",
    "Área destinada à colheita": "area_destinada_colheita",
    "Quantidade produzida" : "quantidade_produzida",
    "Rendimento médio da produção": "rendimento_medio_producao",
    "Valor da produção" : "valor_producao"
}


data.rename(columns=cols, inplace=True)    
print(data.columns)

#Padroniza nome de produtos
data["produto"] = data["produto"].map(dicionario_produtos_pam_permanente)

# conserta dígitos do IBGE
data = fix_ibge_digits(data,list(cols.values()), columns_index[0:3])

# Aplica correção monetária
data["valor_producao"] = data["valor_producao"].astype("float")
data["valor_producao"] = data["valor_producao"].apply(lambda x: currency_fix(x) if isinstance(x, str) else x)

data = data[['ano', 'id_municipio', 'produto', 'quantidade_produzida', 'valor_producao', 
            'area_destinada_colheita',  'area_colhida', 'rendimento_medio_producao']]

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_pam') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'quantidade_produzida': 'numeric',
            'valor_producao': 'numeric',
            'area_destinada_colheita' : 'numeric',
            'area_colhida' : 'numeric',
            'rendimento_medio_producao' : 'numeric',
        }

        db.create_table('lavoura_permanente', columns, drop_if_exists=True)
        
        db.load_data('lavoura_permanente', data, if_exists='replace')