from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
)

from dados.silver.padronizacao_produtos import (
    dicionario_produtos_censo_6955_2518
)


load_dotenv()
TABLE="tbl_2518_2006"

query = f"""
select
nome_variavel,
produto,
tipo_agricultura,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.{TABLE}
where tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar') AND
nome_variavel IN (
'Área colhida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Área plantada nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Número de estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Quantidade produzida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Quantidade vendida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Valor da produção dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12',
'Valor das vendas dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12');
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)


# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "produto", "tipo_agricultura", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:4],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

# renomear colunas
cols = {
    "Área colhida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12" : "area_colhida",
    "Área plantada nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "area_plantada",
    "Número de estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "quantidade_estabelecimentos",
    "Quantidade produzida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "quantidade_produzida",
    "Quantidade vendida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "quantidade_vendida",
    "Valor da produção dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "valor_producao",
    "Valor das vendas dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12": "valor_venda",
}

data.rename(columns=cols, inplace=True)    

data['produto'] = data['produto'].map(dicionario_produtos_censo_6955_2518)

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}


data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)


data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'produto', 'tipo_agricultura'], div_column="quantidade_estabelecimentos")

    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_censoagro') as db:
        
        
        columns = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'tipo_agricultura': 'VARCHAR(255)',
            'quantidade_estabelecimentos': 'integer',
            'quantidade_produzida': 'integer',
            'quantidade_vendida': 'integer',
            'valor_producao': 'numeric',
            'valor_venda' : 'numeric',
            'area_colhida': 'numeric',
            'area_plantada': 'numeric',

        }
            
        db.create_table(f'{TABLE}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')
      