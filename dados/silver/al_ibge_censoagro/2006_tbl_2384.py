from dotenv import load_dotenv
import os
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.silver.utils import (
    fix_ibge_digits,
    check_duplicates,
    calcula_autoconsumo_comercio,
)

from dados.silver.padronizacao_produtos import (
    dicionario_produtos_censo_6957_2337
)

load_dotenv()
TABLE="tbl_2284_2006"

query = f"""
select
nome_variavel,
produto,
tipo_agricultura,
tipo_consumo_estocagem,
id_municipio,
cast(ano as integer) as ano,
valor
from al_ibge_censoagro.{TABLE}
where tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar');
"""


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_RAW_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema='al_ibge_censoagro') as db:
    
    data = db.download_data(query)
    
    

# Checar existência de duplicatas por segurança
columns_index = ["id_municipio", "ano", "produto", "tipo_agricultura", "tipo_consumo_estocagem", "nome_variavel"]

check_duplicates(data, columns_index)

#pivotar tabela
data = data.pivot_table(
    index=columns_index[0:5],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()

# renomear colunas
cols = {
    "Número de estabelecimentos agropecuários": "quantidade_estabelecimentos",
    "Quantidade produzida": "quantidade_produzida",
    "Quantidade vendida": "quantidade_vendida",
    "Valor da produção": "valor_producao",
}

data.rename(columns=cols, inplace=True)    

data['produto'] = data['produto'].map(dicionario_produtos_censo_6957_2337)

# Padroniza tipo agricultura
dicionario_tipo_agricultura = {
    "Agricultura familiar - Lei 11.326": "agricultura familiar",
    "Agricultura não familiar": "agricultura não familiar",
}


data["tipo_agricultura"] = data.tipo_agricultura.map(dicionario_tipo_agricultura)

#Se selecionar tipo_consumo_estocagem = Total tem se o valor total, sem considerar os subgrupos dessa variável;
data = fix_ibge_digits(data,list(cols.values()), ['id_municipio', 'ano', 'produto', 'tipo_agricultura', 'tipo_consumo_estocagem', ], div_column="quantidade_estabelecimentos")

metric_columns = [
    'quantidade_estabelecimentos', 
    'quantidade_produzida', 
    'quantidade_vendida', 
    'valor_producao', 
]

group_keys = ['ano', 'id_municipio', 'produto', 'tipo_agricultura']

# Execução
data_final = calcula_autoconsumo_comercio(
    df=data,
    id_cols=group_keys,
    metric_cols=metric_columns,
    category_col='tipo_consumo_estocagem',
    total_label='Total',
    consumo_label='Consumo no estabelecimento'
)

# Atualiza variável principal
data = data_final.copy()

# Atualiza a variável principal
cols_to_load = [
    'ano', 'id_municipio', 'produto', 'tipo_agricultura',
    
    # Originais (Totais)
    'quantidade_estabelecimentos', 'quantidade_produzida', 
    'quantidade_vendida', 'valor_producao', 
    
    # Autoconsumo (Consumo no estabelecimento)
    'autoconsumo_quantidade_estabelecimentos', 'autoconsumo_quantidade_produzida',
    'autoconsumo_quantidade_vendida', 'autoconsumo_valor_producao', 
    
    # Comércio (Calculado: Total - Autoconsumo)
    'comercio_quantidade_estabelecimentos', 'comercio_quantidade_produzida',
    'comercio_quantidade_vendida', 'comercio_valor_producao',
]

data = data[cols_to_load]

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema='al_ibge_censoagro') as db:
        
        columns_schema = {
            'ano': 'integer',
            'id_municipio': 'VARCHAR(7)',
            'produto': 'VARCHAR(255)',
            'tipo_agricultura': 'VARCHAR(255)',
            
            # Originais
            'quantidade_estabelecimentos': 'integer',
            'quantidade_produzida': 'numeric',
            'quantidade_vendida': 'numeric',
            'valor_producao': 'numeric',

            # Autoconsumo
            'autoconsumo_quantidade_estabelecimentos': 'integer',
            'autoconsumo_quantidade_produzida': 'numeric',
            'autoconsumo_quantidade_vendida': 'numeric',
            'autoconsumo_valor_producao': 'numeric',

            # Comércio
            'comercio_quantidade_estabelecimentos': 'integer',
            'comercio_quantidade_produzida': 'numeric',
            'comercio_quantidade_vendida': 'numeric',
            'comercio_valor_producao': 'numeric',
        }
            
        db.create_table(f'{TABLE}', columns_schema, drop_if_exists=True)
        
        db.load_data(f'{TABLE}', data, if_exists='replace')