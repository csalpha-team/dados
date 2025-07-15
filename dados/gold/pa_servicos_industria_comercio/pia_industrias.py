from dotenv import load_dotenv
import os
import basedosdados as bd
from dados.raw.utils.postgres_interactions import PostgresETL


load_dotenv()

TABLE_ID= 'tbl_1849'
DATASET_ID='br_ibge_pia'


query = f"""
SELECT  
  *
FROM {DATASET_ID}.{TABLE_ID}
where nome_localidade = 'Pará'
"""

with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_SILVER_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID) as db:
    
    data = db.download_data(query)
    


with PostgresETL(
        host='localhost', 
        database=os.getenv("DB_GOLD_ZONE"), 
        user=os.getenv("POSTGRES_USER"), 
        password=os.getenv("POSTGRES_PASSWORD"),
        schema="pa_servicos_industria_comercio") as db:
    
    columns = {
            'ano': 'VARCHAR(255)',
            'nome_localidade': 'VARCHAR(255)',
            'divisao_grupo_cnae_2': 'VARCHAR(255)',
            'custos_materias_primas': 'VARCHAR(255)',
            'encargos_sociais_trabalhistas': 'VARCHAR(255)',
            'quantidade_unidades_locais': 'VARCHAR(255)',
            'pessoal_ocupado_31_12': 'VARCHAR(255)',
            'receita_liquida_vendas_industriais': 'VARCHAR(255)',
            'receita_liquida_vendas_nao_industriais': 'VARCHAR(255)',
            'valor_salarios_remuneracoes': 'VARCHAR(255)',
            'valor_custos_operacoes_industriais': 'VARCHAR(255)',
            'valor_custos_despesas': 'VARCHAR(255)',
            'valor_receitas_liquidas_vendas': 'VARCHAR(255)',
            'valor_bruto_producao_industrial': 'VARCHAR(255)',
            'valor_transformacao_industrial': 'VARCHAR(255)',
        }

    db.create_table('pia_industria', columns, drop_if_exists=True)
    
    db.load_data('pia_industria', data, if_exists='replace')

    

        
      
