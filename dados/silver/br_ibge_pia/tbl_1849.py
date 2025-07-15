from dados.raw.utils.postgres_interactions import (
    PostgresETL,
)

from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
TABLE_ID= 'tbl_1849'
DATASET_ID='br_ibge_pia'


query = f"""
select
nome_variavel,
nome_categoria,
nome_localidade,
cast(ano as integer) as ano,
valor
from {DATASET_ID}.{TABLE_ID}
where nivel_nome = 'Unidade da Federação';

"""

with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_RAW_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID) as db:
    
    data = db.download_data(query)
    
data['valor'] = data.valor.apply(lambda x: 0 if x in ("..", "...", "-", 'X') else x)


#pivotar tabela
data = data.pivot_table(
    index=['ano','nome_localidade', 'nome_categoria' ],
    columns=["nome_variavel"],
    values="valor",
    aggfunc="sum"
).reset_index()


cols = {
    "nome_categoria":"divisao_grupo_cnae_2",
    "Custos com consumo de matérias-primas, materiais auxiliares e componentes": "custos_materias_primas",
    "Encargos sociais e trabalhistas, indenizações e benefícios": "encargos_sociais_trabalhistas",
    "Número de unidades locais": "quantidade_unidades_locais",
    "Pessoal ocupado em 31/12": "pessoal_ocupado_31_12",
    "Receita líquida de vendas de atividades industriais": "receita_liquida_vendas_industriais",
    "Receita líquida de vendas de atividades não industriais": "receita_liquida_vendas_nao_industriais",
    "Salários, retiradas e outras remunerações": "valor_salarios_remuneracoes",
    "Total de custos das operações industriais": "valor_custos_operacoes_industriais",
    "Total de custos e despesas": "valor_custos_despesas",
    "Total de receitas líquidas de vendas": "valor_receitas_liquidas_vendas",
    "Valor bruto da produção industrial": "valor_bruto_producao_industrial",
    "Valor da transformação industrial": "valor_transformacao_industrial",
}

#renomear colunas
data.rename(columns=cols, inplace=True)    
    
with PostgresETL(
    host='localhost', 
    database=os.getenv("DB_SILVER_ZONE"), 
    user=os.getenv("POSTGRES_USER"), 
    password=os.getenv("POSTGRES_PASSWORD"),
    schema=DATASET_ID) as db:
        
        
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
 
            
        db.create_table(f'{TABLE_ID}', columns, drop_if_exists=True)
        
        db.load_data(f'{TABLE_ID}', data, if_exists='replace')