
import basedosdados as bd
import os
from dotenv import load_dotenv
from dados.raw.utils.postgres_interactions import PostgresETL

print('Loading environment variables...')
# Loading .env file
load_dotenv()

# setting up env vars
ID = os.getenv("BASEDOSDADADOS_PROJECT_ID")
ROOT_DIR = os.getenv("ROOT_DIR")
os.chdir(ROOT_DIR)

## número de vínculos
## Massa salaria (nvinc * remnueracao media)



#Calcula a varição do emprego nos setores chave em relacão a variação da produção
#1. Setores chave: 
# nivel municipioal : AindProcessamento, AindTransformação
# nivel estadual: BindProcessamento; BindTransformação
# Cada um dos setores pode ter sua própria proxy de dinâmica

##EX.Para o produto i no subterritório r a  AIndProcessamento (local municipal) poderá ter s
# eu indicador de dinâmica, p. ex., no emprego total registrado na CNAE X(r) (da soma dos
# municípios no subterritório r) e  AIndTransformação na CNAE Y(r) (da soma dos municípios 
# no subterritório r). 


##Por seu turno, BIndProcessamento (estadual) poderá ter sua proxy no emprego
# total registrado na CNAE Xr’, em que r’ são todos
# os demais municípios não-r do território maior; e, do mesmo modo,  
# AIndTransformação na CNAE Yr’. 

##3)O impacto das diferentes dinâmicas será, respectivamente, a taxa de crescimento do emprego da CNAE Xr
# dividido pela taxa de crescimento da produção Qr entre t’ e t, este último o ano para o qual se está produzindo a MIPAlfa de i. 



###
# emprego total registrado na cnae x da soma dos municípios no subterritório y


#ano da mip
#taxar crescimento setor industrial / taxa de crescimento setor alfa (censo/pam)
#ex. taxa de crescimento industria de polpa / taxa de crescimento producao acai fruto

#industr transformacao - industria de beneficiamento

#### n
a tabela da pam
#1. Filtrar produto que equivale ao setor A (exemplo: 1031700 - polpa de açaí)
#2. Filtrar ano - A série deve iniciar no ano da MIP
#3. integrar dados com o censo 2006. 2016. e pam

#### na tabela da rais
#1. Filtrar vinculos ativos (vinculo_ativo_3112 = '1')
#2. Ano - A série deve iniciar no ano da MIP
#1. agregacao -> Setores A precisamos somar os valores da regiao intermediaria a nivel do municipio e calcular a taxa de crescimento
#2. agregacao -> Setores B precisamos somar todos os municipios fora da regiao intermediaria do setor A a nivel do estado e calcular a taxa de crescimento


query = """
WITH 
dicionario_vinculo_ativo_3112 AS (
    SELECT
        chave AS chave_vinculo_ativo_3112,
        valor AS descricao_vinculo_ativo_3112
    FROM `basedosdados.br_me_rais.dicionario`
    WHERE
        TRUE
        AND nome_coluna = 'vinculo_ativo_3112'
        AND id_tabela = 'microdados_vinculos'
)
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    diretorio_id_municipio.nome AS id_municipio_nome,
    descricao_vinculo_ativo_3112 AS vinculo_ativo_3112,
    dados.valor_remuneracao_media as valor_remuneracao_media,
    dados.cnae_2 as cnae_2,
    dados.cnae_2_subclasse AS cnae_2_subclasse,
    diretorio_cnae_2_subclasse.descricao_subclasse AS cnae_2_subclasse_descricao_subclasse,
    diretorio_cnae_2_subclasse.descricao_secao AS cnae_2_subclasse_descricao_secao
FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
LEFT JOIN (SELECT DISTINCT id_municipio,nome  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) AS diretorio_id_municipio
    ON dados.id_municipio = diretorio_id_municipio.id_municipio
LEFT JOIN `dicionario_vinculo_ativo_3112`
    ON dados.vinculo_ativo_3112 = chave_vinculo_ativo_3112
LEFT JOIN (SELECT DISTINCT subclasse,descricao_subclasse,descricao_secao  FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`) AS diretorio_cnae_2_subclasse
    ON dados.cnae_2_subclasse = diretorio_cnae_2_subclasse.subclasse

WHERE sigla_uf = 'PA' AND ano > 2011 and vinculo_ativo_3112 = '1' and cnae_2_subclasse = '1031700';
"""

print('Downloading data...')

df = bd.read_sql(
    query = query,
    billing_project_id=ID
)

print('loading data to postgres')

with PostgresETL(
  host='localhost', 
  database=os.getenv("DB_RAW_ZONE"), 
  user=os.getenv("POSTGRES_USER"), 
  password=os.getenv("POSTGRES_PASSWORD"),
  schema='pa_me_rais') as db:
    
    columns = {
      "ano" : "INTEGER",
      "sigla_uf" : "VARCHAR(2)",
      "id_municipio" : "VARCHAR(7)",
      "id_municipio_nome" : "VARCHAR(256)",
      "vinculo_ativo_3112" : "VARCHAR(3)",
      "valor_remuneracao_media" : "VARCHAR(7)",
      "cnae_2" : "VARCHAR(256)",
      "cnae_2_subclasse" : "VARCHAR(256)",
      "cnae_2_subclasse_descricao_subclasse" : "VARCHAR(7)",
      "cnae_2_subclasse_descricao_secao" : "VARCHAR(7)",
      "diretorio_cnae_2_subclasse" : "VARCHAR(7)",
      
      } 
    
    db.create_table('vinculos', columns, if_not_exists=True)
    
    db.load_data('vinculos', df, if_exists='append')


print('Data loaded')



