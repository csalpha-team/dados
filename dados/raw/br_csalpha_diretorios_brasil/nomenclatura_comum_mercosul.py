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

# Querying data
#NOTE: faz sentido selecionar a tbl completa e filtrar essa para a zona agregada;
#o diretorios precisam ser completos. 
#! modificar aqui
query = """
select
 *
FROM basedosdados.br_bd_diretorios_mundo.nomenclatura_comum_mercosul;

"""



print('Downloading data...')

df = bd.read_sql(
    query = query,
    billing_project_id=ID
)

print('loading data to postgres')
-- Nova estrutura incluindo prodlist da pesca
WITH industria_pesca AS (
    SELECT 
        cnae.classe, 
        cnae.descricao_classe, 
        cnae.subclasse, 
        cnae.descricao_subclasse, 
        cnae.divisao, 
        cnae.descricao_divisao,
        prodlist.id_prodlist,
        prodlist.descricao_prodlist,
        prodlist.id_ncm
    FROM br_csalpha_diretorios_brasil.cnae_2_bioeconomia AS cnae
    INNER JOIN (
        SELECT id_prodlist, descricao_prodlist, id_ncm, id_cnae
        FROM br_csalpha_diretorios_brasil.prodlist_industria
        UNION ALL
        SELECT id_prodlist, descricao_prodlist, id_ncm, id_cnae_classe AS id_cnae
        FROM br_csalpha_diretorios_brasil.prodlist_pesca
    ) AS prodlist
    ON cnae.classe = prodlist.id_cnae
)
SELECT
	distinct
    industria_pesca.*,
    ncm.nome_ncm_portugues AS descricao_ncm_portugues
FROM industria_pesca
INNER JOIN br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul AS ncm
ON industria_pesca.id_ncm = ncm.id_ncm;


-- União dos resultados das consultas pelos produtos CNAE (indústria, pesca e agro)
WITH industria AS (
    SELECT * FROM br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul
    WHERE id_ncm IN (
        SELECT id_ncm FROM br_csalpha_diretorios_brasil.prodlist_industria
        WHERE id_cnae IN (
            SELECT classe FROM br_csalpha_diretorios_brasil.cnae_2_bioeconomia
        )
    )
),
pesca_agro AS (
    SELECT * FROM br_csalpha_diretorios_brasil.nomenclatura_comum_mercosul
    WHERE id_ncm IN (
        SELECT id_ncm FROM br_csalpha_diretorios_brasil.prodlist_pesca
        WHERE id_cnae_classe IN (
            SELECT classe FROM br_csalpha_diretorios_brasil.cnae_2_bioeconomia
        )
    )
)
SELECT * FROM industria
UNION
SELECT * FROM pesca_agro;

with PostgresETL(
  host='localhost', 
  database=os.getenv("DB_TRUSTED_ZONE"), 
  user=os.getenv("POSTGRES_USER"), 
  password=os.getenv("POSTGRES_PASSWORD"),
  schema='br_csalpha_diretorios_brasil') as db:
    
    columns = {
        "id_ncm": "VARCHAR(256)",
        "id_unidade": "VARCHAR(256)",
        "id_sh6": "VARCHAR(256)",
        "id_ppe": "VARCHAR(256)",
        "id_ppi": "VARCHAR(256)",
        "id_fator_agregado_ncm": "VARCHAR(256)",
        "id_cgce_n3": "VARCHAR(256)",
        "id_isic_classe": "VARCHAR(256)",
        "id_siit": "VARCHAR(256)",
        "id_cuci_item": "VARCHAR(256)",
        "nome_unidade": "TEXT",
        "nome_ncm_portugues": "TEXT",
        "nome_ncm_espanhol": "TEXT",
        "nome_ncm_ingles": "TEXT"
    }
          
    db.create_table('nomenclatura_comum_mercosul', columns, if_not_exists=True)
    
    db.load_data('nomenclatura_comum_mercosul', df, if_exists='append')


print('Data loaded')


