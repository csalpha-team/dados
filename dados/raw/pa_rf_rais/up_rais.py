
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

##Setores atacaditas e varejistas com dinâmicas similares

## número de vínculos
## Massa salaria (nvinc * remnueracao media)
produtos_csalpha = {
    'Abacate': {
        'AFIndustBenef' : ['1031700'],
        },
    'Abacaxi': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Abobora': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'AcaiCaroco':  {
        'AFIndustBenef' : ['1061901'],
        },
    'AcaiFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Acerola': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Amendoim': {
        'AFIndustBenef' : ['1041400'],
        },
    'AndirobaFruto': {
        'AFIndustBenef' : ['1041400'],
        },
    'Arroz': {
        'AFIndustBenef' : ['1061901'],
        },
    'BacabaCaroco': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'BacabaFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'BacuriFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Banana': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'BorrachaLatex': {
        'AFIndustBenef' : ['1359600',]
        },
    'BuritiFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'CacauAmendoa': {
        'AFIndustBenef' : ['1093701'],
        },
    'CajaranaFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'CajuAcuFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'CanaDeAcucar': {
        'AFIndustBenef' : ['1071600',],
        },
    'CastanhaDeCaju': {
        'AFIndustBenef' : ['1031700',],
        },
    'CastanhaDoBrasil': {
        'AFIndustBenef' : ['1031700', ],
        },
    'CastanhaDoPara': {
        'AFIndustBenef' : ['1031700',],
        },
    'Coco': {
        'AFIndustBenef' : ['1033302'],
        },
    'CopaibaOleo': {
        'AFIndustBenef' : ['1041400'],
        },
    'CupuacuFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Fava': {
        'AFIndustBenef' : ['1032599',],
        },
    'Feijao': {
        'AFIndustBenef' : ['1032599',],
        },
    'Goiaba': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Laranja': {
        'AFIndustBenef' : [ '1033302'],
        },
    'Lenha': {
        'AFIndustBenef' : ['1610204','3101200'],
        },
    'Lima': {
        'AFIndustBenef' : ['25438'],
        },
    'Limao': {
        'AFIndustBenef' : ['1033302'],
        },
    'MadeiraBranca': {
        'AFIndustBenef' : ['1610204','3101200'],
        },
    'MadeiraVermelha': {
        'AFIndustBenef' : ['1610204','3101200'],
        },
    'Malva': {
        'AFIndustBenef' : ['1312000','1322700'],
        },
    'Mamao': {
        'AFIndustBenef' : ['1031700',],
        },
    'Mandioca': {
        'AFIndustBenef' : ['1063500'],
        },
    'Maracuja': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Melancia': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Milho': {
        'AFIndustBenef' : ['1041400','1064300'],
        },
    'MuriciFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'MuruciFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Murumuru': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'OleoCopaiba': {
        'AFIndustBenef' : ['1041400'],
        },
    'PalmitoInNatura': {
        'AFIndustBenef' : ['1032501'],
        },
    'PimentaDoReino': {
        'AFIndustBenef' : ['1095300'],
        },
    'SementeDeCumaru': {
        'AFIndustBenef' : ['1031700',],
        },
    'Soja': {
        'AFIndustBenef' : ['1041400','1042200'],
        },
    'Tomate': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'TucumaFruto': {
        'AFIndustBenef' : ['1031700', '1033302'],
        },
    'Ucuuba':  {
        'AFIndustBenef' : ['1031700'],
        },
    'Urucum':  {
        'AFIndustBenef' : ['1095300'],
        },
}





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

#### Output
#1. Serie histórica do emprego e/ou massa salarial do setor para regiao x

#1. Filtrar produto que equivale ao setor A (exemplo: 1031700 - polpa de açaí)
#2. Filtrar ano - A série deve iniciar no ano da MIP

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
), t2 as (
SELECT
    dados.ano as ano,
    dados.id_municipio AS id_municipio,
    CASE 
        -- RI Araguaia
        WHEN id_municipio IN ('1500347', '1501253', '1502707', '1502764', '1503044', '1505437', '1505551', '1506138', '1506161', '1506583', '1506708', '1507300', '1507755', '1508084', '1508308') THEN 'Araguaia'
        
        -- RI Baixo Amazonas
        WHEN id_municipio IN ('1500404', '1500503', '1501451', '1502855', '1503002', '1503903', '1504752', '1504802', '1505007', '1505106', '1506005', '1506807', '1507979') THEN 'Baixo Amazonas'
        
        -- RI Carajás
        WHEN id_municipio IN ('1501576', '1501758', '1502152', '1502772', '1502954', '1504208', '1505494', '1505536', '1505635', '1507151', '1507458', '1507508') THEN 'Carajás'
        
        -- RI Guajará
        WHEN id_municipio IN ('1501402', '1500800', '1501501', '1504422', '1506351') THEN 'Guajará'
        
        -- RI Guamá
        WHEN id_municipio IN ('1502400', '1502608', '1502905', '1503200', '1503408', '1504000', '1504307', '1504406', '1506500', '1506609', '1507003', '1507102', '1507201', '1507409', '1507466', '1507607', '1507961', '1508209') THEN 'Guamá'
        
        -- RI Lago de Tucuruí
        WHEN id_municipio IN ('1501782', '1503093', '1503705', '1503804', '1504976', '1505064', '1508100') THEN 'Lago de Tucuruí'
        
        -- RI Marajó
        WHEN id_municipio IN ('1500305', '1500701', '1501105', '1501808', '1502004', '1502509', '1502806', '1503101', '1504505', '1504901', '1505205', '1505700', '1505809', '1506302', '1506401', '1507706', '1507904') THEN 'Marajó'
        
        -- RI Rio Caeté
        WHEN id_municipio IN ('1500909', '1501600', '1501709', '1501956', '1502202', '1505304', '1505601', '1506104', '1506112', '1506203', '1506559', '1506906', '1507474', '1508035', '1508407') THEN 'Rio Caeté'
        
        -- RI Rio Capim
        WHEN id_municipio IN ('1500131', '1500958', '1501907', '1502301', '1502756', '1502939', '1503077', '1503457', '1503507', '1504059', '1504950', '1505403', '1505502', '1506187', '1508001', '1508126') THEN 'Rio Capim'
        
        -- RI Tapajós
        WHEN id_municipio IN ('1501006', '1503606', '1503754', '1505031', '1506195', '1508050') THEN 'Tapajós'
        
        -- RI Tocantins
        WHEN id_municipio IN ('1500107', '1500206', '1501204', '1501303', '1502103', '1503309', '1504109', '1504604', '1504703', '1507953') THEN 'Tocantins'
        
        -- RI Xingu
        WHEN id_municipio IN ('1500602', '1500859', '1501725', '1504455', '1505486', '1505650', '1505908', '1507805', '1508159', '1508357') THEN 'Xingu'
        
        ELSE 'Não classificado'
    END AS regiao_integracao,
    descricao_vinculo_ativo_3112 AS vinculo_ativo_3112,
    dados.valor_remuneracao_dezembro as valor_remuneracao_dezembro,
    dados.cnae_2_subclasse AS cnae_2_subclasse,
    diretorio_cnae_2_subclasse.descricao_subclasse AS cnae_2_subclasse_descricao_subclasse,
    diretorio_cnae_2_subclasse.descricao_secao AS cnae_2_subclasse_descricao_secao
FROM `basedosdados.br_me_rais.microdados_vinculos` AS dados
LEFT JOIN `dicionario_vinculo_ativo_3112`
    ON dados.vinculo_ativo_3112 = chave_vinculo_ativo_3112
LEFT JOIN (SELECT DISTINCT subclasse,descricao_subclasse,descricao_secao  FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`) AS diretorio_cnae_2_subclasse
    ON dados.cnae_2_subclasse = diretorio_cnae_2_subclasse.subclasse

WHERE sigla_uf = 'PA' AND 
ano > 1997 and 
vinculo_ativo_3112 = '1' and cnae_2_subclasse = '1031700')

SELECT
t2.ano,
t2.regiao_integracao,
t2.cnae_2_subclasse,
t2.cnae_2_subclasse_descricao_subclasse,
t2.cnae_2_subclasse_descricao_secao,
ROUND(AVG(valor_remuneracao_dezembro),2) as remuneracao_media_dezembro,
COUNT(*) as quantidade_vinculos_ativos,
ROUND(AVG(valor_remuneracao_dezembro) * COUNT(*),2) as massa_salarial
FROM t2
GROUP BY all;
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



