from enum import Enum
from pydantic import BaseModel
from typing import List,
#Construi modelo enum para validar e salvar valores selcionados;  
#! Para as cnaes; e ncms; e prodlists;;

# class cnae_2_list(Enum) -> str:
#     # Lista com CNAEs que conterá os dados;


#NOTE:
#? Preciso estruturar um converso de pydantinc para SQL
#testa model_dump()
class ComexStat(BaseModel):
    
    ano: int
    mes: int
    id_ncm: str
    id_unidade: str
    id_pais: str
    sigla_pais_iso3: str
    sigla_uf_ncm: str
    id_via: str
    id_urf: str
    quantidade_estatistica: float
    peso_liquido_kg: float
    valor_fob_dolar: float
    
    
    



