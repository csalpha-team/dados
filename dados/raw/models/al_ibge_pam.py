# datos/raw/models/ibge_pam.py
from typing import Optional, ClassVar
from pydantic import Field
from .base import BaseTable, TableMetadata

class BasePamTable(BaseTable):
    """Base class for all IBGE PAM tables"""
    id_variavel: str = Field(description="ID da variável", json_schema_extra={"sql_type": "VARCHAR(255)"})
    nome_variavel: str = Field(description="Nome da variável", json_schema_extra={"sql_type": "VARCHAR(255)"})
    unidade_medida: str = Field(description="Unidade de medida", json_schema_extra={"sql_type": "VARCHAR(255)"})
    id_produto: str = Field(description="ID do produto", json_schema_extra={"sql_type": "VARCHAR(255)"})
    produto: str = Field(description="Nome do produto", json_schema_extra={"sql_type": "VARCHAR(255)"})
    nome_municipio: str = Field(description="Nome do município", json_schema_extra={"sql_type": "VARCHAR(255)"})
    id_municipio: str = Field(description="ID do município", json_schema_extra={"sql_type": "VARCHAR(7)"})
    ano: str = Field(description="Ano de referência", json_schema_extra={"sql_type": "VARCHAR(255)"})
    valor: str = Field(description="Valor da variável", json_schema_extra={"sql_type": "VARCHAR(255)"})

Class (CustomModel):
    tabela: BasePamTable
    descricao: BaseDescription
    processo: correcao dos valore X; Selecao das colunas y; 
    Unidade_medidade
    
    

class LavouraTemporaria(BasePamTable):
    """IBGE PAM - Lavoura Temporária"""
    
    __metadata__: ClassVar[TableMetadata] = TableMetadata(
        schema="al_ibge_pam",
        table_name="lavoura_temporaria",
        description="Produção Agrícola Municipal - Lavoura Temporária"
    )

class LavouraTemporariaProcessada(BaseTable):
    """IBGE PAM - Lavoura Temporária (Processada diretamente do BD)"""
    ano: int = Field(description="Ano de referência", json_schema_extra={"sql_type": "INTEGER"})
    id_municipio: str = Field(description="ID do município", json_schema_extra={"sql_type": "VARCHAR(7)"})
    produto: str = Field(description="Nome do produto", json_schema_extra={"sql_type": "VARCHAR(100)"})
    area_plantada: float = Field(description="Área plantada (ha)", json_schema_extra={"sql_type": "FLOAT"})
    area_colhida: float = Field(description="Área colhida (ha)", json_schema_extra={"sql_type": "FLOAT"})
    quantidade_produzida: float = Field(description="Quantidade produzida", json_schema_extra={"sql_type": "FLOAT"})
    rendimento_medio_producao: float = Field(description="Rendimento médio da produção", json_schema_extra={"sql_type": "FLOAT"})
    valor_producao: float = Field(description="Valor da produção", json_schema_extra={"sql_type": "FLOAT"})
    
    __metadata__: ClassVar[TableMetadata] = TableMetadata(
        schema="br_ibge_pam",
        table_name="lavoura_temporaria",
        description="Produção Agrícola Municipal - Lavoura Temporária (Processada)"
    )

class LavouraPermenente(BasePamTable):
    """IBGE PAM - Lavoura Permanente"""
    
    __metadata__: ClassVar[TableMetadata] = TableMetadata(
        schema="al_ibge_pam",
        table_name="lavoura_permanente",
        description="Produção Agrícola Municipal - Lavoura Permanente"
    )

class LavouraPermanenteProcessada(BaseTable):
    """IBGE PAM - Lavoura Permanente (Processada diretamente do BD)"""
    ano: int = Field(description="Ano de referência", json_schema_extra={"sql_type": "INTEGER"})
    id_municipio: str = Field(description="ID do município", json_schema_extra={"sql_type": "VARCHAR(7)"})
    produto: str = Field(description="Nome do produto", json_schema_extra={"sql_type": "VARCHAR(100)"})
    area_destinada_colheita: float = Field(description="Área destinada à colheita (ha)", json_schema_extra={"sql_type": "FLOAT"})
    area_colhida: float = Field(description="Área colhida (ha)", json_schema_extra={"sql_type": "FLOAT"})
    quantidade_produzida: float = Field(description="Quantidade produzida", json_schema_extra={"sql_type": "FLOAT"})
    rendimento_medio_producao: float = Field(description="Rendimento médio da produção", json_schema_extra={"sql_type": "FLOAT"})
    valor_producao: float = Field(description="Valor da produção", json_schema_extra={"sql_type": "FLOAT"})
    
    __metadata__: ClassVar[TableMetadata] = TableMetadata(
        schema="br_ibge_pam",
        table_name="lavoura_permanente",
        description="Produção Agrícola Municipal - Lavoura Permanente (Processada)"
    )