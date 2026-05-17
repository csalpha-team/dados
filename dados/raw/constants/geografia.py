# -*- coding: utf-8 -*-
"""Constantes geográficas — códigos IBGE de UF e siglas."""

from enum import Enum


class UF(Enum):
    """Unidades da Federação com código IBGE, sigla e nome."""
    #NOTE: são utilizadas para gerar os headers das requisições de ingestão da API de Geoserviços do IBGE;

    RO = (11, "RO", "Rondônia")
    AC = (12, "AC", "Acre")
    AM = (13, "AM", "Amazonas")
    RR = (14, "RR", "Roraima")
    PA = (15, "PA", "Pará")
    AP = (16, "AP", "Amapá")
    TO = (17, "TO", "Tocantins")
    MA = (21, "MA", "Maranhão")
    PI = (22, "PI", "Piauí")
    CE = (23, "CE", "Ceará")
    RN = (24, "RN", "Rio Grande do Norte")
    PB = (25, "PB", "Paraíba")
    PE = (26, "PE", "Pernambuco")
    AL = (27, "AL", "Alagoas")
    SE = (28, "SE", "Sergipe")
    BA = (29, "BA", "Bahia")
    MG = (31, "MG", "Minas Gerais")
    ES = (32, "ES", "Espírito Santo")
    RJ = (33, "RJ", "Rio de Janeiro")
    SP = (35, "SP", "São Paulo")
    PR = (41, "PR", "Paraná")
    SC = (42, "SC", "Santa Catarina")
    RS = (43, "RS", "Rio Grande do Sul")
    MS = (50, "MS", "Mato Grosso do Sul")
    MT = (51, "MT", "Mato Grosso")
    GO = (52, "GO", "Goiás")
    DF = (53, "DF", "Distrito Federal")

    def __init__(self, codigo_ibge: int, sigla: str, nome: str):
        self.codigo_ibge = codigo_ibge
        self.sigla = sigla
        self.nome = nome


UF_ID_SIGLA: dict[int, str] = {uf.codigo_ibge: uf.sigla for uf in UF}
