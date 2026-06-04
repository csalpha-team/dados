"""Silver schemas for br_csalpha_diretorios_brasil."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RegioesIntegracao(BaseModel):
    """Pará municipality → integration region (RI) directory.

    Built by joining the raw FAPESPA/SEPLAD transcription (by municipality name)
    against the IBGE municipality directory to attach the 7-digit code.
    """

    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    nome: str = Field(
        description="Municipality name from the IBGE directory",
        json_schema_extra={"unit": "code"},
    )
    sigla_uf: str = Field(
        description="State (UF) two-letter code",
        json_schema_extra={"unit": "code"},
    )
    nome_regiao_integracao: str = Field(
        description="Pará integration region (RI) the municipality belongs to",
        json_schema_extra={"unit": "code"},
    )
