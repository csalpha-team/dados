"""Gold schema for rural employment technical coefficients."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field


class PaCoeficientesEmpregoRuralCenso2017(BaseModel):
    """Technical employment coefficient from Censo Agropecuário 2017."""

    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    id_municipio: str = Field(
        description="IBGE 7-digit municipality code",
        json_schema_extra={"unit": "code"},
    )
    nome: str | None = Field(
        description="Municipality name", json_schema_extra={"unit": "name"}
    )
    nome_regiao_integracao: str | None = Field(
        description="Pará integration region",
        json_schema_extra={"unit": "name"},
    )
    sigla_uf: str | None = Field(
        description="State abbreviation", json_schema_extra={"unit": "code"}
    )
    grupo_atividade_economica: str = Field(
        description="IBGE economic activity group; temporary and permanent crops are distinct values",
        json_schema_extra={"unit": "code"},
    )
    pessoal_ocupado: Decimal | None = Field(
        description="Persons employed in rural establishments",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabelecimentos: Decimal | None = Field(
        description="Number of agricultural establishments",
        json_schema_extra={"unit": "head_count"},
    )
    quantidade_estabelecimentos_produtivos: Decimal | None = Field(
        description="Number of agricultural establishments with production",
        json_schema_extra={"unit": "head_count"},
    )
    valor_producao: Decimal | None = Field(
        description="Production value in thousands of BRL (IBGE unit: Mil Reais)",
        json_schema_extra={"unit": "1000xBRL"},
    )
    coeficiente_tecnico: Decimal | None = Field(
        description="Production value per employed person; null when VBP or employment is not positive",
        json_schema_extra={"unit": "1000xBRL/head_count"},
    )
