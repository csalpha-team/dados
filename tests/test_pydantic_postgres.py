from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field

from dados.utils.pydantic_postgres import pydantic_to_postgres_columns


class ExampleModel(BaseModel):
    valor: Decimal | None = Field(
        description="Optional decimal value",
        json_schema_extra={"unit": "BRL"},
    )


def test_pipe_optional_decimal_maps_to_numeric() -> None:
    assert pydantic_to_postgres_columns(ExampleModel)["valor"] == "NUMERIC"
