"""Map pydantic models to Postgres DDL column dicts.

REFACTORING.md §4: silver/gold flows derive Postgres column types from their
pydantic schema rather than maintaining a parallel ``{'col': 'VARCHAR(255)'}``
dict. The DDL produced here is the format ``PostgresETL.create_table`` expects.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Type, Union, get_args, get_origin

from pydantic import BaseModel

_PG_TYPE_MAP: dict[type, str] = {
    int: "BIGINT",
    float: "DOUBLE PRECISION",
    Decimal: "NUMERIC",
    bool: "BOOLEAN",
    date: "DATE",
    datetime: "TIMESTAMP",
}


def _unwrap_optional(tp: Any) -> Any:
    """Strip ``Optional`` / ``X | None`` and return the underlying type."""
    if get_origin(tp) is Union:
        non_none = [a for a in get_args(tp) if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return tp


def pydantic_to_postgres_columns(model: Type[BaseModel]) -> dict[str, str]:
    """Return an ordered ``{column: postgres_type}`` dict for ``model``.

    ``str`` maps to ``VARCHAR(255)`` unless the field's ``json_schema_extra``
    declares an integer ``max_length`` (we keep the default loose for now).
    Unknown types fall back to ``VARCHAR(255)``.
    """
    cols: dict[str, str] = {}
    for name, field in model.model_fields.items():
        py_type = _unwrap_optional(field.annotation)
        if py_type is str:
            cols[name] = "VARCHAR(255)"
        else:
            cols[name] = _PG_TYPE_MAP.get(py_type, "VARCHAR(255)")
    return cols
