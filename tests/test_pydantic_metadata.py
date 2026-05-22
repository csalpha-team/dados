"""Lint test: every silver/gold pydantic field declares description + unit.

REFACTORING.md §4 mandates that every column carries a Python type, a
``description`` and a ``unit`` (via ``json_schema_extra``). This test walks
the ``models`` submodule inside each dataset package under ``dados.silver``
and ``dados.gold``, and fails if any ``BaseModel`` subclass has a field
missing either piece of metadata.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest
from pydantic import BaseModel


def _iter_models(zone_package: str):
    pkg = importlib.import_module(zone_package)
    for mod_info in pkgutil.iter_modules(pkg.__path__):
        if not mod_info.ispkg or mod_info.name.startswith("_"):
            continue
        models_mod_name = f"{zone_package}.{mod_info.name}.models"
        try:
            module = importlib.import_module(models_mod_name)
        except ModuleNotFoundError:
            continue
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseModel)
                and attr is not BaseModel
                and attr.__module__ == module.__name__
            ):
                yield module.__name__, attr


def _collect_violations(zone_package: str) -> list[str]:
    violations: list[str] = []
    for module_name, model in _iter_models(zone_package):
        for field_name, field in model.model_fields.items():
            extra = field.json_schema_extra or {}
            if not field.description:
                violations.append(
                    f"{module_name}.{model.__name__}.{field_name} missing description"
                )
            if not (isinstance(extra, dict) and extra.get("unit")):
                violations.append(
                    f"{module_name}.{model.__name__}.{field_name} missing unit"
                )
    return violations


@pytest.mark.parametrize("zone_package", ["dados.silver", "dados.gold"])
def test_models_declare_description_and_unit(zone_package: str) -> None:
    violations = _collect_violations(zone_package)
    assert not violations, "Missing metadata:\n  " + "\n  ".join(violations)
