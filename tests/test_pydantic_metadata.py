"""Lint test: every silver/gold pydantic field declares description + unit.

REFACTORING.md §4 mandates that every column carries a Python type, a
``description`` and a ``unit`` (via ``json_schema_extra``). This test walks
``dados.silver.models`` and ``dados.gold.models``, imports each module, and
fails if any ``BaseModel`` subclass has a field missing either piece of
metadata.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest
from pydantic import BaseModel


def _iter_models(package_name: str):
    pkg = importlib.import_module(package_name)
    for mod_info in pkgutil.iter_modules(pkg.__path__):
        if mod_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{package_name}.{mod_info.name}")
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseModel)
                and attr is not BaseModel
                and attr.__module__ == module.__name__
            ):
                yield module.__name__, attr


def _collect_violations(package_name: str) -> list[str]:
    violations: list[str] = []
    for module_name, model in _iter_models(package_name):
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


@pytest.mark.parametrize("package", ["dados.silver.models", "dados.gold.models"])
def test_models_declare_description_and_unit(package: str) -> None:
    violations = _collect_violations(package)
    assert not violations, "Missing metadata:\n  " + "\n  ".join(violations)
