from __future__ import annotations

import tempfile
import unittest
import zipfile
from pathlib import Path
import sys
import types
from unittest.mock import patch

import pandas as pd

sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *a, **k: None))
sys.modules.setdefault(
    "loguru",
    types.SimpleNamespace(
        logger=types.SimpleNamespace(
            remove=lambda *a, **k: None,
            add=lambda *a, **k: None,
            bind=lambda **kwargs: types.SimpleNamespace(
                info=lambda *a, **k: None,
                error=lambda *a, **k: None,
                exception=lambda *a, **k: None,
            )
        )
    ),
)

from dados.export import dump_gold_l2  # noqa: E402


class GoldExportL2Tests(unittest.TestCase):
    def test_consumption_values_export_uses_long_format(self) -> None:
        source = pd.DataFrame(
            {
                "ano": [2018, 2018],
                "coeff_key": ["DemandaA", "DemandaB"],
                "valor": [0.1, 0.2],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with (
                patch.object(dump_gold_l2, "OUTPUT_DIR", output_dir),
                patch.object(dump_gold_l2, "_read", return_value=source),
            ):
                out = dump_gold_l2.export_consumption_values()

            exported = pd.read_csv(out)

        self.assertEqual(exported.columns.tolist(), ["ano", "coeff_key", "valor"])
        pd.testing.assert_frame_equal(exported, source)

    def test_export_coefficients_uses_fob_payload(self) -> None:
        source = pd.DataFrame(
            {
                "ano": [2020],
                "produto": ["A"],
                "valor_fob_dolar": [10.0],
                "valor_fob_real": [50.0],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with patch.object(dump_gold_l2, "_read", return_value=source):
                out = dump_gold_l2.export_export_coefficients(output_dir)

            payload = out.read_text(encoding="utf-8")

        self.assertIn("valor_fob_dolar", payload)
        self.assertIn("valor_fob_real", payload)
        self.assertNotIn('"coeff"', payload)

    def test_income_exports_read_default_tables(self) -> None:
        calls = []

        def fake_read(schema: str, query: str) -> pd.DataFrame:
            calls.append((schema, query))
            return pd.DataFrame(
                {"ano": [2020], "conta_alfa": ["ContaA"], "coeff": [1.0]}
            )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with patch.object(dump_gold_l2, "_read", side_effect=fake_read):
                dump_gold_l2.export_income_productivity(output_dir)
                dump_gold_l2.export_income_salary(output_dir)

        queries = [query for _, query in calls]
        self.assertTrue(any("renda_produtividade" in q for q in queries))
        self.assertTrue(any("renda_salario" in q for q in queries))

    def test_bundle_zip_scopes_files_to_package_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "layer2_new_values"
            output_dir.mkdir()
            zip_path = Path(tmp) / "layer2_new_values.zip"
            (output_dir / "cost_values.csv").write_text(
                "ano,tipo_coeff,valor\n",
                encoding="utf-8",
            )
            (output_dir / ".~lock.cost_values.csv#").write_text("", encoding="utf-8")

            out = dump_gold_l2.bundle_zip(output_dir, zip_path)

            with zipfile.ZipFile(out) as zf:
                names = zf.namelist()

        self.assertEqual(names, ["gold_export/layer2_new_values/cost_values.csv"])


if __name__ == "__main__":
    unittest.main()
