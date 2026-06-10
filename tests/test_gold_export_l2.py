from __future__ import annotations

import tempfile
import unittest
import zipfile
import json
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
    def test_consumption_coefficients_export_uses_legacy_wide_format(self) -> None:
        source = pd.DataFrame(
            {
                "ano": [2018, 2018],
                "coeff_key": ["DemandaA", "DemandaB"],
                "coeff": [0.1, 0.2],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with patch.object(dump_gold_l2, "_read", return_value=source):
                out = dump_gold_l2.export_consumption_coefficients(output_dir)

            exported = pd.read_csv(out)

        self.assertEqual(exported.columns.tolist(), ["DemandaA", "DemandaB"])
        self.assertEqual(
            exported.to_dict("records"),
            [{"DemandaA": 0.1, "DemandaB": 0.2}],
        )

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

    def test_export_coefficients_uses_legacy_coeff_payload(self) -> None:
        source = pd.DataFrame(
            {
                "ano": [2020, 2020],
                "produto": ["A", "B"],
                "coeff": [0.25, 0.75],
            }
        )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with patch.object(dump_gold_l2, "_read", return_value=source):
                out = dump_gold_l2.export_export_coefficients(output_dir)

            payload = out.read_text(encoding="utf-8")

        self.assertIn('"coeff": 0.25', payload)
        self.assertNotIn("valor_fob_dolar", payload)

    def test_export_values_uses_fob_payload(self) -> None:
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
                out = dump_gold_l2.export_export_values(output_dir)

            payload = out.read_text(encoding="utf-8")

        self.assertIn("valor_fob_dolar", payload)
        self.assertIn("valor_fob_real", payload)
        self.assertNotIn('"coeff"', payload)

    def test_income_old_and_new_exports_read_distinct_tables(self) -> None:
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
                dump_gold_l2.export_income_productivity_values(output_dir)
                dump_gold_l2.export_income_salary(output_dir)
                dump_gold_l2.export_income_salary_values(output_dir)

        queries = [query for _, query in calls]
        self.assertTrue(any("renda_produtividade_old" in q for q in queries))
        self.assertTrue(any("renda_produtividade" in q for q in queries))
        self.assertTrue(any("renda_salario_old" in q for q in queries))
        self.assertTrue(any("renda_salario" in q for q in queries))

    def test_generated_legacy_income_matches_local_benchmarks(self) -> None:
        benchmark_dir = Path("/home/cleyton/Downloads")
        generated_dir = Path("gold_export/gold_old")
        for filename in ["income_productivity.json", "income_salary.json"]:
            benchmark = benchmark_dir / filename
            generated = generated_dir / filename
            if not benchmark.exists() or not generated.exists():
                self.skipTest(f"Benchmark or generated {filename} is unavailable.")

            generated_payload = json.loads(generated.read_text(encoding="utf-8"))
            benchmark_payload = json.loads(benchmark.read_text(encoding="utf-8"))
            self.assertEqual(generated_payload.keys(), benchmark_payload.keys())
            for year, expected_accounts in benchmark_payload.items():
                self.assertEqual(
                    generated_payload[year].keys(),
                    expected_accounts.keys(),
                )
                for account, expected in expected_accounts.items():
                    self.assertAlmostEqual(
                        float(generated_payload[year][account]),
                        float(expected),
                        places=9,
                    )

    def test_bundle_zip_scopes_files_to_package_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "gold_new"
            output_dir.mkdir()
            zip_path = Path(tmp) / "gold_new.zip"
            (output_dir / "cost_values.csv").write_text(
                "ano,tipo_coeff,valor\n",
                encoding="utf-8",
            )
            (output_dir / ".~lock.cost_values.csv#").write_text("", encoding="utf-8")

            out = dump_gold_l2.bundle_zip(output_dir, zip_path)

            with zipfile.ZipFile(out) as zf:
                names = zf.namelist()

        self.assertEqual(names, ["gold_export/gold_new/cost_values.csv"])


if __name__ == "__main__":
    unittest.main()
