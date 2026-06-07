from __future__ import annotations

import tempfile
import unittest
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

from dados.export import dump_gold_l2


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


if __name__ == "__main__":
    unittest.main()
