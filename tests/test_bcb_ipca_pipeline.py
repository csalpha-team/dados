from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

from dados.raw.br_bcb_sgs_ipca import ipca as raw_ipca
from dados.silver.br_bcb_sgs_ipca import ipca_anual


class BcbIpcaRawTests(unittest.TestCase):
    def test_extract_grava_cache_validado_e_o_reutiliza(self) -> None:
        payload = [
            {"data": "01/01/1995", "valor": "1.70"},
            {"data": "01/02/1995", "valor": "1.02"},
        ]
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = payload

        with tempfile.TemporaryDirectory() as directory:
            cache_path = Path(directory) / "ipca.json"
            with patch.object(raw_ipca.requests, "get", return_value=response):
                live = raw_ipca.extract(
                    cache_path=cache_path, end_date=date(1995, 2, 28)
                )

            self.assertTrue(cache_path.exists())
            self.assertEqual(live["serie_sgs"].unique().tolist(), [433])
            self.assertEqual(live["variacao_mensal_pct"].tolist(), [1.70, 1.02])

            with patch.object(
                raw_ipca.requests,
                "get",
                side_effect=requests.ConnectionError("offline"),
            ):
                cached = raw_ipca.extract(
                    cache_path=cache_path, end_date=date(1995, 2, 28)
                )

            pd.testing.assert_frame_equal(live, cached)

    def test_validate_rejeita_lacuna_mensal(self) -> None:
        frame = pd.DataFrame(
            {
                "data_referencia": pd.to_datetime(["1995-01-01", "1995-03-01"]),
                "variacao_mensal_pct": [1.0, 1.0],
                "serie_sgs": [433, 433],
                "fonte": [raw_ipca.SOURCE_NAME, raw_ipca.SOURCE_NAME],
            }
        )

        with self.assertRaisesRegex(ValueError, "meses ausentes"):
            raw_ipca.validate(frame)

    def test_extract_rejeita_resposta_que_nao_cobre_inicio_solicitado(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [{"data": "01/02/1995", "valor": "1.02"}]

        with tempfile.TemporaryDirectory() as directory:
            with patch.object(raw_ipca.requests, "get", return_value=response):
                with self.assertRaisesRegex(RuntimeError, "nao existe cache"):
                    raw_ipca.extract(
                        cache_path=Path(directory) / "missing.json",
                        end_date=date(1995, 2, 28),
                    )


class BcbIpcaSilverTests(unittest.TestCase):
    @staticmethod
    def _monthly(months: int = 12) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "data_referencia": pd.date_range(
                    "2020-01-01", periods=months, freq="MS"
                ),
                "variacao_mensal_pct": [1.0] * months,
                "serie_sgs": [433] * months,
                "fonte": [raw_ipca.SOURCE_NAME] * months,
            }
        )

    def test_transform_encadeia_variacoes_e_marca_ano_completo(self) -> None:
        annual = ipca_anual.transform(self._monthly())

        expected_levels = pd.Series([100.0 * (1.01**month) for month in range(1, 13)])
        self.assertAlmostEqual(annual.loc[0, "indice_ipca"], expected_levels.mean())
        self.assertAlmostEqual(
            annual.loc[0, "variacao_acumulada_ano_pct"],
            ((1.01**12) - 1.0) * 100.0,
        )
        self.assertEqual(annual.loc[0, "meses_observados"], 12)
        self.assertTrue(annual.loc[0, "ano_completo"])

    def test_transform_identifica_ano_incompleto_sem_inventar_mes(self) -> None:
        annual = ipca_anual.transform(self._monthly(months=11))

        self.assertEqual(annual.loc[0, "meses_observados"], 11)
        self.assertFalse(annual.loc[0, "ano_completo"])


if __name__ == "__main__":
    unittest.main()
