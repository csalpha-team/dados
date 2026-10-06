"""Raw flow: BCB SGS 433 — monthly IPCA percentage change.

The official BCData/SGS API is the primary source. A local JSON cache under
``tmp_data/`` makes reruns possible when the service is temporarily
unavailable; malformed or incomplete cached payloads are never accepted.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_bcb_sgs_ipca"
ZONE = "raw"
TABLE = "ipca_mensal"

SERIES_CODE = 433
SOURCE_NAME = "Banco Central do Brasil — SGS 433"
API_URL = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{SERIES_CODE}/dados"
START_DATE = date(1995, 1, 1)
DEFAULT_CACHE_PATH = (
    Path(__file__).resolve().parents[3]
    / "tmp_data"
    / DATASET_ID
    / f"sgs_{SERIES_CODE}.json"
)

COLUMNS_DDL = {
    "data_referencia": "DATE",
    "variacao_mensal_pct": "NUMERIC",
    "serie_sgs": "INTEGER",
    "fonte": "VARCHAR(255)",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _parse_payload(payload: Any) -> pd.DataFrame:
    if not isinstance(payload, list) or not payload:
        raise ValueError("A API SGS 433 retornou um payload vazio ou invalido")

    rows: list[dict[str, object]] = []
    for position, item in enumerate(payload):
        if not isinstance(item, dict) or "data" not in item or "valor" not in item:
            raise ValueError(
                f"Registro invalido na resposta da API SGS 433: posicao {position}"
            )
        rows.append(
            {
                "data_referencia": item["data"],
                "variacao_mensal_pct": item["valor"],
                "serie_sgs": SERIES_CODE,
                "fonte": SOURCE_NAME,
            }
        )

    frame = pd.DataFrame(rows, columns=COLUMNS_DDL.keys())
    frame["data_referencia"] = pd.to_datetime(
        frame["data_referencia"], format="%d/%m/%Y", errors="coerce"
    )
    frame["variacao_mensal_pct"] = pd.to_numeric(
        frame["variacao_mensal_pct"], errors="coerce"
    )
    return frame


def _validate_monthly_sequence(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("A serie SGS 433 nao contem observacoes")
    if df[["data_referencia", "variacao_mensal_pct"]].isna().any().any():
        raise ValueError("A serie SGS 433 contem data ou valor invalido")
    if df["data_referencia"].duplicated().any():
        raise ValueError("A serie SGS 433 contem meses duplicados")
    if (df["variacao_mensal_pct"] <= -100).any():
        raise ValueError("A serie SGS 433 contem variacao mensal menor ou igual a -100%")

    ordered = df.sort_values("data_referencia").reset_index(drop=True)
    observed_periods = pd.PeriodIndex(ordered["data_referencia"], freq="M")
    expected_periods = pd.period_range(
        observed_periods.min(), observed_periods.max(), freq="M"
    )
    missing = expected_periods.difference(observed_periods)
    if not missing.empty:
        missing_text = ", ".join(str(period) for period in missing[:6])
        raise ValueError(f"A serie SGS 433 possui meses ausentes: {missing_text}")
    return ordered


def _validate_requested_start(df: pd.DataFrame) -> pd.DataFrame:
    first_observation = df["data_referencia"].min().date()
    if first_observation != START_DATE:
        raise ValueError(
            "A serie SGS 433 nao cobre o inicio solicitado: "
            f"esperado {START_DATE}, recebido {first_observation}"
        )
    return df


def _read_cache(cache_path: Path) -> list[dict[str, str]]:
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cache IPCA invalido em {cache_path}: {exc}") from exc
    if not isinstance(payload, list):
        raise ValueError(f"Cache IPCA invalido em {cache_path}: raiz nao e uma lista")
    return payload


def _write_cache(cache_path: Path, payload: list[dict[str, str]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = cache_path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary_path.replace(cache_path)


def extract(
    *,
    cache_path: Path = DEFAULT_CACHE_PATH,
    end_date: date | None = None,
) -> pd.DataFrame:
    end_date = end_date or date.today()
    params = {
        "formato": "json",
        "dataInicial": START_DATE.strftime("%d/%m/%Y"),
        "dataFinal": end_date.strftime("%d/%m/%Y"),
    }

    try:
        log.info(
            "extract.api.start",
            serie_sgs=SERIES_CODE,
            start=params["dataInicial"],
            end=params["dataFinal"],
        )
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        frame = _validate_requested_start(
            _validate_monthly_sequence(_parse_payload(payload))
        )
        _write_cache(cache_path, payload)
        log.info("extract.api.done", rows=len(frame), cache_path=str(cache_path))
        return frame
    except (requests.RequestException, ValueError, json.JSONDecodeError) as exc:
        if not cache_path.exists():
            raise RuntimeError(
                "Nao foi possivel obter o IPCA na API SGS 433 e nao existe cache local"
            ) from exc
        log.warning(
            "extract.api.fallback_cache",
            error=str(exc),
            cache_path=str(cache_path),
        )
        cached = _validate_requested_start(
            _validate_monthly_sequence(_parse_payload(_read_cache(cache_path)))
        )
        if cached["data_referencia"].max().date() < end_date.replace(day=1):
            log.warning(
                "extract.cache.older_than_requested_end",
                cache_end=str(cached["data_referencia"].max().date()),
                requested_end=str(end_date),
            )
        return cached


def validate(df: pd.DataFrame) -> pd.DataFrame:
    missing_columns = set(COLUMNS_DDL).difference(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatorias ausentes: {sorted(missing_columns)}")
    return _validate_monthly_sequence(df[list(COLUMNS_DDL)].copy())


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(COLUMNS_DDL)].copy()


def load(df: pd.DataFrame) -> None:
    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_RAW_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, COLUMNS_DDL, if_not_exists=True)
        db.load_data(TABLE, df, if_exists="replace")


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        frame = extract()
        frame = validate(frame)
        log.info("validate.done", rows=len(frame))
        frame = transform(frame)
        log.info("transform.done", rows=len(frame))
        load(frame)
        log.info("load.done", rows=len(frame))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(frame))


if __name__ == "__main__":
    flow()
