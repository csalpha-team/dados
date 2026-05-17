"""Raw flow: BR diretorios — PRODLIST pesca/agro (CNAE/NCM mapping).

Source: local xlsx ``prodlist_pesca.xlsx`` staged under ``tmp_data/<dataset>/input/``.
Lands into ``$DB_RAW_ZONE.br_csalpha_diretorios_brasil.prodlist_pesca``.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from dados.raw.br_csalpha_diretorios_brasil.utils import process_ncm_codes
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.paths import tmp_dir

load_dotenv()

DATASET_ID = "br_csalpha_diretorios_brasil"
ZONE = "raw"
TABLE = "prodlist_pesca"

SOURCE_XLSX_NAME = "prodlist_pesca.xlsx"
_PKG_DIR = Path(__file__).resolve().parent

COLUMNS_DDL = {
    "id_prodlist": "VARCHAR(255)",
    "descricao_prodlist": "TEXT",
    "unidade_medida_prodlist": "VARCHAR(255)",
    "id_ncm": "VARCHAR(255)",
    "id_cnae_classe": "VARCHAR(255)",
    "descricao_cnae_classe": "TEXT",
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def _stage_source() -> Path:
    input_dir = tmp_dir(DATASET_ID, "input")
    staged = input_dir / SOURCE_XLSX_NAME
    src = _PKG_DIR / SOURCE_XLSX_NAME
    if not staged.exists():
        if not src.exists():
            log.error("extract.error", reason="missing_source_xlsx", path=str(src))
            raise FileNotFoundError(f"Source xlsx not found: {src}")
        shutil.copy2(src, staged)
        log.info("extract.stage.done", path=str(staged))
    return staged


def extract() -> pd.DataFrame:
    path = _stage_source()
    log.info("extract.read.start", path=str(path))
    df = pd.read_excel(path, header=None)
    df.dropna(how="all", inplace=True)

    pattern = r"CNAE\s*([\d\.\-]+):\s*(.+)"

    df["id_cnae"] = None
    df["descricao_cnae"] = None

    mask = df[1].str.contains(pattern, na=False, regex=True)
    extracted_cnae = df.loc[mask, 1].str.extract(pattern, expand=True)

    df.loc[mask, ["id_cnae", "descricao_cnae"]] = extracted_cnae.values
    df[["id_cnae", "descricao_cnae"]] = df[["id_cnae", "descricao_cnae"]].ffill()

    df = df[~df[1].astype(str).str.contains("CNAE", na=False)]
    df = df[~df[1].astype(str).str.contains("PRODLIST", na=False)]

    df.rename(
        columns={
            1: "id_prodlist",
            2: "descricao_prodlist",
            3: "unidade_medida_prodlist",
            4: "id_ncm",
            6: "atualizacao",
            "descricao_cnae": "descricao_cnae_classe",
            "id_cnae": "id_cnae_classe",
        },
        inplace=True,
    )

    df["id_ncm"] = df["id_ncm"].str.split("+")
    df["id_ncm"] = df["id_ncm"].apply(process_ncm_codes)
    df = df.explode("id_ncm")
    df["id_cnae_classe"] = (
        df["id_cnae_classe"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "", regex=False)
    )
    df["id_ncm"] = df["id_ncm"].str.replace(".", "", regex=False)
    df["id_prodlist"] = df["id_prodlist"].str.replace(".", "", regex=False)
    log.info("extract.read.done", rows=len(df))
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")
    missing = set(COLUMNS_DDL.keys()) - set(df.columns)
    if missing:
        log.error("validate.error", missing_columns=sorted(missing))
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df[list(COLUMNS_DDL.keys())]


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
        df = extract()
        log.info("extract.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
