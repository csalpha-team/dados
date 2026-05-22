"""Silver flow: Censo Agropecuário 2006 — table 2782 (family workforce)."""

from __future__ import annotations

from dotenv import load_dotenv
import pandas as pd

from dados.silver.al_ibge_censoagro._common import (
    DATASET_ID,
    TIPO_AGRICULTURA_2006,
    ZONE,
    assert_pk_unique,
    coerce_decimals,
    download_raw,
    write_silver,
)
from dados.silver.al_ibge_censoagro.models import AlIbgeCensoagroTbl27822006
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_2782_2006"
PK_COLS = ["ano", "id_municipio", "tipo_agricultura"]
METRIC_COLS = [
    "pessoal_ocupado_mais_14_anos_familia",
    "pessoal_total_ocupado_familia",
]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            tipo_agricultura,
            MAX(CASE WHEN nome_variavel = 'Pessoal ocupado em estabelecimentos agropecuários em 31/12 com laço de parentesco com o produtor'
                     THEN valor END) AS pessoal_total_ocupado_familia,
            MAX(CASE WHEN nome_variavel = 'Pessoal ocupado em estabelecimentos agropecuários em 31/12 com 14 anos e mais de idade e com laço de parentesco com o produtor'
                     THEN valor END) AS pessoal_ocupado_mais_14_anos_familia
        FROM {DATASET_ID}.{TABLE}
        WHERE aspecto_pessoal_ocupado = 'Total'
          AND tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar')
        GROUP BY ano, id_municipio, tipo_agricultura
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tipo_agricultura"] = df["tipo_agricultura"].map(TIPO_AGRICULTURA_2006)
    df = fix_ibge_digits(df, METRIC_COLS, ["id_municipio", "ano", "tipo_agricultura"])
    return df[list(AlIbgeCensoagroTbl27822006.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    df = coerce_decimals(df, METRIC_COLS)
    [AlIbgeCensoagroTbl27822006(**r) for r in df.to_dict("records")]
    return df


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract()
        log.info("extract.done", rows=len(df))
        df = transform(df)
        log.info("transform.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        write_silver(TABLE, df, AlIbgeCensoagroTbl27822006)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
