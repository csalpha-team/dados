"""Silver flow: Censo Agropecuário 2017 — table 6899 (farm expenditures)."""
from __future__ import annotations

from dotenv import load_dotenv
import pandas as pd

from dados.silver.al_ibge_censoagro._common import (
    DATASET_ID,
    TIPO_AGRICULTURA_2017,
    ZONE,
    assert_pk_unique,
    coerce_decimals,
    download_raw,
    write_silver,
)
from dados.silver.models.al_ibge_censoagro import AlIbgeCensoagroTbl68992017
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_6899_2017"
PK_COLS = ["ano", "id_municipio", "tipo_agricultura", "tipo_despesa"]
METRIC_COLS = ["quantidade_estabelecimentos_fizeram_despesa", "valor_despesa"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            tipo_agricultura,
            despesa AS tipo_despesa,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários que realizaram despesas'
                     THEN valor END) AS quantidade_estabelecimentos_fizeram_despesa,
            MAX(CASE WHEN nome_variavel = 'Valor das despesas realizadas pelos estabelecimentos agropecuários'
                     THEN valor END) AS valor_despesa
        FROM {DATASET_ID}.{TABLE}
        WHERE tipo_agricultura IN ('Agricultura familiar - sim', 'Agricultura familiar - não')
        GROUP BY ano, id_municipio, tipo_agricultura, despesa
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tipo_agricultura"] = df["tipo_agricultura"].map(TIPO_AGRICULTURA_2017)
    df = fix_ibge_digits(
        df,
        METRIC_COLS,
        ["id_municipio", "ano", "tipo_despesa", "tipo_agricultura"],
        div_column="quantidade_estabelecimentos_fizeram_despesa",
    )
    return df[list(AlIbgeCensoagroTbl68992017.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    df = coerce_decimals(df, METRIC_COLS)
    [AlIbgeCensoagroTbl68992017(**r) for r in df.to_dict("records")]
    return df


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        df = extract();    log.info("extract.done", rows=len(df))
        df = transform(df);log.info("transform.done", rows=len(df))
        df = validate(df); log.info("validate.done", rows=len(df))
        write_silver(TABLE, df, AlIbgeCensoagroTbl68992017); log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
