"""Silver flow: Censo Agropecuário 2006 — table 2337 (lavoura temporária)."""

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
from dados.silver.al_ibge_censoagro.models import AlIbgeCensoagroTbl23372006
from dados.silver.constants.produtos import dicionario_produtos_censo_6957_2337
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_2337_2006"
PK_COLS = ["ano", "id_municipio", "produto", "tipo_agricultura"]
METRIC_COLS = [
    "quantidade_estabelecimentos",
    "quantidade_produzida",
    "quantidade_vendida",
    "valor_producao",
    "area_colhida",
]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            tipo_agricultura,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários' THEN valor END) AS quantidade_estabelecimentos,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida' THEN valor END) AS quantidade_produzida,
            MAX(CASE WHEN nome_variavel = 'Quantidade vendida' THEN valor END) AS quantidade_vendida,
            MAX(CASE WHEN nome_variavel = 'Valor da produção' THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Área colhida' THEN valor END) AS area_colhida
        FROM {DATASET_ID}.{TABLE}
        WHERE tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar')
        GROUP BY ano, id_municipio, produto, tipo_agricultura
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["produto"] = df["produto"].map(dicionario_produtos_censo_6957_2337)
    df["tipo_agricultura"] = df["tipo_agricultura"].map(TIPO_AGRICULTURA_2006)
    df = fix_ibge_digits(
        df,
        METRIC_COLS,
        ["id_municipio", "ano", "produto", "tipo_agricultura"],
        div_column="quantidade_estabelecimentos",
    )
    return df[list(AlIbgeCensoagroTbl23372006.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    df = coerce_decimals(df, METRIC_COLS)
    [AlIbgeCensoagroTbl23372006(**r) for r in df.to_dict("records")]
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
        write_silver(TABLE, df, AlIbgeCensoagroTbl23372006)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
