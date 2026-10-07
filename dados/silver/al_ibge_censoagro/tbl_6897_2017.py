"""Silver flow: Censo Agropecuário 2017 — table 6897 (production value by economic activity group)."""

from __future__ import annotations

from dotenv import load_dotenv
import pandas as pd

from dados.silver.al_ibge_censoagro._common import (
    DATASET_ID,
    ZONE,
    assert_pk_unique,
    coerce_decimals,
    download_raw,
    write_silver,
)
from dados.silver.al_ibge_censoagro.models import AlIbgeCensoagroTbl68972017
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_6897_2017"
PK_COLS = ["ano", "id_municipio", "grupo_atividade_economica"]
METRIC_COLS = ["valor_producao", "quantidade_estabelecimentos_produtivos"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    # tipo_agricultura is always 'Total' here (only classification 12517 is requested).
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            grupo_atividade_economica,
            MAX(CASE WHEN nome_variavel = 'Valor da produção dos estabelecimentos agropecuários'
                     THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários com produção'
                     THEN valor END) AS quantidade_estabelecimentos_produtivos
        FROM {DATASET_ID}.{TABLE}
        GROUP BY ano, id_municipio, grupo_atividade_economica
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = fix_ibge_digits(df, ["quantidade_estabelecimentos_produtivos"], PK_COLS)
    df = fix_ibge_digits(
        df,
        ["valor_producao"],
        PK_COLS,
        div_column="quantidade_estabelecimentos_produtivos",
    )
    return df[list(AlIbgeCensoagroTbl68972017.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    df = coerce_decimals(df, METRIC_COLS)
    [AlIbgeCensoagroTbl68972017(**r) for r in df.to_dict("records")]
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
        write_silver(TABLE, df, AlIbgeCensoagroTbl68972017)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
