"""Silver flow: Censo Agropecuário 2017 — table 6885 (employed persons by tie)."""

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
from dados.silver.models.al_ibge_censoagro import AlIbgeCensoagroTbl68852017
from dados.silver.utils import fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_6885_2017"
PK_COLS = ["ano", "id_municipio", "tipo_agricultura"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            tipo_agricultura,
            MAX(CASE WHEN nome_variavel = 'Pessoal ocupado em estabelecimentos agropecuários' THEN valor END) AS pessoal_total_ocupado,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários com pessoal ocupado' THEN valor END) AS quantidade_total_estabecimentos,
            MAX(CASE WHEN nome_variavel = 'Pessoal ocupado em estabelecimentos agropecuários com laço de parentesco com o produtor' THEN valor END) AS pessoal_ocupado_familia,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários com pessoal ocupado com laço de parentesco com o produtor' THEN valor END) AS quantidade_estabecimentos_pessoal_ocupado_familia,
            MAX(CASE WHEN nome_variavel = 'Pessoal ocupado em estabelecimentos agropecuários sem laço de parentesco com o produtor' THEN valor END) AS pessoal_ocupado_fora_familia,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários com pessoal ocupado sem laço de parentesco com o produtor' THEN valor END) AS quantidade_estabecimentos_pessoal_ocupado_fora_familia
        FROM {DATASET_ID}.{TABLE}
        WHERE tipo_agricultura IN ('Agricultura familiar - sim', 'Agricultura familiar - não')
          AND faixa_idade = 'Total'
        GROUP BY ano, id_municipio, tipo_agricultura
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["tipo_agricultura"] = df["tipo_agricultura"].map(TIPO_AGRICULTURA_2017)

    df = fix_ibge_digits(
        df,
        ["pessoal_total_ocupado"],
        PK_COLS,
        div_column="quantidade_total_estabecimentos",
    )
    df = fix_ibge_digits(
        df,
        ["pessoal_ocupado_familia"],
        PK_COLS,
        div_column="quantidade_estabecimentos_pessoal_ocupado_familia",
    )
    df = fix_ibge_digits(
        df,
        ["pessoal_ocupado_fora_familia"],
        PK_COLS,
        div_column="quantidade_total_estabecimentos",
    )

    return df[list(AlIbgeCensoagroTbl68852017.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    decimal_cols = [c for c in df.columns if c not in PK_COLS]
    df = coerce_decimals(df, decimal_cols)
    [AlIbgeCensoagroTbl68852017(**r) for r in df.to_dict("records")]
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
        write_silver(TABLE, df, AlIbgeCensoagroTbl68852017)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
