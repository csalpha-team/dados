"""Silver flow: Censo Agropecuário 2006 — table 2518 (lavoura permanente, autoconsumo)."""

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
from dados.silver.models.al_ibge_censoagro import AlIbgeCensoagroTbl25182006
from dados.silver.constants.produtos import dicionario_produtos_censo_6955_2518
from dados.silver.utils import calcula_autoconsumo_comercio, fix_ibge_digits
from dados.utils.logging import get_logger

load_dotenv()

TABLE = "tbl_2518_2006"
PK_COLS = ["ano", "id_municipio", "produto", "tipo_agricultura"]
METRIC_COLS = [
    "quantidade_estabelecimentos",
    "quantidade_produzida",
    "quantidade_vendida",
    "valor_producao",
    "valor_venda",
]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    # The "Total"/"Consumo no estabelecimento" axis is kept long-format here so
    # calcula_autoconsumo_comercio can derive the comércio columns downstream.
    query = f"""
        SELECT
            CAST(ano AS INTEGER) AS ano,
            id_municipio,
            produto,
            tipo_agricultura,
            tipo_consumo_estocagem,
            tipo_venda_entrega,
            MAX(CASE WHEN nome_variavel = 'Número de estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS quantidade_estabelecimentos,
            MAX(CASE WHEN nome_variavel = 'Quantidade produzida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS quantidade_produzida,
            MAX(CASE WHEN nome_variavel = 'Quantidade vendida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS quantidade_vendida,
            MAX(CASE WHEN nome_variavel = 'Valor da produção dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS valor_producao,
            MAX(CASE WHEN nome_variavel = 'Valor das vendas dos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS valor_venda,
            MAX(CASE WHEN nome_variavel = 'Área colhida nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS area_colhida,
            MAX(CASE WHEN nome_variavel = 'Área plantada nos estabelecimentos agropecuários com mais de 50 pés existentes em 31/12' THEN valor END) AS area_plantada
        FROM {DATASET_ID}.{TABLE}
        WHERE tipo_agricultura IN ('Agricultura familiar - Lei 11.326', 'Agricultura não familiar')
        GROUP BY ano, id_municipio, produto, tipo_agricultura, tipo_consumo_estocagem, tipo_venda_entrega
    """
    return download_raw(query)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["produto"] = df["produto"].map(dicionario_produtos_censo_6955_2518)
    df["tipo_agricultura"] = df["tipo_agricultura"].map(TIPO_AGRICULTURA_2006)

    df = fix_ibge_digits(
        df,
        METRIC_COLS + ["area_colhida", "area_plantada"],
        [
            "id_municipio",
            "ano",
            "produto",
            "tipo_agricultura",
            "tipo_consumo_estocagem",
            "tipo_venda_entrega",
        ],
        div_column="quantidade_estabelecimentos",
    )

    df = calcula_autoconsumo_comercio(
        df=df,
        id_cols=PK_COLS,
        metric_cols=METRIC_COLS,
        category_col="tipo_consumo_estocagem",
        total_label="Total",
        consumo_label="Consumo no estabelecimento",
    )
    return df[list(AlIbgeCensoagroTbl25182006.model_fields.keys())]


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("transform produced an empty dataframe")
    assert_pk_unique(df, PK_COLS)
    decimal_cols = [c for c in df.columns if c not in PK_COLS]
    df = coerce_decimals(df, decimal_cols)
    [AlIbgeCensoagroTbl25182006(**r) for r in df.to_dict("records")]
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
        write_silver(TABLE, df, AlIbgeCensoagroTbl25182006)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


if __name__ == "__main__":
    flow()
