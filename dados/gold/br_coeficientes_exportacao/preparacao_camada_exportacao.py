"""Gold flow: br_coeficientes_exportacao — coeficientes FOB de exportação.

NOTE: still reads NCM/Comex tables directly from the raw zone. A silver layer
for ``pa_me_comex_stat`` / ``br_csalpha_diretorios_brasil`` does not yet exist;
when it does, switch the extract over and drop the raw-zone fallback.
"""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel, Field

from dados.gold.br_coeficientes_exportacao.utils import (
    carregar_parametros_brutos,
    carregar_parametros_exportacao,
    construir_consulta_exportacao,
    gerar_grafico_series_coeficientes,
    preparar_dados_coeficientes_exportacao,
    salvar_resumo_coeficientes,
    salvar_json_coeficientes_exportacao,
    salvar_verificacao_matches,
)
from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger
from dados.utils.paths import tmp_dir
from dados.utils.pydantic_postgres import pydantic_to_postgres_columns

load_dotenv()

DATASET_ID = "br_coeficientes_exportacao"
ZONE = "gold"
TABLE = "preparacao_camada_exportacao"

DATABASE_ORIGEM = (
    os.getenv("DATABASE_ORIGEM_EXPORTACAO")
    or os.getenv("EXPORT_SOURCE_DATABASE")
    or os.getenv("DB_RAW_ZONE")
)
ESQUEMA_ORIGEM = os.getenv("ESQUEMA_ORIGEM_EXPORTACAO") or os.getenv(
    "EXPORT_SOURCE_SCHEMA", "pa_me_comex_stat"
)
TABELA_ORIGEM = os.getenv("TABELA_ORIGEM_EXPORTACAO") or os.getenv(
    "EXPORT_SOURCE_TABLE", "ncm_exportacao"
)
ESQUEMA_NCM = os.getenv("ESQUEMA_NCM_EXPORTACAO") or os.getenv(
    "EXPORT_NCM_SCHEMA", "br_csalpha_diretorios_brasil"
)
TABELA_NCM = os.getenv("TABELA_NCM_EXPORTACAO") or os.getenv(
    "EXPORT_NCM_TABLE", "nomenclatura_comum_mercosul"
)

CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_exportacao.json")
RESULTADOS_DIR = Path(__file__).with_name("resultados")

PK_COLS = ["ano", "produto"]
NUMERIC_COLS = ["valor_fob_dolar", "valor_fob_real", "coeff"]

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


class BrCoeficientesExportacaoPreparacaoCamadaExportacao(BaseModel):
    ano: int = Field(description="Reference year", json_schema_extra={"unit": "YYYY"})
    produto: str = Field(
        description="Aggregated product label (mapped from NCM)",
        json_schema_extra={"unit": "code"},
    )
    valor_fob_dolar: Decimal | None = Field(
        description="Total FOB export value in US dollars",
        json_schema_extra={"unit": "USD"},
    )
    valor_fob_real: Decimal | None = Field(
        description="Total FOB export value converted to Brazilian reais",
        json_schema_extra={"unit": "BRL"},
    )
    coeff: Decimal | None = Field(
        description="Export coefficient for the product/year cell",
        json_schema_extra={"unit": "ratio"},
    )


MODEL = BrCoeficientesExportacaoPreparacaoCamadaExportacao


def _database_gold() -> str:
    database = os.getenv("DB_GOLD_ZONE") or os.getenv("DB_AGREGATED_ZONE")
    if not database:
        raise ValueError(
            "Banco gold não configurado. Defina DB_GOLD_ZONE ou DB_AGREGATED_ZONE."
        )
    return database


def extract() -> tuple[pd.DataFrame, dict]:
    (
        preparacoes_produtos,
        participacoes_especificas,
        anos,
        taxa_cambio_brl_por_usd,
        uf_alvo,
    ) = carregar_parametros_exportacao(CONFIG_PATH)

    if not DATABASE_ORIGEM:
        raise ValueError(
            "Banco de origem não configurado. Defina DATABASE_ORIGEM_EXPORTACAO, "
            "EXPORT_SOURCE_DATABASE ou DB_RAW_ZONE."
        )

    with PostgresETL(
        host="localhost",
        database=DATABASE_ORIGEM,
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=ESQUEMA_ORIGEM,
    ) as db:
        consulta = construir_consulta_exportacao(
            db,
            esquema_origem=ESQUEMA_ORIGEM,
            tabela_origem=TABELA_ORIGEM,
            esquema_ncm=ESQUEMA_NCM,
            tabela_ncm=TABELA_NCM,
        )
        dados_exportacao = db.download_data(consulta)

    params = {
        "preparacoes_produtos": preparacoes_produtos,
        "participacoes_especificas": participacoes_especificas,
        "anos": anos,
        "taxa_cambio_brl_por_usd": taxa_cambio_brl_por_usd,
        "uf_alvo": uf_alvo,
    }
    return dados_exportacao, params


def transform(payload: tuple[pd.DataFrame, dict]) -> pd.DataFrame:
    dados_exportacao, params = payload
    df = preparar_dados_coeficientes_exportacao(dados_exportacao, **params)
    return df[list(MODEL.model_fields.keys())].copy()


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("transform produced empty dataframe")

    dupes = df.duplicated(subset=PK_COLS, keep=False)
    if dupes.any():
        log.error("validate.error", reason="duplicate_pk", count=int(dupes.sum()))
        raise ValueError(f"Found {int(dupes.sum())} rows duplicating PK {PK_COLS}")

    for col in NUMERIC_COLS:
        df[col] = df[col].apply(lambda v: None if pd.isna(v) else Decimal(str(v)))

    [MODEL(**r) for r in df.to_dict("records")]
    return df


def load(df: pd.DataFrame) -> None:
    columns = pydantic_to_postgres_columns(MODEL)
    with PostgresETL(
        host="localhost",
        database=_database_gold(),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        db.create_table(TABLE, columns, drop_if_exists=True)
        db.load_data(TABLE, df, if_exists="append")

    output_json_path = os.getenv(
        "CAMINHO_SAIDA_JSON_COEFICIENTES_EXPORTACAO"
    ) or os.getenv("EXPORT_COEFFICIENTS_OUTPUT_JSON_PATH")
    if output_json_path:
        salvar_json_coeficientes_exportacao(df, Path(output_json_path))


def carregar_coeficientes() -> pd.DataFrame:
    with PostgresETL(
        host="localhost",
        database=_database_gold(),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        return db.download_table(TABLE)


def gerar_auditoria_matches() -> Path:
    parametros = carregar_parametros_brutos(CONFIG_PATH)
    caminho = salvar_verificacao_matches(
        parametros,
        RESULTADOS_DIR / "auditoria_matches_ncm.xlsx",
    )
    log.info("auditoria_matches_ncm.done", xlsx=str(caminho))
    return caminho


def gerar_auditoria_series() -> tuple[Path, Path]:
    coeficientes = carregar_coeficientes()
    caminho_resumo = salvar_resumo_coeficientes(
        coeficientes,
        tmp_dir(DATASET_ID, "output") / "resumo_coeficientes.csv",
    )
    caminho_grafico = gerar_grafico_series_coeficientes(
        coeficientes,
        tmp_dir(DATASET_ID, "output") / "series_coeficientes_exportacao.png",
    )
    log.info(
        "auditoria_series.done",
        resumo=str(caminho_resumo),
        grafico=str(caminho_grafico),
    )
    return caminho_resumo, caminho_grafico


def flow() -> None:
    log.info("flow.start", table=TABLE)
    try:
        payload = extract()
        log.info("extract.done", rows=len(payload[0]))
        df = transform(payload)
        log.info("transform.done", rows=len(df))
        df = validate(df)
        log.info("validate.done", rows=len(df))
        load(df)
        log.info("load.done", rows=len(df))
    except Exception as exc:
        log.exception("flow.error", error=str(exc))
        raise
    log.info("flow.end", rows=len(df))


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Executa a camada gold de exportacao")
    parser.add_argument(
        "--auditoria-matches",
        action="store_true",
        help="gera somente o XLSX sintetico produto x NCM",
    )
    parser.add_argument(
        "--auditoria-series",
        action="store_true",
        help="gera resumo e grafico das series ja carregadas no banco gold",
    )
    args = parser.parse_args()

    if args.auditoria_matches:
        gerar_auditoria_matches()
        return
    if args.auditoria_series:
        gerar_auditoria_series()
        return
    flow()


if __name__ == "__main__":
    main()
