from dotenv import load_dotenv
import os
from pathlib import Path

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.gold.br_coeficientes_exportacao.utils import (
    carregar_parametros_exportacao,
    construir_consulta_exportacao,
    preparar_dados_coeficientes_exportacao,
    salvar_json_coeficientes_exportacao,
)


load_dotenv()

DATASET_ID = "br_coeficientes_exportacao"
TABLE_ID = "preparacao_camada_exportacao"

DATABASE_ORIGEM = (
    os.getenv("DATABASE_ORIGEM_EXPORTACAO")
    or os.getenv("EXPORT_SOURCE_DATABASE")
    or os.getenv("DB_RAW_ZONE")
)
ESQUEMA_ORIGEM = os.getenv("ESQUEMA_ORIGEM_EXPORTACAO") or os.getenv(
    "EXPORT_SOURCE_SCHEMA", "al_me_comex_stat"
)
TABELA_ORIGEM = os.getenv("TABELA_ORIGEM_EXPORTACAO") or os.getenv(
    "EXPORT_SOURCE_TABLE", "comex_stat"
)
ESQUEMA_NCM = os.getenv("ESQUEMA_NCM_EXPORTACAO") or os.getenv(
    "EXPORT_NCM_SCHEMA", "br_csalpha_diretorios_brasil"
)
TABELA_NCM = os.getenv("TABELA_NCM_EXPORTACAO") or os.getenv(
    "EXPORT_NCM_TABLE", "nomenclatura_comum_mercosul"
)

CONFIG_PATH = Path(__file__).with_name("parametros_coeficientes_exportacao.json")


def main() -> None:
    (
        preparacoes_produtos,
        participacoes_especificas,
        anos,
        taxa_cambio_brl_por_usd,
        uf_alvo,
    ) = carregar_parametros_exportacao(CONFIG_PATH)

    if not DATABASE_ORIGEM:
        raise ValueError(
            "Banco de origem nao configurado. Defina DATABASE_ORIGEM_EXPORTACAO, "
            "EXPORT_SOURCE_DATABASE ou DB_RAW_ZONE."
        )

    with PostgresETL(
        host="localhost",
        database=DATABASE_ORIGEM,
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=ESQUEMA_ORIGEM,
    ) as db:
        consulta_origem = construir_consulta_exportacao(
            db,
            esquema_origem=ESQUEMA_ORIGEM,
            tabela_origem=TABELA_ORIGEM,
            esquema_ncm=ESQUEMA_NCM,
            tabela_ncm=TABELA_NCM,
        )
        dados_exportacao = db.download_data(consulta_origem)

    dados_coeficientes = preparar_dados_coeficientes_exportacao(
        dados_exportacao,
        preparacoes_produtos=preparacoes_produtos,
        participacoes_especificas=participacoes_especificas,
        anos=anos,
        taxa_cambio_brl_por_usd=taxa_cambio_brl_por_usd,
        uf_alvo=uf_alvo,
    )

    with PostgresETL(
        host="localhost",
        database=os.getenv("DB_GOLD_ZONE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        schema=DATASET_ID,
    ) as db:
        columns = {
            "ano": "integer",
            "produto": "VARCHAR(255)",
            "valor_fob_dolar": "numeric",
            "valor_fob_real": "numeric",
            "coeff": "numeric",
        }

        db.create_table(TABLE_ID, columns, drop_if_exists=True)
        db.load_data(TABLE_ID, dados_coeficientes, if_exists="replace")

    output_json_path = os.getenv("CAMINHO_SAIDA_JSON_COEFICIENTES_EXPORTACAO") or os.getenv(
        "EXPORT_COEFFICIENTS_OUTPUT_JSON_PATH"
    )
    if output_json_path:
        salvar_json_coeficientes_exportacao(dados_coeficientes, Path(output_json_path))


if __name__ == "__main__":
    main()
