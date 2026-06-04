"""Raw flow: BR diretorios — Regiões de Integração do Pará.

Source: FAPESPA/SEPLAD "Anexo VI - Regiões de Integração e Municípios do Estado
do Pará" (https://seplad.pa.gov.br/wp-content/uploads/2015/07/anexo_vi.pdf),
transcribed below as a ``{regiao: [municipio, ...]}`` constant. Municipality
names and region placements follow the project's canonical assignment (matching
``dados/gold/pa_indexadores_producao_rural/utils.py``): "Guajará" instead of
"Região Metropolitana", "Lago de Tucuruí", Santa Isabel do Pará under Guamá and
Oeiras do Pará under Marajó.

The ``id_municipio`` is intentionally NOT resolved here — the raw zone keeps the
source mapping by municipality name. The silver flow joins the municipality
directory to attach the IBGE code.

Lands into ``$DB_RAW_ZONE.br_csalpha_diretorios_brasil.regioes_integracao``.
"""

from __future__ import annotations

import os

import pandas as pd
from dotenv import load_dotenv

from dados.raw.utils.postgres_interactions import PostgresETL
from dados.utils.logging import get_logger

load_dotenv()

DATASET_ID = "br_csalpha_diretorios_brasil"
ZONE = "raw"
TABLE = "regioes_integracao"

# Total de municípios do Pará — usado como guarda de integridade da transcrição.
TOTAL_MUNICIPIOS_PA = 144

COLUMNS_DDL = {
    "nome_municipio": "VARCHAR(255)",
    "nome_regiao_integracao": "VARCHAR(255)",
}

# Transcrição do Anexo VI (FAPESPA/SEPLAD). Nomes grafados conforme o diretório
# de municípios do IBGE para que o join na silver seja direto.
REGIOES_INTEGRACAO: dict[str, list[str]] = {
    "Araguaia": [
        "Água Azul do Norte",
        "Bannach",
        "Conceição do Araguaia",
        "Cumaru do Norte",
        "Floresta do Araguaia",
        "Ourilândia do Norte",
        "Pau d'Arco",
        "Redenção",
        "Rio Maria",
        "Santa Maria das Barreiras",
        "Santana do Araguaia",
        "São Félix do Xingu",
        "Sapucaia",
        "Tucumã",
        "Xinguara",
    ],
    "Baixo Amazonas": [
        "Alenquer",
        "Almeirim",
        "Belterra",
        "Curuá",
        "Faro",
        "Juruti",
        "Mojuí dos Campos",
        "Monte Alegre",
        "Óbidos",
        "Oriximiná",
        "Prainha",
        "Santarém",
        "Terra Santa",
    ],
    "Carajás": [
        "Bom Jesus do Tocantins",
        "Brejo Grande do Araguaia",
        "Canaã dos Carajás",
        "Curionópolis",
        "Eldorado do Carajás",
        "Marabá",
        "Palestina do Pará",
        "Parauapebas",
        "Piçarra",
        "São Domingos do Araguaia",
        "São Geraldo do Araguaia",
        "São João do Araguaia",
    ],
    "Guajará": [
        "Belém",
        "Ananindeua",
        "Benevides",
        "Marituba",
        "Santa Bárbara do Pará",
    ],
    "Guamá": [
        "Castanhal",
        "Colares",
        "Curuçá",
        "Igarapé-Açu",
        "Inhangapi",
        "Magalhães Barata",
        "Maracanã",
        "Marapanim",
        "Santa Isabel do Pará",
        "Santa Maria do Pará",
        "Santo Antônio do Tauá",
        "São Caetano de Odivelas",
        "São Domingos do Capim",
        "São Francisco do Pará",
        "São João da Ponta",
        "São Miguel do Guamá",
        "Terra Alta",
        "Vigia",
    ],
    "Lago de Tucuruí": [
        "Breu Branco",
        "Goianésia do Pará",
        "Itupiranga",
        "Jacundá",
        "Nova Ipixuna",
        "Novo Repartimento",
        "Tucuruí",
    ],
    "Marajó": [
        "Afuá",
        "Anajás",
        "Bagre",
        "Breves",
        "Cachoeira do Arari",
        "Chaves",
        "Curralinho",
        "Gurupá",
        "Melgaço",
        "Muaná",
        "Oeiras do Pará",
        "Ponta de Pedras",
        "Portel",
        "Salvaterra",
        "Santa Cruz do Arari",
        "São Sebastião da Boa Vista",
        "Soure",
    ],
    "Rio Caeté": [
        "Augusto Corrêa",
        "Bonito",
        "Bragança",
        "Cachoeira do Piriá",
        "Capanema",
        "Nova Timboteua",
        "Peixe-Boi",
        "Primavera",
        "Quatipuru",
        "Salinópolis",
        "Santa Luzia do Pará",
        "Santarém Novo",
        "São João de Pirabas",
        "Tracuateua",
        "Viseu",
    ],
    "Rio Capim": [
        "Abel Figueiredo",
        "Aurora do Pará",
        "Bujaru",
        "Capitão Poço",
        "Concórdia do Pará",
        "Dom Eliseu",
        "Garrafão do Norte",
        "Ipixuna do Pará",
        "Irituia",
        "Mãe do Rio",
        "Nova Esperança do Piriá",
        "Ourém",
        "Paragominas",
        "Rondon do Pará",
        "Tomé-Açu",
        "Ulianópolis",
    ],
    "Tapajós": [
        "Aveiro",
        "Itaituba",
        "Jacareacanga",
        "Novo Progresso",
        "Rurópolis",
        "Trairão",
    ],
    "Tocantins": [
        "Abaetetuba",
        "Acará",
        "Baião",
        "Barcarena",
        "Cametá",
        "Igarapé-Miri",
        "Limoeiro do Ajuru",
        "Mocajuba",
        "Moju",
        "Tailândia",
    ],
    "Xingu": [
        "Altamira",
        "Anapu",
        "Brasil Novo",
        "Medicilândia",
        "Pacajá",
        "Placas",
        "Porto de Moz",
        "Senador José Porfírio",
        "Uruará",
        "Vitória do Xingu",
    ],
}

log = get_logger(dataset_id=DATASET_ID, zone=ZONE)


def extract() -> pd.DataFrame:
    log.info("extract.start", source="anexo_vi_fapespa")
    rows = [
        {"nome_municipio": municipio, "nome_regiao_integracao": regiao}
        for regiao, municipios in REGIOES_INTEGRACAO.items()
        for municipio in municipios
    ]
    df = pd.DataFrame(rows, columns=list(COLUMNS_DDL.keys()))
    log.info("extract.done", rows=len(df), regioes=len(REGIOES_INTEGRACAO))
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        log.error("validate.error", reason="empty_dataframe")
        raise ValueError("extract produced an empty dataframe")

    missing = set(COLUMNS_DDL.keys()) - set(df.columns)
    if missing:
        log.error("validate.error", missing_columns=sorted(missing))
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if len(df) != TOTAL_MUNICIPIOS_PA:
        log.error("validate.error", reason="unexpected_count", count=len(df))
        raise ValueError(
            f"Expected {TOTAL_MUNICIPIOS_PA} municipalities, got {len(df)}"
        )

    dupes = df.duplicated(subset=["nome_municipio"], keep=False)
    if dupes.any():
        offenders = sorted(df.loc[dupes, "nome_municipio"].unique())
        log.error("validate.error", reason="duplicate_municipio", offenders=offenders)
        raise ValueError(f"Municipality assigned to more than one region: {offenders}")
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
