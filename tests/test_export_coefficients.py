from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pandas as pd

from dados.gold.br_coeficientes_exportacao.preparacao_camada_exportacao import (
    BrCoeficientesExportacaoPreparacaoCamadaExportacao,
)
from dados.gold.br_coeficientes_exportacao.utils import (
    COLUNAS_FINAIS,
    carregar_parametros_brutos,
    carregar_parametros_exportacao,
    construir_verificacao_matches,
    gerar_grafico_series_coeficientes,
    preparar_dados_coeficientes_exportacao,
    salvar_resumo_coeficientes,
    salvar_verificacao_matches,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PARAMETROS_PATH = (
    REPO_ROOT
    / "dados"
    / "gold"
    / "br_coeficientes_exportacao"
    / "parametros_coeficientes_exportacao.json"
)
RAW_COMEX_PATH = REPO_ROOT / "dados" / "raw" / "pa_me_comex_stat"


def _normalizar_texto(valor: str) -> str:
    sem_acento = (
        unicodedata.normalize("NFKD", str(valor))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    return re.sub(r"[^a-z0-9]+", " ", sem_acento).strip()


def test_carrega_composicao_enriquecida_e_taxa_cambio_csv(tmp_path) -> None:
    taxas_path = tmp_path / "taxa_cambio.csv"
    taxas_path.write_text("ano,taxa_cambio\n2020,5.0\n2021,6.0\n", encoding="utf-8")

    config_path = tmp_path / "parametros.json"
    config_path.write_text(
        json.dumps(
            {
                "uf_alvo": "PA",
                "anos_previsao": [2020, 2021],
                "taxa_cambio_brl_por_usd": {
                    "arquivo": "taxa_cambio.csv",
                    "coluna_ano": "ano",
                    "coluna_taxa": "taxa_cambio",
                },
                "composicao_produtos": {
                    "AcaiFruto": [
                        {
                            "id_ncm": "20079921",
                            "nome_ncm": "Purês de açaí (Euterpe oleracea)",
                        }
                    ]
                },
                "participacoes_especificas": [],
            }
        ),
        encoding="utf-8",
    )

    preparacoes, participacoes, anos, taxas, uf = carregar_parametros_exportacao(
        config_path
    )

    assert preparacoes == {"AcaiFruto": ["Purês de açaí (Euterpe oleracea)"]}
    assert participacoes == {}
    assert anos == [2020, 2021]
    assert taxas == {2020: 5.0, 2021: 6.0}
    assert uf == "PA"


def test_parametros_gold_sao_compativeis_com_raws_de_apoio() -> None:
    parametros = json.loads(PARAMETROS_PATH.read_text(encoding="utf-8"))
    produtos_unicos = json.loads(
        (RAW_COMEX_PATH / "produtos_unicos_matrizes.json").read_text(encoding="utf-8")
    )
    catalogo_ncm = pd.read_csv(
        RAW_COMEX_PATH / "NCM.csv",
        sep=";",
        encoding="cp1252",
        dtype=str,
    )
    taxas_cambio = pd.read_csv(RAW_COMEX_PATH / "taxa_cambio.csv")

    assert list(parametros["composicao_produtos"].keys()) == produtos_unicos

    catalogo_por_codigo = {
        str(linha.CO_NCM): _normalizar_texto(linha.NO_NCM_POR)
        for linha in catalogo_ncm.itertuples()
    }
    for produto, composicao in parametros["composicao_produtos"].items():
        assert isinstance(composicao, list), produto
        for item in composicao:
            assert {"id_ncm", "nome_ncm"}.issubset(set(item))
            assert set(item).issubset(
                {
                    "id_ncm",
                    "nome_ncm",
                    "tipo_match",
                    "participacao",
                    "grupo_distribuicao",
                }
            )
            assert item["id_ncm"] in catalogo_por_codigo
            assert (
                _normalizar_texto(item["nome_ncm"])
                == catalogo_por_codigo[item["id_ncm"]]
            )

    anos_taxa = set(taxas_cambio["ano"].astype(int))
    assert set(parametros["anos_previsao"]).issubset(anos_taxa)


def test_auditoria_matches_ncm_gera_verificacao_sintetica(tmp_path) -> None:
    parametros = carregar_parametros_brutos(PARAMETROS_PATH)
    verificacao = construir_verificacao_matches(parametros)
    caminho = salvar_verificacao_matches(
        parametros, tmp_path / "auditoria_matches_ncm.xlsx"
    )

    assert not verificacao.empty
    assert verificacao.columns.tolist() == ["produto", "id_ncm", "nome_ncm"]
    assert caminho.exists()
    assert pd.ExcelFile(caminho).sheet_names == ["matches"]


def test_preparacao_aplica_taxa_cambio_por_ano() -> None:
    exportacoes = pd.DataFrame(
        {
            "ano": [2020, 2021],
            "id_ncm": ["20079921", "20079921"],
            "nome_ncm_portugues": [
                "Purês de açaí (Euterpe oleracea)",
                "Purês de açaí (Euterpe oleracea)",
            ],
            "sigla_uf_ncm": ["PA", "PA"],
            "valor_fob_dolar": [1000.0, 1000.0],
        }
    )

    resultado = preparar_dados_coeficientes_exportacao(
        exportacoes,
        preparacoes_produtos={"AcaiFruto": ["Purês de açaí (Euterpe oleracea)"]},
        participacoes_especificas={},
        anos=[2020, 2021],
        taxa_cambio_brl_por_usd={2020: 5.0, 2021: 6.0},
        uf_alvo="PA",
    )

    assert resultado["valor_fob_real"].tolist() == [5.0, 6.0]


def test_pipeline_gold_exportacao_entrega_contrato_e_coeficientes() -> None:
    exportacoes_raw = pd.DataFrame(
        {
            "ano": [2020, 2020, 2020, 2020, 2021, 2021],
            "id_ncm": [
                "20079921",
                "20098990",
                "20079921",
                "20079921",
                "20079921",
                "20098990",
            ],
            "nome_ncm_portugues": [
                "Purês de açaí (Euterpe oleracea)",
                "Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar",
                "Purês de açaí (Euterpe oleracea)",
                "Purês de açaí (Euterpe oleracea)",
                "Purês de açaí (Euterpe oleracea)",
                "Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar",
            ],
            "sigla_uf_ncm": ["PA", "PA", "PA", "MA", "PA", "PA"],
            "valor_fob_dolar": [1000, 1000, 500, 9999, 2000, 1000],
        }
    )

    resultado = preparar_dados_coeficientes_exportacao(
        exportacoes_raw,
        preparacoes_produtos={
            "AcaiFruto": [
                "Purês de açaí (Euterpe oleracea)",
                "Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar",
            ],
            "Abacaxi": [
                "Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar",
            ],
        },
        participacoes_especificas={
            (
                "AcaiFruto",
                "Sucos (sumo) de outras frutas, não fermentado, sem adição de açúcar",
            ): 0.8,
        },
        anos=[2020, 2021],
        taxa_cambio_brl_por_usd={2020: 5.0, 2021: 6.0},
        uf_alvo="PA",
    )

    assert resultado.columns.tolist() == COLUNAS_FINAIS
    assert resultado["ano"].tolist() == [2020, 2020, 2021, 2021]
    assert set(resultado["produto"]) == {"Abacaxi", "AcaiFruto"}
    for soma_coeff in resultado.groupby("ano")["coeff"].sum():
        assert abs(float(soma_coeff) - 1.0) < 1e-12

    registros = resultado.to_dict("records")
    [BrCoeficientesExportacaoPreparacaoCamadaExportacao(**r) for r in registros]

    acai_2020 = resultado.query("ano == 2020 and produto == 'AcaiFruto'").iloc[0]
    assert acai_2020["valor_fob_dolar"] == 2300.0
    assert acai_2020["valor_fob_real"] == 11.5


def test_auditoria_series_gera_grafico_e_resumo(tmp_path) -> None:
    coeficientes = pd.DataFrame(
        {
            "ano": [2020, 2020, 2021, 2021],
            "produto": ["AcaiFruto", "Abacaxi", "AcaiFruto", "Abacaxi"],
            "valor_fob_dolar": [1000, 500, 1200, 300],
            "valor_fob_real": [5, 2.5, 7.2, 1.8],
            "coeff": [0.6667, 0.3333, 0.8, 0.2],
        }
    )

    grafico = gerar_grafico_series_coeficientes(
        coeficientes, tmp_path / "grafico.png"
    )
    resumo = salvar_resumo_coeficientes(coeficientes, tmp_path / "resumo.csv")

    assert grafico.exists()
    assert grafico.stat().st_size > 0
    assert resumo.exists()
    assert pd.read_csv(resumo).columns.tolist() == [
        "produto",
        "coeff_medio",
        "coeff_maximo",
        "anos",
    ]
