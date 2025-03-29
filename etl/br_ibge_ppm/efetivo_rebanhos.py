import asyncio
import glob
from etl.utils.ibge_api_crawler import (
    async_crawler_censoagro,
)

API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO            = "3939" # É a tabela no SIDRA
PERIODOS            = range(1974, 2022 + 1)
VARIAVEIS           = ["105"] # As variáveis da tabela
NIVEL_GEOGRAFICO    = "N6" # N6 = Municipal
LOCALIDADES         = "1100015"
CLASSIFICACAO       = "79" # Código pré-definido por agregado
CATEGORIAS          = ["2670", "2675", "2672", "32794", "32795", "2681", "2677"
                       "32796", "32793", "2680"]  # Produtos
ANOS_BAIXADOS       = [int(glob.os.path.basename(f).split(".")[0]) for f in glob.glob(f"../json/*.json")]
ANOS_RESTANTES      = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]


if __name__ == "__main__":
    asyncio.run(
        async_crawler_censoagro(
            years=ANOS_RESTANTES, 
            variables=VARIAVEIS,
            categorias=CATEGORIAS,
            api_url_base=API_URL_BASE,
            agregado=AGREGADO,
            nivel_geografico=NIVEL_GEOGRAFICO,
            localidades=LOCALIDADES,
            classificacao=CLASSIFICACAO,
        )
    )
