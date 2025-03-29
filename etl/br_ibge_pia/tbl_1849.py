import asyncio
import glob
from etl.utils.ibge_api_crawler import (
    async_crawler,
)

API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO         = "1849"
PERIODOS         = range(2007, 2022+1)
VARIAVEIS        = ["839", "840","810", "811"]
NIVEL_GEOGRAFICO = "N3"
LOCALIDADES      = "all"
CLASSIFICACAO    = "12762"
CATEGORIAS       = "all"

ANOS_BAIXADOS       = [int(glob.os.path.basename(f).split(".")[0]) for f in glob.glob(f"../json/*.json")]
ANOS_RESTANTES      = [int(ANO) for ANO in PERIODOS if ANO not in ANOS_BAIXADOS]

print(f"Anos baixados: {ANOS_BAIXADOS}")
print(f"Anos restantes: {ANOS_RESTANTES}")


if __name__ == "__main__":
    asyncio.run(
        async_crawler(
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
