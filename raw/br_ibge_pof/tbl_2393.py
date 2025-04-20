import asyncio
import glob
from raw.utils.ibge_api_crawler import (
    async_crawler,
)
#https://servicodados.ibge.gov.br/api/docs/agregados?versao=3#api-bq
API_URL_BASE        = "https://servicodados.ibge.gov.br/api/v3/agregados/{}/periodos/{}/variaveis/{}?localidades={}[{}]&classificacao={}[{}]"
AGREGADO         = "2393"
PERIODOS         = [2002, 2008,2018]
VARIAVEIS        = ["1207"]
NIVEL_GEOGRAFICO = "N1"
LOCALIDADES      = "all"
CLASSIFICACAO    = "217"
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
