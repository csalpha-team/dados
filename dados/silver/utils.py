import pandas as pd
from typing import List
import pandas as pd

def fix_ibge_digits(df: pd.DataFrame, columns: List[str], group_vars: List[str] = None, 
                   div_column: str = None) -> pd.DataFrame:
    """
    Corrige valores não numéricos em colunas específicas de um dataframe do IBGE,
    substituindo valores não numéricos específicos por 0 e valores 'X' por
    médias apropriadas agrupadas pelas variáveis especificadas.
   
    #! -	"Zero absoluto, não resultante de um cálculo ou arredondamento.
    #Ex: Em determinado município não existem pessoas de 14 anos de idade sem instrução."
    #! 0	"Zero resultante de um cálculo ou arredondamento.
    #Ex: A inflação do feijão em determinada Região Metropolitana foi 0.
    #EX. Determinado município produziu 400 kg de sementes de girassol e os dados da tabela são expressos em toneladas."
    #! X	"Valor inibido para não identificar o informante.
    #Ex: Determinado município só possui uma empresa produtora de cimento, logo o valor de sua produção deve ser inibido."
    #! ..	"Valor não se aplica.
    #Ex: Não se pode obter o total da produção agrícola em determinado município quando os produtos agrícolas são contabilizados com unidades de medida distintas."
    #! ...	"Valor não disponível.
    #Ex: A produção de feijão em determinado município não foi pesquisada ou determinado município não existia no ano da pesquisa."

    Parâmetros:
    -----------
    df : pd.DataFrame
        O dataframe contendo dados do IBGE
    columns : List[str]
        Lista de nomes de colunas para processar
    group_vars : List[str], opcional
        Lista de variáveis para agrupar no cálculo das médias.
        Padrão é ['id_municipio', 'ano', 'produto'] se None
    div_column : str, opcional
        Nome da coluna contendo o número de estabelecimentos para dividir a média.
        Se None, a média não será dividida.
       
    Retorna:
    --------
    pd.DataFrame
        O dataframe com valores corrigidos
    
    Raises:
    -------
    ValueError
        Se algum valor 'X' não puder ser substituído
    """
    # Cria uma cópia para evitar modificar o dataframe original
    df_fixed = df.copy()
    
    # Define as variáveis de agrupamento padrão se não fornecidas
    if group_vars is None:
        group_vars = ['id_municipio', 'ano', 'produto']
    
    # Garantir que 'id_municipio' está nas variáveis de agrupamento para extrair UF
    if 'id_municipio' not in group_vars:
        print("AVISO: 'id_municipio' não está nas variáveis de agrupamento. Adicionando para extrair UF...")
        group_vars = ['id_municipio'] + group_vars
   
    # Operação será realizada por UF (estado)
    # Extrai códigos de estado únicos dos IDs de municípios
    uf_codes = df_fixed['id_municipio'].str[0:2].unique()
   
    for column in columns:
        # Filtra valores que não são dígitos e imprime suas ocorrências
        non_digit_values = df_fixed[column][~df_fixed[column].astype(str).str.isdigit()]
        print(f"Valores não numéricos na coluna {column}:")
        print(non_digit_values.value_counts())
       
        # Substitui '..' e '-' por 0
        df_fixed[column] = df_fixed[column].apply(lambda x: 0 if x in ("..", "...", "-") else x)
       
        # Se valores 'X' forem encontrados, calcula e substitui pelas médias agrupadas
        if 'X' in non_digit_values.values:
            print(f"\nSubstituindo valores 'X' na coluna {column} por médias agrupadas...")
            df_fixed = fix_ibge_x_digit(df_fixed, column, group_vars, div_column)
   
    return df_fixed

def fix_ibge_x_digit(df: pd.DataFrame, column: str, group_vars: List[str], 
                    div_column: str = None) -> pd.DataFrame:
    """
    Substitui valores 'X' em uma coluna pela média calculada
    agrupando pelos valores em group_vars. Se div_column for fornecido,
    a média será dividida pelo valor dessa coluna.
   
    Parâmetros:
    -----------
    df : pd.DataFrame
        O dataframe contendo dados do IBGE
    column : str
        Nome da coluna a ser processada
    group_vars : List[str]
        Lista de variáveis para agrupar no cálculo das médias
    div_column : str, opcional
        Nome da coluna contendo o número de estabelecimentos para dividir a média.
        Se None, a média não será dividida.
       
    Retorna:
    --------
    pd.DataFrame
        O dataframe com valores 'X' substituídos por médias
    
    Raises:
    -------
    ValueError
        Se algum valor 'X' não puder ser substituído
    """
    # Cria uma cópia para evitar modificar o dataframe original
    df_fixed = df.copy()
    
    # Contabiliza quantos valores 'X' existem inicialmente
    x_count_before = (df_fixed[column] == 'X').sum()
    print(f"Total de valores 'X' para substituir na coluna {column}: {x_count_before}")
   
    # Garante que a coluna é numérica para cálculo (exceto valores 'X')
    df_fixed[column] = pd.to_numeric(df_fixed[column].replace('X', None), errors='coerce')
   
    # Dicionário para rastrear substituições por UF
    uf_substitutions = {}
    for uf_code in df_fixed['id_municipio'].str[0:2].unique():
        uf_substitutions[uf_code] = 0
    
    # Obtém todas as combinações únicas de variáveis de agrupamento
    # Removemos 'id_municipio' do agrupamento, mas mantemos para filtragem por UF
    group_vars_without_mun = [var for var in group_vars if var != 'id_municipio']
    
    # Processa cada UF separadamente
    for uf_code in df_fixed['id_municipio'].str[0:2].unique():
        # Filtra o dataframe para a UF atual
        uf_mask = df_fixed['id_municipio'].str[0:2] == uf_code
        uf_df = df_fixed[uf_mask].copy()
        
        # Calcula as médias para cada combinação de variáveis de agrupamento
        # Agrupa por todas as variáveis exceto 'id_municipio'
        if group_vars_without_mun:
            group_means = uf_df.groupby(group_vars_without_mun)[column].mean().reset_index()
            
            # Para cada grupo, aplica a média correspondente
            for _, row in group_means.iterrows():
                # Cria uma máscara para este grupo
                group_mask = pd.Series(True, index=uf_df.index)
                for var in group_vars_without_mun:
                    group_mask &= (uf_df[var] == row[var])
                
                # Adiciona a máscara para valores 'X' (agora NaN)
                x_mask = group_mask & uf_df[column].isna()
                
                # Aplica a média se existirem valores a substituir e a média não for NaN
                x_count = x_mask.sum()
                if x_count > 0 and not pd.isna(row[column]):
                    mean_value = row[column]
                    
                    # Se div_column for fornecido, divide a média pelo valor dessa coluna
                    if div_column is not None:
                        # Para cada linha com valor X, divide a média pelo valor de div_column
                        for idx in uf_df.loc[x_mask].index:
                            div_value = df_fixed.loc[idx, div_column]
                            # Evita divisão por zero
                            try:
                                df_fixed.loc[idx, column] = mean_value / int(div_value)
                            except ZeroDivisionError as e:
                                df_fixed.loc[idx, column] = mean_value  # Mantém o valor original se div_value for zero
                    else:
                        # Comportamento original: substitui diretamente pela média
                        df_fixed.loc[x_mask, column] = mean_value
                    
                    uf_substitutions[uf_code] += x_count
        else:
            # Se não houver variáveis de agrupamento além de 'id_municipio',
            # calcula a média geral da UF
            mean_value = uf_df[column].mean()
            x_mask = uf_df[column].isna()
            x_count = x_mask.sum()
            if x_count > 0 and not pd.isna(mean_value):
                if div_column is not None:
                    # Para cada linha com valor X, divide a média pelo valor de div_column
                    for idx in uf_df.loc[x_mask].index:
                        div_value = df_fixed.loc[idx, div_column]
                        # Evita divisão por zero
                        if div_value != 0:
                            df_fixed.loc[idx, column] = mean_value / div_value
                        else:
                            df_fixed.loc[idx, column] = mean_value  # Mantém o valor original se div_value for zero
                else:
                    # Comportamento original: substitui diretamente pela média
                    df_fixed.loc[x_mask, column] = mean_value
                
                uf_substitutions[uf_code] += x_count
    
    print("\nResumo de substituições por UF:")
    for uf, count in uf_substitutions.items():
        print(f"  UF {uf}: {count} valores substituídos")
    
    remaining_x = df_fixed[column].isna().sum()
    if remaining_x > 0:
        print(f"AVISO: Ainda existem {remaining_x} valores 'X' não substituídos na coluna {column}.")
        print("Convertendo valores remanescentes para 0.")
        # Substitui valores NaN remanescentes por 0
        df_fixed[column] = df_fixed[column].fillna(0)
    
    remaining_x = (df_fixed[column] == 'X').sum()
    if remaining_x > 0:
        print(f"AVISO: Ainda existem {remaining_x} valores literais 'X' não substituídos na coluna {column}.")
        print("Convertendo valores 'X' remanescentes para 0.")
        # Substitui valores 'X' remanescentes por 0
        df_fixed[column] = df_fixed[column].replace('X', 0)
   
    return df_fixed

        

def currency_fix(row):
    """
    Valor da produção (Mil Cruzeiros [1974 a 1985, 1990 a 1992], Mil Cruzados [1986 a 1988],
    Mil Cruzados Novos [1989], Mil Cruzeiros Reais [1993], Mil Reais [1994 a 2022])
    Verificado em http://www.igf.com.br/calculadoras/conversor/conversor.htm
    """

    if 1974 <= row["ano"] <= 1985:
        return row["valor_producao"] / (1000**4 * 2.75)
    elif 1986 <= row["ano"] <= 1988:
        return row["valor_producao"] / (1000**3 * 2.75)
    elif row["ano"] == 1989:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif 1990 <= row["ano"] <= 1992:
        return row["valor_producao"] / (1000**2 * 2.75)
    elif row["ano"] == 1993:
        return row["valor_producao"] / (1000 * 2.75)
    else:
        return row["valor_producao"]



def products_weight_ratio_fix(row):
    """
    2 - A partir do ano de 2001 as quantidades produzidas dos produtos abacate, banana,
    caqui, figo, goiaba, laranja, limão, maçã, mamão, manga, maracujá, marmelo, pera,
    pêssego e tangerina passam a ser expressas em toneladas. Nos anos anteriores eram
    expressas em mil frutos, com exceção da banana, que era expressa em mil cachos. O
    rendimento médio passa a ser expresso em Kg/ha. Nos anos anteriores era expresso
    em frutos/ha, com exceção da banana, que era expressa em cachos/ha.
    3 - Veja em o documento
    https://sidra.ibge.gov.br/content/documentos/pam/AlteracoesUnidadesMedidaFrutas.pdf
    com as alterações de unidades de medida das frutíferas ocorridas em 2001 e a tabela
    de conversão fruto x quilograma.
    """

    DICIONARIO_DE_PROPORCOES = {
        "Abacate": 0.38,
        "Banana (cacho)": 10.20,
        "Caqui": 0.18,
        "Figo": 0.09,
        "Goiaba": 0.16,
        "Larajna": 0.16,
        "Limão": 0.10,
        "Maçã": 0.15,
        "Mamão": 0.80,
        "Manga": 0.31,
        "Maracujá": 0.15,
        "Marmelo": 0.19,
        "Pera": 0.17,
        "Pêra": 0.17, # Para garantir, pois nos dados parece que só há Pera, sem acento
        "Pêssego": 0.13,
        "Tangerina": 0.15,
        "Melancia": 6.08,
        "Melão": 1.39
    }

    if row["ano"] >= 2001:
        return row

    if (pd.isna(row["quantidade_produzida"]) or pd.isna(row["area_colhida"])
        or row["quantidade_produzida"] == 0 or row["area_colhida"] == 0):
        return row

    if row["produto"] not in DICIONARIO_DE_PROPORCOES.keys():
        return row

    quantidade_produzida = row["quantidade_produzida"] * DICIONARIO_DE_PROPORCOES[row["produto"]]

    rendimento_medio_producao = quantidade_produzida / row["area_colhida"] * 1000 # kg / ha

    row["quantidade_produzida"] = quantidade_produzida
    row["rendimento_medio_producao"] = rendimento_medio_producao

    return row



def check_duplicates(data: pd.DataFrame, columns: List) -> None:
    """
    Verifica linhas duplicadas no DataFrame com base nas colunas especificadas.

    Args:
        data (pd.DataFrame): O DataFrame a ser verificado para duplicatas.
        columns (List): A lista de nomes de colunas a serem consideradas para a detecção de duplicatas.

    Raises:
        ValueError: Se forem encontradas linhas duplicadas no DataFrame.
    """
    print(f"Verificando duplicatas com base nas colunas: {columns}")
    
    duplicates = data.duplicated(subset=columns, keep=False)
    
    if duplicates.any():
        duplicate_rows = data[duplicates]
        print("Foram encontradas combinações duplicadas:")
        print(duplicate_rows.count())
        raise ValueError("Foram encontrados múltiplos valores para algumas combinações das colunas especificadas.")
    else:
        print("Nenhuma combinação duplicada encontrada.")


