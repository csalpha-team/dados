import pandas as pd
from typing import List


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
        Nome da coluna contendo o número de estabelecimentos para multiplicar a média.
        Se None, a média não será multiplicar.
       
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

import pandas as pd
import numpy as np
from typing import List

def fix_ibge_x_digit(df: pd.DataFrame, column: str, group_vars: List[str], 
                    div_column: str = None) -> pd.DataFrame:
    """
    Substitui valores 'X' utilizando a lógica de Média Unitária (Ratio Imputation).
    
    Lógica:
    1. Calcula a razão (Valor / Divisor) para os dados existentes.
    2. Encontra a média dessa razão por grupo.
    3. Para os valores 'X', imputa: (Média da Razão do Grupo) * (Valor do Divisor da linha)
    
    Se div_column for None, utiliza a média simples (Média Normal).
    """
    # Cria uma cópia para evitar modificar o dataframe original
    df_fixed = df.copy()
    
    # Contabiliza quantos valores 'X' existem inicialmente
    x_count_before = (df_fixed[column] == 'X').sum()
    print(f"Total de valores 'X' para substituir na coluna {column}: {x_count_before}")
    
    # 1. Tratamento de Tipos
    # Garante que a coluna alvo é numérica (X vira NaN)
    df_fixed[column] = pd.to_numeric(df_fixed[column].replace('X', np.nan), errors='coerce')
    
    # Garante que a coluna divisora é numérica (se existir)
    if div_column:
        df_fixed[div_column] = pd.to_numeric(df_fixed[div_column], errors='coerce').fillna(0)

    # 2. Cálculo da Coluna de Razão (Unitária)
    # Variável auxiliar para saber qual coluna usar no cálculo da média
    target_col_for_mean = column 
    
    if div_column:
        # Nome temporário para a coluna de produtividade/razão
        ratio_col = f'_temp_ratio_{column}'
        
        # Cria a coluna: Valor / Estabelecimentos (evita divisão por zero)
        # Linhas com NaN em 'column' resultarão em NaN 
        df_fixed[ratio_col] = df_fixed.apply(
            lambda row: row[column] / row[div_column] if (row[div_column] > 0 and pd.notna(row[column])) else np.nan, 
            axis=1
        )
        target_col_for_mean = ratio_col

    # Dicionário para rastrear substituições por UF (apenas para log)
    uf_substitutions = {}
    for uf_code in df_fixed['id_municipio'].str[0:2].unique():
        uf_substitutions[uf_code] = 0
    
    # Variáveis de agrupamento sem o município
    group_vars_without_mun = [var for var in group_vars if var != 'id_municipio']
    
    # 3. Processamento por UF
    for uf_code in df_fixed['id_municipio'].str[0:2].unique():
        # Filtra a UF
        uf_mask = df_fixed['id_municipio'].str[0:2] == uf_code
        uf_df = df_fixed[uf_mask].copy()
        
        # Se houver variáveis de grupo, calcula médias agrupadas
        if group_vars_without_mun:
            # Calcula a média da variável alvo (seja ela o Total ou a Razão Unitária)
            group_means = uf_df.groupby(group_vars_without_mun)[target_col_for_mean].mean().reset_index()
            
            for _, row in group_means.iterrows():
                # Cria máscara para encontrar as linhas deste grupo específico dentro da UF
                group_mask = pd.Series(True, index=uf_df.index)
                for var in group_vars_without_mun:
                    group_mask &= (uf_df[var] == row[var])
                
                # Identifica onde temos 'X' (que agora é NaN na coluna original)
                x_mask = group_mask & uf_df[column].isna()
                x_count = x_mask.sum()
                
                # Se houver buracos para preencher E tivermos uma média válida para o grupo
                if x_count > 0 and not pd.isna(row[target_col_for_mean]):
                    mean_val = row[target_col_for_mean]
                    
                    if div_column:
                        # Valor = (Média Unitária do Grupo) * (Quantidade de Estabelecimentos da Linha)
                        for idx in uf_df.loc[x_mask].index:
                            div_val = df_fixed.loc[idx, div_column]
                            imputed_value = mean_val * div_val
                            df_fixed.loc[idx, column] = imputed_value
                    else:
                        # Lógica Antiga (Fallback): Substituição direta pela média simples
                        df_fixed.loc[x_mask, column] = mean_val
                    
                    uf_substitutions[uf_code] += x_count
                    
        else:
            # Fallback se não houver grupos: Média geral da UF
            mean_val = uf_df[target_col_for_mean].mean()
            x_mask = uf_df[column].isna()
            
            if x_mask.sum() > 0 and not pd.isna(mean_val):
                if div_column:
                    for idx in uf_df.loc[x_mask].index:
                        df_fixed.loc[idx, column] = mean_val * df_fixed.loc[idx, div_column]
                else:
                    df_fixed.loc[x_mask, column] = mean_val
                
                uf_substitutions[uf_code] += x_mask.sum()

    # 4. Limpeza e Logs Finais
    
    # Remove a coluna temporária se foi criada
    if div_column and f'_temp_ratio_{column}' in df_fixed.columns:
        df_fixed.drop(columns=[f'_temp_ratio_{column}'], inplace=True)

    print("\nResumo de substituições por UF:")
    for uf, count in uf_substitutions.items():
        if count > 0: print(f"  UF {uf}: {count} valores substituídos")
    
    # Preenchimento de remanescentes com 0 (casos onde não houve média de grupo disponível)
    remaining_nan = df_fixed[column].isna().sum()
    if remaining_nan > 0:
        print(f"AVISO: {remaining_nan} valores não puderam ser calculados (sem média de grupo). Preenchendo com 0.")
        df_fixed[column] = df_fixed[column].fillna(0)
    
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



def calcula_autoconsumo_comercio(
    df: pd.DataFrame, 
    id_cols: List[str], 
    metric_cols: List[str], 
    category_col: str,
    total_label: str = 'Total',
    consumo_label: str = 'Consumo no estabelecimento'
) -> pd.DataFrame:
    """
    Pivota o dataframe para criar colunas de Total e Autoconsumo, 
    e calcula o Comércio (Total - Autoconsumo).

    Retorna um DataFrame com:
    - Colunas originais (Total)
    - Colunas com prefixo 'autoconsumo_'
    - Colunas com prefixo 'comercio_'
    """
    
    # 1. Pivotagem
    # Transforma 'Total' e 'Consumo' em colunas hierárquicas
    # fill_value=0 garante que se não houver registro de consumo, seja 0 e não NaN
    df_pivot = df.pivot_table(
        index=id_cols,
        columns=category_col,
        values=metric_cols,
        aggfunc='sum',
        fill_value=0
    )
    
    # O df_pivot agora tem colunas MultiIndex: (Métrica, Categoria)
    # Ex: ('quantidade_produzida', 'Total')
    
    # 2. Construção do DataFrame Final Plano
    output_df = pd.DataFrame(index=df_pivot.index)
    
    for metric in metric_cols:
        # Acessa as séries usando o MultiIndex do pivot
        # Usamos .get() ou try/except implícito para segurança caso falte uma categoria inteira
        try:
            val_total = df_pivot[(metric, total_label)]
        except KeyError:
            val_total = 0
            
        try:
            val_consumo = df_pivot[(metric, consumo_label)]
        except KeyError:
            val_consumo = 0
            
        # 2.1 Mantém nome original para o TOTAL
        output_df[metric] = val_total
        
        # 2.2 Cria variável com prefixo AUTOCONSUMO
        output_df[f'autoconsumo_{metric}'] = val_consumo
        
        # 2.3 Cria variável calculada COMERCIO (Total - Autoconsumo)
        # clip(lower=0) previne negativos por erros de dados
        output_df[f'comercio_{metric}'] = (output_df[metric] - output_df[f'autoconsumo_{metric}']).clip(lower=0)

    # 3. Reset do índice para trazer id_cols de volta como colunas normais
    return output_df.reset_index()

