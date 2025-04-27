import pandas as pd
import numpy as np

def process_ncm_codes(value:str)-> str:
    """
    Processa códigos NCM de uma célula, tratando corretamente códigos parciais
    incluindo casos especiais com múltiplos pontos.
    
    Exemplos:
    Caso 1:
      Entrada: ['1003.90.10 ', ' .80 ', ' .90 ']
      Saída:   ['1003.90.10', '1003.90.80', '1003.90.90']
    
    Caso 2 (especial - com múltiplos pontos no código parcial):
      Entrada: ['1001.19.00 ', ' .99.00']
      Saída:   ['1001.19.00', '1001.99.00']
    
    Regras:
    1. Para códigos parciais com um único ponto: manter o prefixo até o último ponto
       do código anterior e substituir apenas os dígitos após o último ponto.
    2. Para códigos parciais com múltiplos pontos: usar apenas a primeira parte
       do código anterior (antes do primeiro ponto).
    
    Args:
        value: Valor da célula da coluna id_ncm
        
    Returns:
        Lista de códigos NCM processados
    """
    # Tratamento de valores nulos
    if value is None or (isinstance(value, (float, str)) and pd.isna(value)):
        return []
    
    # Função auxiliar para processar um único código
    def process_single_code(code, previous):
        if not code or code.isspace():
            return None
            
        code = code.strip()
        
        if not code.startswith('.'):
            return code
            
        if not previous:
            return None
            
        # Contar quantos pontos existem no código parcial
        dot_count = code.count('.')
        
        if dot_count > 1:
            # Caso especial: código parcial tem múltiplos pontos
            # Usar apenas a primeira parte do código anterior
            first_part = previous.split('.')[0]
            return first_part + code
        else:
            # Caso normal: código parcial tem apenas um ponto
            # Manter o prefixo até o último ponto do código anterior
            last_dot_index = previous.rfind('.')
            if last_dot_index != -1:
                prefix = previous[:last_dot_index + 1]
                return prefix + code[1:]
        
        return None
    
    # Tratamento para tipos de lista/array
    if isinstance(value, (list, np.ndarray, pd.Series)):
        processed_codes = []
        previous_code = None
        
        for item in value:
            if item is None or pd.isna(item):
                continue
                
            code = str(item).strip()
            processed = process_single_code(code, previous_code)
            
            if processed:
                processed_codes.append(processed)
                previous_code = processed
                
        return processed_codes
        
    # Processamento para valor único (string)
    codes = str(value).split('+')
    
    processed_codes = []
    previous_code = None
    
    for code in codes:
        processed = process_single_code(code, previous_code)
        
        if processed:
            processed_codes.append(processed)
            previous_code = processed
            
    return processed_codes
