import pandas as pd
from ..base_transformer import BaseTransformer

def transform_rfv_trans_puesto_bolsa_mp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la hoja BB_RFVTransPuestoBolsaMP.
    
    Args:
        df (pd.DataFrame): DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Crear una copia del DataFrame
    df_transformed = df.copy()
    
    # Renombrar columnas para estandarizar
    column_mapping = {
        'Puesto de Bolsa': 'puesto_bolsa',
        'Monto Transado': 'monto_transado',
        'Cantidad de Operaciones': 'cantidad_operaciones',
        'Participación': 'participacion'
    }
    df_transformed = df_transformed.rename(columns=column_mapping)
    
    # Convertir columnas numéricas
    numeric_columns = ['monto_transado', 'cantidad_operaciones', 'participacion']
    for col in numeric_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col].astype(str).str.replace(',', '').str.replace('%', ''), errors='coerce')
    
    # Si participacion está en porcentaje, convertir a decimal
    if 'participacion' in df_transformed.columns:
        df_transformed['participacion'] = df_transformed['participacion'] / 100
    
    # Eliminar filas con todos los valores numéricos en NA
    df_transformed = df_transformed.dropna(subset=numeric_columns, how='all')
    
    # Ordenar por monto transado descendente
    df_transformed = df_transformed.sort_values('monto_transado', ascending=False)
    
    return df_transformed 