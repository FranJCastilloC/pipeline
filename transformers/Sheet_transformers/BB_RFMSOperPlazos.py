import pandas as pd
from ..base_transformer import BaseTransformer

def transform_rfms_oper_plazos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la hoja BB_RFMSOperPlazos.
    
    Args:
        df (pd.DataFrame): DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Crear una copia del DataFrame
    df_transformed = df.copy()
    
    # Renombrar columnas para estandarizar
    column_mapping = {
        'Emisor': 'emisor',
        'Código': 'codigo',
        'Instrumento': 'instrumento',
        'Fecha de Emisión': 'fecha_emision',
        'Fecha de Vencimiento': 'fecha_vencimiento',
        'Moneda': 'moneda',
        'Valor Nominal': 'valor_nominal',
        'Tasa de Interés': 'tasa_interes',
        'Precio': 'precio',
        'Rendimiento': 'rendimiento',
        'Valor Transado': 'valor_transado',
        'Cantidad de Operaciones': 'cantidad_operaciones',
        'Plazo': 'plazo',
        'Fecha de Inicio': 'fecha_inicio',
        'Fecha de Término': 'fecha_termino',
        'Modalidad': 'modalidad'
    }
    df_transformed = df_transformed.rename(columns=column_mapping)
    
    # Convertir fechas
    date_columns = ['fecha_emision', 'fecha_vencimiento', 'fecha_inicio', 'fecha_termino']
    for col in date_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
    
    # Convertir columnas numéricas
    numeric_columns = ['valor_nominal', 'tasa_interes', 'precio', 'rendimiento', 
                      'valor_transado', 'cantidad_operaciones', 'plazo']
    for col in numeric_columns:
        if col in df_transformed.columns:
            # Limpiar y convertir a numérico
            df_transformed[col] = pd.to_numeric(
                df_transformed[col].astype(str)
                .str.replace(',', '')
                .str.replace('%', '')
                .str.replace('$', '')
                .str.replace('días', '')
                .str.strip(),
                errors='coerce'
            )
    
    # Convertir tasas de porcentaje a decimal
    percentage_columns = ['tasa_interes', 'rendimiento']
    for col in percentage_columns:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col] / 100
    
    # Estandarizar modalidad
    if 'modalidad' in df_transformed.columns:
        df_transformed['modalidad'] = df_transformed['modalidad'].str.upper().str.strip()
    
    # Calcular plazo si no existe y tenemos las fechas necesarias
    if 'plazo' not in df_transformed.columns and 'fecha_inicio' in df_transformed.columns and 'fecha_termino' in df_transformed.columns:
        df_transformed['plazo'] = (df_transformed['fecha_termino'] - df_transformed['fecha_inicio']).dt.days
    
    # Eliminar filas con valores nulos en columnas críticas
    critical_columns = ['emisor', 'codigo', 'valor_transado', 'plazo', 'modalidad']
    df_transformed = df_transformed.dropna(subset=critical_columns, how='any')
    
    # Ordenar por plazo y valor transado
    df_transformed = df_transformed.sort_values(['modalidad', 'plazo', 'valor_transado'], 
                                              ascending=[True, True, False])
    
    return df_transformed 