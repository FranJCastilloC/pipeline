import pandas as pd
from ..base_transformer import BaseTransformer

def transform_renta_fija_operaciones_futuras(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la hoja BB_RentaFijaOperacionesFuturasA.
    
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
        'Fecha de Operación': 'fecha_operacion',
        'Fecha de Liquidación': 'fecha_liquidacion',
        'Precio': 'precio',
        'Rendimiento': 'rendimiento',
        'Valor Transado': 'valor_transado',
        'Cantidad de Operaciones': 'cantidad_operaciones'
    }
    df_transformed = df_transformed.rename(columns=column_mapping)
    
    # Convertir fechas
    date_columns = ['fecha_emision', 'fecha_vencimiento', 'fecha_operacion', 'fecha_liquidacion']
    for col in date_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
    
    # Convertir columnas numéricas
    numeric_columns = ['valor_nominal', 'tasa_interes', 'precio', 'rendimiento', 
                      'valor_transado', 'cantidad_operaciones']
    for col in numeric_columns:
        if col in df_transformed.columns:
            # Limpiar y convertir a numérico
            df_transformed[col] = pd.to_numeric(
                df_transformed[col].astype(str)
                .str.replace(',', '')
                .str.replace('%', '')
                .str.replace('$', ''),
                errors='coerce'
            )
    
    # Convertir tasas de porcentaje a decimal
    percentage_columns = ['tasa_interes', 'rendimiento']
    for col in percentage_columns:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col] / 100
    
    # Calcular días hasta liquidación
    if 'fecha_operacion' in df_transformed.columns and 'fecha_liquidacion' in df_transformed.columns:
        df_transformed['dias_hasta_liquidacion'] = (
            df_transformed['fecha_liquidacion'] - df_transformed['fecha_operacion']
        ).dt.days
    
    # Eliminar filas con valores nulos en columnas críticas
    critical_columns = ['emisor', 'codigo', 'valor_transado', 'fecha_operacion', 'fecha_liquidacion']
    df_transformed = df_transformed.dropna(subset=critical_columns, how='any')
    
    # Ordenar por fecha de liquidación y valor transado
    df_transformed = df_transformed.sort_values(['fecha_liquidacion', 'valor_transado'], 
                                              ascending=[True, False])
    
    return df_transformed 