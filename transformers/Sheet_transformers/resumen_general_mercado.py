"""
Transformador para la hoja ResumenGeneralMercado.
Desarrollo y limpieza de datos.
"""

import pandas as pd
import sys
import os

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Importar función para obtener datos
from extraction.data_manager import get_dataset

# Configuración para desarrollo
start_date = "2025-06-17"
end_date = "2025-06-17"
sheet_name = "BB_ResumenGeneralMercado"

# Obtener DataFrame para desarrollo
df = get_dataset(start_date, end_date, sheet_name)

if df is not None:
    print(f"DataFrame obtenido: {df.shape}")
    print("\nPrimeras filas:")
    print(df.head(50))
    print("\nColumnas:")
    print(list(df.columns))
else:
    print("No se encontró el DataFrame solicitado.")

def transform_resumen_general_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de ResumenGeneralMercado.
    
    Args:
        df (pd.DataFrame): DataFrame original de ResumenGeneralMercado
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # TODO: Implementar transformación
    return df







