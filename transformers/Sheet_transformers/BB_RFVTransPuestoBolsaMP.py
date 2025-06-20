import pandas as pd
import sys
import os

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Importar desde la ubicación correcta
from transformers.Sheet_transformers.funciones_de_limpieza import LimpiezaExcel

def transform_rfv_trans_puesto_bolsa_mp(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la hoja BB_RFVTransPuestoBolsaMP.
    
    Args:
        df (pd.DataFrame): DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    # Aplicar la función recortar_df dentro de la función de transformación
    # Usar la segunda coincidencia de "Participante" y la primera de "Total"
    df = LimpiezaExcel().recortar_df(df, columna=0, palabra_inicial="Participante", palabra_final="Total", n_inicial=2, n_final=1)
    
    # Seleccionar solo las columnas necesarias
    df = LimpiezaExcel().seleccionar_columnas(df, [0, 1, 2, 3, 7])

    # Renombrar las columnas
    df.columns = [
        "participante", 
        'transado_usd',
        'usd_equivalente_dop',
        'transado_dop',
        'fecha'
    ]
    
    # Limpiar espacios en blanco de la columna participante
    df['participante'] = df['participante'].str.strip()
    
    return df
