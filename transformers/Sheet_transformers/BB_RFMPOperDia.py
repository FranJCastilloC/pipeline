import pandas as pd
import sys
import os

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))


from transformers.Sheet_transformers.funciones_de_limpieza import LimpiezaExcel

def transform_rfmp_oper_dia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma los datos de la hoja BB_RFMPOperDia.
    
    Args:
        df (pd.DataFrame): DataFrame original
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    df.drop(columns=[df.columns[5]], inplace=True)
    
    df.columns = [
        "numero_operacion",
        "rueda",
        "Cod_Local",
        "Cod_ISIN",
        "Cod_Emisor",
        "Fecha_Venc",
        "Frec_Pago",
        "Tasa_Cupon",
        "Nom_Unit",
        "Valor_Negociado",
        "Precio",
        "Valor_Transado",
        "Rend_Equiv",
        "Mon",
        "Equiv_en_DOP",
        "Fecha_Liq",
        "Dias_Venc",
        "Fecha"
    ]


    # Crear una copia del DataFrame
    df = df.dropna(subset=[df.columns[0]])
    df = df.iloc[:-1]
    df = LimpiezaExcel().recortar_df(df, columna=0, palabra_inicial="Número Operación", palabra_final=None, n_inicial=1, n_final=1)
    return df

