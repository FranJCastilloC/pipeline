"""
Transformador para la hoja ResumenGeneralmercado.
Desarrollo y limpieza de datos.
#Esta pendiente el cambio de las funciones de limpieza

"""

import pandas as pd
import sys
import os

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def transform_resumen_general_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de ResumenGeneralmercado.
    
    Args:
        df (pd.DataFrame): DataFrame original de ResumenGeneralmercado
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    def recortar_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Recorta del dataframe las filas que estén en el rango definido.
        """
        # Buscar "Operaciones del Día"
        inicio_mask = df.iloc[:, 0].str.contains("Operaciones del Día", na=False)
        if not inicio_mask.any():
            return df
        
        inicio_idx = inicio_mask.idxmax()
        
        # Buscar "Acumulado de la Semana"
        fin_mask = df.iloc[:, 0].str.contains("Acumulado de la Semana", na=False)
        if not fin_mask.any():
            # Si no encuentra el fin, usar todo el DataFrame desde el inicio
            return df.iloc[inicio_idx+1:].reset_index(drop=True)
        
        fin_idx = fin_mask.idxmax()
        return df.iloc[inicio_idx+2:fin_idx].reset_index(drop=True)
    

    def eliminar_filas_innecesarias(df: pd.DataFrame) -> pd.DataFrame:
        """
        Elimina filas que contengan nombres específicos en la columna 0.
        """
        # Lista de nombres a eliminar
        nombres_a_eliminar = [
            "Mercado de Renta Fija",
            "Mercado de Renta Variable", 
            "Total Día",
        ]
        # Crear máscara para identificar filas a eliminar
        filas_a_eliminar = (df.iloc[:, 0].str.contains('|'.join(nombres_a_eliminar), na=False))
 
  
        
        # Eliminar las filas que contengan esos nombres
        df_limpio = df[~filas_a_eliminar].reset_index(drop=True)
        
        return df_limpio



    def seleccionar_columnas(df: pd.DataFrame) -> pd.DataFrame:
        """
        Selecciona solo las columnas necesarias: Fecha y las columnas 1, 4, 5, 6, 7
        """
        # Seleccionar columnas específicas por posición
        columnas_seleccionadas = df.iloc[:, [2, 3, 5, 6, 7, 8]]  # Fecha está en la columna 0
        return columnas_seleccionadas

    def definir_columnas(df: pd.DataFrame) -> pd.DataFrame:
        """
        Define los nombres de las columnas según el orden especificado.
        """
        df.columns = [
            "mercado", 
            'transado_usd',
            'usd_equivalente_dop',
            'transado_dop',
            'total_transado_dop',
            'fecha'
        ]
        
        # Limpiar el campo mercado eliminando espacios en blanco y caracteres especiales
        df['mercado'] = df['mercado'].str.strip().str.replace('\n', ' ').str.replace('\r', ' ')
        
        # Eliminar caracteres especiales y normalizar espacios múltiples
        df['mercado'] = df['mercado'].str.replace(r'\s+', ' ', regex=True)
        
        # Convertir a mayúsculas para consistencia
        df['mercado'] = df['mercado'].str.upper()
        # Eliminar filas donde la columna "mercado" tenga valores nulos
        df = df.dropna(subset=["mercado"]).reset_index(drop=True)
        
        return df
    
    # Aplicar las transformaciones en orden correcto
    df = recortar_df(df)
    df = eliminar_filas_innecesarias(df)
    df = seleccionar_columnas(df)
    df = definir_columnas(df)
    return df







