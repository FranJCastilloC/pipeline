"""
BANCO DE FUNCIONES DE LIMPIEZA DE DATOS
=======================================

Este módulo contiene funciones útiles para la limpieza y procesamiento de DataFrames.
Cada función está documentada con su propósito, parámetros y ejemplos de uso.

Autor: Francisco Castillo
Fecha de creación: 19/06/2025
Última actualización: 19/06/2025
"""

import pandas as pd
import numpy as np
from typing import Union, List, Optional


# =============================================================================
# FUNCIÓN 1: RECORTE DE DATAFRAMES
# =============================================================================

class LimpiezaExcel:
    """
    Clase para realizar operaciones de limpieza y procesamiento de DataFrames de Excel.
    
    Esta clase contiene métodos útiles para la limpieza y procesamiento de datos
    extraídos de archivos Excel, incluyendo recorte de DataFrames, limpieza de columnas,
    y otras operaciones comunes de limpieza de datos.
    """
    
    def __init__(self):
        """
        Inicializa la clase LimpiezaExcel.
        """
        pass
    def recortar_df(self, df: pd.DataFrame, columna: Union[int, str] = 0, palabra_inicial: str = None, palabra_final: str = None, n_inicial: int = 1, n_final: int = 1) -> pd.DataFrame:
        """
        Recorta el DataFrame excluyendo las filas que contienen palabra_inicial y palabra_final (coincidencia parcial).
        Permite elegir la n-ésima coincidencia de cada palabra.
        
        Args:
            df: DataFrame a procesar.
            columna: Índice o nombre de la columna donde buscar las palabras (por defecto 0).
            palabra_inicial: Palabra que marca el inicio del recorte (requerido).
            palabra_final: Palabra que marca el fin del recorte (opcional).
            n_inicial: Número de coincidencia de palabra_inicial a usar (por defecto 1, es decir, la primera).
            n_final: Número de coincidencia de palabra_final a usar (por defecto 1, es decir, la primera).
        
        Returns:
            DataFrame recortado desde después de palabra_inicial hasta antes de palabra_final.
            
        Raises:
            ValueError: Si no se encuentra la coincidencia requerida para palabra_inicial o palabra_final, o si la columna no existe.
        """
        if palabra_inicial is None:
            raise ValueError("Debe proporcionar una palabra_inicial para el recorte")

        # Obtener columna objetivo
        if isinstance(columna, str):
            if columna not in df.columns:
                raise ValueError(f"La columna '{columna}' no existe en el DataFrame")
            columna_data = df[columna]
        else:
            if columna >= len(df.columns):
                raise ValueError(f"El índice de columna {columna} está fuera de rango")
            columna_data = df.iloc[:, columna]

        columna_str = columna_data.astype(str).str.lower().str.strip()

        # Buscar la n-ésima coincidencia de palabra_inicial
        idxs_inicio = columna_str[columna_str.str.contains(palabra_inicial.lower(), na=False)].index
        if len(idxs_inicio) < n_inicial:
            raise ValueError(f"No se encontró la {n_inicial}-ésima coincidencia para '{palabra_inicial}'")
        idx_inicio = idxs_inicio[n_inicial - 1]

        if palabra_final:
            idxs_final = columna_str[columna_str.str.contains(palabra_final.lower(), na=False)].index
            if len(idxs_final) < n_final:
                raise ValueError(f"No se encontró la {n_final}-ésima coincidencia para '{palabra_final}'")
            idx_final = idxs_final[n_final - 1]
            return df.iloc[idx_inicio + 1: idx_final].reset_index(drop=True)
        else:
            return df.iloc[idx_inicio + 1:].reset_index(drop=True)
        
    def seleccionar_columnas(self, df: pd.DataFrame, columnas: List[int]) -> pd.DataFrame:
        """
        Selecciona solo las columnas necesarias: Fecha y las columnas 1, 4, 5, 6, 7
        """
        # Seleccionar columnas específicas por posición
        df = df.iloc[:, columnas].reset_index(drop=True)  # Fecha está en la columna 0
        return df


