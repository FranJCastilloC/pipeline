import pandas as pd
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import numpy as np

class BaseTransformer(ABC):
    """
    Clase base abstracta para todos los transformadores de hojas.
    Define la interfaz común y proporciona utilidades compartidas.
    """
    
    def __init__(self):
        self.required_columns = []
        self.output_columns = []
        self.date_columns = []
        self.numeric_columns = []
        
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Método principal de transformación que debe ser implementado por cada transformador específico.
        
        Args:
            df (pd.DataFrame): DataFrame original a transformar
            
        Returns:
            pd.DataFrame: DataFrame transformado
        """
        pass
    
    def validate_input(self, df: pd.DataFrame) -> bool:
        """
        Valida que el DataFrame de entrada tenga las columnas requeridas.
        
        Args:
            df (pd.DataFrame): DataFrame a validar
            
        Returns:
            bool: True si el DataFrame es válido, False en caso contrario
        """
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Faltan las siguientes columnas requeridas: {missing_columns}")
            return False
        return True
    
    def clean_numeric_columns(self, df: pd.DataFrame, columns: Optional[list] = None) -> pd.DataFrame:
        """
        Limpia y convierte columnas numéricas, manejando diferentes formatos.
        
        Args:
            df (pd.DataFrame): DataFrame a limpiar
            columns (list, optional): Lista de columnas a limpiar. Si es None, usa self.numeric_columns
            
        Returns:
            pd.DataFrame: DataFrame con columnas numéricas limpias
        """
        columns = columns or self.numeric_columns
        df_copy = df.copy()
        
        for col in columns:
            if col in df_copy.columns:
                # Convertir a string primero para manejar diferentes tipos de datos
                df_copy[col] = df_copy[col].astype(str)
                
                # Limpiar caracteres no numéricos excepto punto y coma
                df_copy[col] = df_copy[col].str.replace('[^0-9\.\-\,]', '', regex=True)
                
                # Reemplazar coma por punto si es separador decimal
                df_copy[col] = df_copy[col].str.replace(',', '.')
                
                # Convertir a float
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                
        return df_copy
    
    def clean_date_columns(self, df: pd.DataFrame, columns: Optional[list] = None, 
                         format: str = '%d/%m/%Y') -> pd.DataFrame:
        """
        Limpia y convierte columnas de fecha al formato especificado.
        
        Args:
            df (pd.DataFrame): DataFrame a limpiar
            columns (list, optional): Lista de columnas a limpiar. Si es None, usa self.date_columns
            format (str): Formato esperado de las fechas
            
        Returns:
            pd.DataFrame: DataFrame con columnas de fecha limpias
        """
        columns = columns or self.date_columns
        df_copy = df.copy()
        
        for col in columns:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col], format=format, errors='coerce')
                
        return df_copy
    
    def drop_empty_rows(self, df: pd.DataFrame, subset: Optional[list] = None) -> pd.DataFrame:
        """
        Elimina filas que están completamente vacías en las columnas especificadas.
        
        Args:
            df (pd.DataFrame): DataFrame a limpiar
            subset (list, optional): Lista de columnas a considerar
            
        Returns:
            pd.DataFrame: DataFrame sin filas vacías
        """
        return df.dropna(subset=subset or self.required_columns, how='all')
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Estandariza los nombres de las columnas: lowercase, sin espacios ni caracteres especiales.
        
        Args:
            df (pd.DataFrame): DataFrame a estandarizar
            
        Returns:
            pd.DataFrame: DataFrame con nombres de columnas estandarizados
        """
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.lower()
        df_copy.columns = df_copy.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
        df_copy.columns = df_copy.columns.str.replace('[^0-9a-zA-Z]+', '_', regex=True)
        df_copy.columns = df_copy.columns.str.strip('_')
        return df_copy
    
    def add_metadata(self, df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
        """
        Agrega columnas de metadata al DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame original
            metadata (dict): Diccionario con metadata a agregar
            
        Returns:
            pd.DataFrame: DataFrame con metadata agregada
        """
        df_copy = df.copy()
        for key, value in metadata.items():
            df_copy[key] = value
        return df_copy 