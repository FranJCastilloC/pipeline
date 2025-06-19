import pandas as pd
import numpy as np
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any
import logging
import re

class BaseTransformer(ABC):
    """
    Clase base abstracta para todos los transformadores de datos.
    Define la interfaz estándar y funcionalidades comunes.
    """
    
    def __init__(self, sheet_name: str):
        self.sheet_name = sheet_name
        self.logger = self._setup_logger()
        
        # Métricas de transformación
        self.metrics = {
            'input_rows': 0,
            'output_rows': 0,
            'columns_created': 0,
            'data_quality_issues': 0,
            'processing_time': 0
        }
    
    def _setup_logger(self) -> logging.Logger:
        """Configurar logger específico para este transformador"""
        logger = logging.getLogger(f'Transformer.{self.sheet_name}')
        logger.setLevel(logging.INFO)
        return logger
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Método principal de transformación que debe implementar cada transformador.
        
        Args:
            df (pd.DataFrame): DataFrame de entrada con datos crudos
            
        Returns:
            Optional[pd.DataFrame]: DataFrame transformado o None si falló
        """
        pass
    
    @abstractmethod
    def get_target_schema(self) -> Dict[str, str]:
        """
        Define el esquema objetivo (nombres de columnas y tipos de datos).
        
        Returns:
            Dict[str, str]: Diccionario con nombre_columna: tipo_dato
        """
        pass
    
    def validate_input(self, df: pd.DataFrame) -> bool:
        """
        Valida el DataFrame de entrada antes de procesar.
        
        Args:
            df (pd.DataFrame): DataFrame a validar
            
        Returns:
            bool: True si es válido, False si no
        """
        try:
            # Validaciones básicas
            if df is None or df.empty:
                self.logger.error("❌ DataFrame de entrada está vacío o es None")
                return False
            
            if 'fecha' not in df.columns:
                self.logger.error("❌ Columna 'fecha' no encontrada en el DataFrame")
                return False
            
            # Verificar que hay fechas únicas
            unique_dates = df['fecha'].nunique()
            if unique_dates == 0:
                self.logger.error("❌ No se encontraron fechas válidas")
                return False
            
            self.logger.info(f"✅ Validación de entrada exitosa: {len(df)} filas, {unique_dates} fechas únicas")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en validación de entrada: {str(e)}")
            return False
    
    def validate_output(self, df: pd.DataFrame) -> bool:
        """
        Valida el DataFrame de salida después de procesar.
        
        Args:
            df (pd.DataFrame): DataFrame transformado a validar
            
        Returns:
            bool: True si es válido, False si no
        """
        try:
            if df is None or df.empty:
                self.logger.error("❌ DataFrame de salida está vacío o es None")
                return False
            
            # Verificar esquema objetivo
            target_schema = self.get_target_schema()
            missing_columns = set(target_schema.keys()) - set(df.columns)
            
            if missing_columns:
                self.logger.error(f"❌ Columnas faltantes en salida: {missing_columns}")
                return False
            
            # Verificar que hay datos por fecha
            if 'fecha' in df.columns:
                dates_with_data = df.groupby('fecha').size()
                if dates_with_data.min() == 0:
                    self.logger.warning("⚠️ Algunas fechas no tienen datos")
            
            self.logger.info(f"✅ Validación de salida exitosa: {len(df)} filas procesadas")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error en validación de salida: {str(e)}")
            return False
    
    def extract_numeric_value(self, value: Any) -> float:
        """
        Extrae valor numérico de una celda, manejando formatos diversos.
        
        Args:
            value: Valor a convertir
            
        Returns:
            float: Valor numérico o 0.0 si no se puede convertir
        """
        try:
            if pd.isna(value):
                return 0.0
            
            value_str = str(value).strip()
            
            # Casos especiales
            if value_str in ['', '0', 'NaN', 'nan', '-', 'N/A']:
                return 0.0
            
            # Remover comas, espacios y otros caracteres
            value_clean = re.sub(r'[,\s]', '', value_str)
            
            # Manejar paréntesis como números negativos
            if value_clean.startswith('(') and value_clean.endswith(')'):
                value_clean = '-' + value_clean[1:-1]
            
            return float(value_clean)
            
        except (ValueError, TypeError):
            self.metrics['data_quality_issues'] += 1
            return 0.0
    
    def find_section_index(self, df: pd.DataFrame, section_name: str, 
                          column_index: int = 0) -> Optional[int]:
        """
        Busca el índice de una sección específica en el DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame donde buscar
            section_name (str): Nombre de la sección a buscar
            column_index (int): Índice de la columna donde buscar
            
        Returns:
            Optional[int]: Índice de la fila donde se encontró la sección o None
        """
        try:
            for idx in range(len(df)):
                if idx < len(df) and column_index < len(df.columns):
                    cell_value = str(df.iloc[idx, column_index])
                    if section_name.lower() in cell_value.lower():
                        self.logger.debug(f"🔍 '{section_name}' encontrado en índice {idx}")
                        return idx
            
            self.logger.warning(f"⚠️ Sección '{section_name}' no encontrada")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Error buscando sección '{section_name}': {str(e)}")
            return None
    
    def extract_date_range(self, df: pd.DataFrame) -> tuple:
        """
        Extrae el rango de fechas del DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame con columna 'fecha'
            
        Returns:
            tuple: (fecha_min, fecha_max)
        """
        try:
            if 'fecha' not in df.columns:
                return None, None
            
            dates = pd.to_datetime(df['fecha'], format='%d-%m-%Y', errors='coerce')
            valid_dates = dates.dropna()
            
            if len(valid_dates) == 0:
                return None, None
            
            return valid_dates.min().strftime('%Y-%m-%d'), valid_dates.max().strftime('%Y-%m-%d')
            
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo rango de fechas: {str(e)}")
            return None, None
    
    def create_metrics_summary(self) -> Dict:
        """
        Crea un resumen de las métricas de transformación.
        
        Returns:
            Dict: Resumen de métricas
        """
        return {
            'sheet_name': self.sheet_name,
            'metrics': self.metrics.copy(),
            'data_quality_score': max(0, 100 - (self.metrics['data_quality_issues'] * 5)),
            'success': self.metrics['output_rows'] > 0
        }
    
    def run(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Método principal que ejecuta todo el flujo de transformación.
        
        Args:
            df (pd.DataFrame): DataFrame de entrada
            
        Returns:
            Optional[pd.DataFrame]: DataFrame transformado o None si falló
        """
        import time
        start_time = time.time()
        
        try:
            self.logger.info(f"🔄 Iniciando transformación de {self.sheet_name}")
            
            # Registrar métricas de entrada
            self.metrics['input_rows'] = len(df) if df is not None else 0
            
            # Validar entrada
            if not self.validate_input(df):
                return None
            
            # Ejecutar transformación específica
            result_df = self.transform(df)
            
            # Validar salida
            if result_df is not None and self.validate_output(result_df):
                self.metrics['output_rows'] = len(result_df)
                self.metrics['columns_created'] = len(result_df.columns)
                
                self.logger.info(f"✅ Transformación completada: {self.metrics['input_rows']} → {self.metrics['output_rows']} filas")
                return result_df
            else:
                self.logger.error("❌ Transformación falló en validación de salida")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error en transformación: {str(e)}")
            return None
        finally:
            self.metrics['processing_time'] = time.time() - start_time

class SimpleTransformer(BaseTransformer):
    """
    Transformador simple para hojas que no requieren lógica específica.
    Solo agrega estructura básica y limpieza general.
    """
    
    def __init__(self, sheet_name: str):
        super().__init__(sheet_name)
    
    def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Transformación básica: limpieza y estructura estándar"""
        try:
            result_df = df.copy()
            
            # Limpiar nombres de columnas
            result_df.columns = [str(col).strip().replace(' ', '_').lower() 
                               for col in result_df.columns]
            
            # Asegurar que fecha esté presente y bien formateada
            if 'fecha' in result_df.columns:
                # Convertir fecha a formato estándar
                result_df['fecha_procesamiento'] = pd.to_datetime(
                    result_df['fecha'], format='%d-%m-%Y', errors='coerce'
                ).dt.strftime('%Y-%m-%d')
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"❌ Error en transformación simple: {str(e)}")
            return None
    
    def get_target_schema(self) -> Dict[str, str]:
        """Esquema básico para transformador simple"""
        return {
            'fecha': 'str',
            'fecha_procesamiento': 'str'
        } 