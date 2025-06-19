import pandas as pd
import numpy as np
import sys
import os

# Agregar el directorio padre al path para importar BaseTransformer
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformers.base_transformer import BaseTransformer

class ResumenGeneralMercadoTransformer(BaseTransformer):
    """
    Transformador específico para la hoja ResumenGeneralMercado.
    Extrae métricas diarias de operaciones de los mercados bursátiles.
    """
    
    def __init__(self):
        super().__init__("ResumenGeneralMercado")
        
        # Configuración específica del transformador
        self.target_columns = {
            # Mercado de Renta Fija - Secundario
            'mdo_secundario_rf_usd': 0,
            'mdo_secundario_rf_usd_equiv_dop': 0,
            'mdo_secundario_rf_dop': 0,
            'mdo_secundario_rf_total_dop': 0,
            # Mercado de Renta Fija - Primario
            'mdo_primario_rf_usd': 0,
            'mdo_primario_rf_usd_equiv_dop': 0,
            'mdo_primario_rf_dop': 0,
            'mdo_primario_rf_total_dop': 0,
            # Mercado de Renta Variable - Secundario
            'mdo_secundario_rv_usd': 0,
            'mdo_secundario_rv_usd_equiv_dop': 0,
            'mdo_secundario_rv_dop': 0,
            'mdo_secundario_rv_total_dop': 0,
            # Mercado de Renta Variable - Primario
            'mdo_primario_rv_usd': 0,
            'mdo_primario_rv_usd_equiv_dop': 0,
            'mdo_primario_rv_dop': 0,
            'mdo_primario_rv_total_dop': 0,
        }
    
    def get_target_schema(self) -> dict:
        """Define el esquema objetivo para ResumenGeneralMercado"""
        schema = {'fecha': 'str'}
        schema.update({col: 'float' for col in self.target_columns.keys()})
        return schema
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformación principal para ResumenGeneralMercado.
        Extrae métricas diarias de "Operaciones del Día" para todas las fechas.
        """
        try:
            self.logger.info(f"🔄 Iniciando transformación de ResumenGeneralMercado")
            
            if 'fecha' not in df.columns:
                self.logger.error("❌ Columna 'fecha' no encontrada")
                return None
            
            fechas_unicas = df['fecha'].unique()
            self.logger.info(f"📅 Fechas encontradas: {len(fechas_unicas)} días")
            
            todas_las_metricas = []
            
            for fecha in fechas_unicas:
                self.logger.info(f"🗓️ Procesando fecha: {fecha}")
                metricas_fecha = self._process_single_date(df, fecha)
                
                if metricas_fecha:
                    todas_las_metricas.append(metricas_fecha)
                    self.logger.info(f"✅ {fecha} procesado exitosamente")
                else:
                    self.logger.warning(f"⚠️ {fecha} sin datos válidos")
            
            if todas_las_metricas:
                resultado_df = pd.DataFrame(todas_las_metricas)
                resultado_df = resultado_df.sort_values('fecha').reset_index(drop=True)
                
                self.logger.info(f"✅ Transformación completada: {len(resultado_df)} fechas procesadas")
                return resultado_df
            else:
                self.logger.error("❌ No se procesaron datos para ninguna fecha")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error en transformación: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _process_single_date(self, df: pd.DataFrame, fecha: str) -> dict:
        """
        Procesa los datos de una fecha específica.
        
        Args:
            df (pd.DataFrame): DataFrame completo
            fecha (str): Fecha a procesar
            
        Returns:
            dict: Métricas extraídas para la fecha o None si falló
        """
        try:
            # Filtrar datos para esta fecha específica y resetear índices
            df_fecha = df[df['fecha'] == fecha].copy().reset_index(drop=True)
            
            if len(df_fecha) == 0:
                self.logger.warning(f"   ❌ No hay datos para {fecha}")
                return None
            
            # Inicializar métricas para esta fecha
            metricas_fecha = {'fecha': fecha}
            metricas_fecha.update(self.target_columns.copy())
            
            # Buscar sección "Operaciones del Día"
            operaciones_dia_idx = self._find_operaciones_dia_section(df_fecha)
            
            if operaciones_dia_idx is not None:
                # Extraer métricas de mercados
                mercados_extraidos = self._extract_market_metrics(df_fecha, operaciones_dia_idx)
                metricas_fecha.update(mercados_extraidos)
                
                return metricas_fecha
            else:
                self.logger.warning(f"   ❌ No se encontró 'Operaciones del Día' para {fecha}")
                return metricas_fecha  # Retornar con valores en 0
                
        except Exception as e:
            self.logger.error(f"   ❌ Error procesando {fecha}: {str(e)}")
            return None
    
    def _find_operaciones_dia_section(self, df_fecha: pd.DataFrame) -> int:
        """
        Busca la sección "Operaciones del Día" en el DataFrame de una fecha.
        
        Args:
            df_fecha (pd.DataFrame): DataFrame filtrado por fecha
            
        Returns:
            int: Índice donde se encontró la sección o None
        """
        for idx in range(len(df_fecha)):
            row = df_fecha.iloc[idx]
            if any('Operaciones del Día' in str(cell) for cell in row if pd.notna(cell)):
                self.logger.debug(f"   ✅ 'Operaciones del Día' encontrado en índice {idx}")
                return idx
        
        return None
    
    def _extract_market_metrics(self, df_fecha: pd.DataFrame, start_idx: int) -> dict:
        """
        Extrae las métricas de mercados desde la sección "Operaciones del Día".
        
        Args:
            df_fecha (pd.DataFrame): DataFrame de la fecha
            start_idx (int): Índice donde inicia la búsqueda
            
        Returns:
            dict: Métricas extraídas por mercado
        """
        metricas = {}
        
        # Buscar en las siguientes 20 filas después de "Operaciones del Día"
        search_range = range(start_idx + 1, min(start_idx + 21, len(df_fecha)))
        
        for idx in search_range:
            if idx >= len(df_fecha):
                break
                
            row = df_fecha.iloc[idx]
            nombre_mercado = str(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else ""
            
            # Extraer valores numéricos de las columnas
            valores = self._extract_row_values(row)
            
            # Identificar tipo de mercado y asignar valores
            market_type = self._identify_market_type(nombre_mercado)
            if market_type:
                self._assign_market_values(metricas, market_type, valores)
                self.logger.debug(f"   📊 {market_type} - USD: {valores['usd']:,.2f}, DOP: {valores['dop']:,.2f}")
        
        return metricas
    
    def _extract_row_values(self, row: pd.Series) -> dict:
        """
        Extrae valores numéricos de una fila.
        
        Args:
            row (pd.Series): Fila del DataFrame
            
        Returns:
            dict: Valores extraídos por tipo
        """
        return {
            'usd': self.extract_numeric_value(row.iloc[3] if len(row) > 3 else 0),
            'usd_equiv_dop': self.extract_numeric_value(row.iloc[5] if len(row) > 5 else 0),
            'dop': self.extract_numeric_value(row.iloc[6] if len(row) > 6 else 0),
            'total_dop': self.extract_numeric_value(row.iloc[7] if len(row) > 7 else 0)
        }
    
    def _identify_market_type(self, nombre_mercado: str) -> str:
        """
        Identifica el tipo de mercado basado en el nombre.
        
        Args:
            nombre_mercado (str): Nombre del mercado
            
        Returns:
            str: Tipo de mercado identificado o None
        """
        patterns = {
            'secundario_rf': ['Mdo. Secundario RF', 'Secundario RF', 'MDO SECUNDARIO RF'],
            'primario_rf': ['Mdo. Primario RF', 'Primario RF', 'MDO PRIMARIO RF'],
            'secundario_rv': ['Mdo. Secundario RV', 'Secundario RV', 'MDO SECUNDARIO RV'],
            'primario_rv': ['Mdo. Primario RV', 'Primario RV', 'MDO PRIMARIO RV']
        }
        
        for market_type, patterns_list in patterns.items():
            if any(pattern in nombre_mercado for pattern in patterns_list):
                return market_type
        
        return None
    
    def _assign_market_values(self, metricas: dict, market_type: str, valores: dict):
        """
        Asigna los valores extraídos a las métricas del mercado correspondiente.
        
        Args:
            metricas (dict): Diccionario de métricas a actualizar
            market_type (str): Tipo de mercado
            valores (dict): Valores extraídos
        """
        prefix = f"mdo_{market_type}"
        
        metricas[f"{prefix}_usd"] = valores['usd']
        metricas[f"{prefix}_usd_equiv_dop"] = valores['usd_equiv_dop']
        metricas[f"{prefix}_dop"] = valores['dop']
        metricas[f"{prefix}_total_dop"] = valores['total_dop']

# Función de compatibilidad con el sistema existente
def clean_and_build_resumen_general_mercado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función de compatibilidad para mantener la interfaz existente.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada
        
    Returns:
        pd.DataFrame: DataFrame transformado
    """
    transformer = ResumenGeneralMercadoTransformer()
    return transformer.run(df)
