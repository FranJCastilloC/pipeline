"""
Scraper para descargar archivos Excel de la Bolsa de Valores de República Dominicana.
Módulo de extracción de datos que descarga y procesa archivos Excel por rango de fechas.
"""

import logging
import time
from datetime import datetime, date
from io import BytesIO
from typing import Dict, List, Optional
import pandas as pd
import requests

# Configurar logging
logging.basicConfig(level=logging.WARNING)  # Cambiar a WARNING para reducir ruido
logger = logging.getLogger(__name__)


class ScraperBase:
    """Clase base que contiene toda la configuración y constantes."""
    
    # URLs y headers
    BASE_URL = "https://boletin.bvrd.com.do/BOLETINES+Y+PRECIOS+{year}/Boletin+Consolidado/{month_number}.+{month_name}/{day_number}-{month_number1}-{year}-Boletin+BVRD+Consolidado+excel.xlsx"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://boletin.bvrd.com.do/"
    }
    
    # Configuración de descarga
    DOWNLOAD_TIMEOUT = 30
    MAX_RETRY_ATTEMPTS = 3
    RETRY_DELAY = 2
    
    # Mapeo de meses
    MESES_ES = {
        "January": "Enero", "February": "Febrero", "March": "Marzo", "April": "Abril",
        "May": "Mayo", "June": "Junio", "July": "Julio", "August": "Agosto",
        "September": "Septiembre", "October": "Octubre", "November": "Noviembre", "December": "Diciembre"
    }


class ScraperUtils:
    """Clase con funciones de utilidad para validación y procesamiento."""
    
    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """
        Valida que una fecha esté en el formato correcto 'YYYY-MM-DD'.
        
        Args:
            date_str (str): Fecha a validar
            
        Returns:
            bool: True si el formato es válido, False en caso contrario
            
        Example:
            >>> validate_date_format('2025-01-03')
            True
            >>> validate_date_format('2025/01/03')
            False
        """
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> bool:
        """
        Valida que el rango de fechas sea válido.
        
        Args:
            start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
            end_date (str): Fecha de fin en formato 'YYYY-MM-DD'
            
        Returns:
            bool: True si el rango es válido, False en caso contrario
            
        Raises:
            ValueError: Si las fechas no están en formato correcto o el rango es inválido
        """
        if not ScraperUtils.validate_date_format(start_date) or not ScraperUtils.validate_date_format(end_date):
            raise ValueError("Las fechas deben estar en formato 'YYYY-MM-DD'")
        
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start > end:
            raise ValueError("La fecha de inicio no puede ser mayor que la fecha de fin")
        
        days_diff = (end - start).days
        if days_diff > 365:
            raise ValueError("El rango máximo permitido es de 365 días")
        
        return True
    
    @staticmethod
    def create_date_range(start_date: str, end_date: str) -> List[str]:
        """
        Genera una lista de fechas entre start_date y end_date.
        
        Args:
            start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
            end_date (str): Fecha de fin en formato 'YYYY-MM-DD'
        
        Returns:
            List[str]: Lista de fechas en formato 'DD-MM-YYYY'
        
        Example:
            >>> create_date_range('2025-01-01', '2025-01-03')
            ['01-01-2025', '02-01-2025', '03-01-2025']
        """
        date_range = pd.date_range(start=start_date, end=end_date)
        return [date.strftime('%d-%m-%Y') for date in date_range]
    
    @staticmethod
    def build_file_url(date_str: str) -> str:
        """
        Construye la URL del archivo Excel para una fecha específica.
        Con esta funcion podremos crear tantas urls como fechas tengamos en el rango de fechas.
        
        Args:
            date_str (str): Fecha en formato 'DD-MM-YYYY'
            
        Returns:
            str: URL completa del archivo Excel
            
        Example:
            >>> build_file_url('03-01-2025')
            'https://boletin.bvrd.com.do/BOLETINES+Y+PRECIOS+2025/Boletin+Consolidado/1.+Enero/03-01-2025-Boletin+BVRD+Consolidado+excel.xlsx'
        """
        date_obj = datetime.strptime(date_str, '%d-%m-%Y')
        year = date_obj.year # Año del archivo
        month_name = ScraperBase.MESES_ES[date_obj.strftime('%B')] # Nombre del mes en español
        month_number = date_obj.month # Mes sin ceros a la izquierda
        day_number = f"{date_obj.day:02d}"  # Día con ceros a la izquierda
        month_number_padded = f"{date_obj.month:02d}" # Mes con ceros a la izquierda
        
        return ScraperBase.BASE_URL.format(
            year=year,
            month_number=month_number,
            month_name=month_name,
            day_number=day_number,
            month_number1=month_number_padded
        )
    
    @staticmethod
    def download_excel_file(url: str) -> Optional[BytesIO]:
        """
        Descarga un archivo Excel desde la URL proporcionada.
        
        Args:
            url (str): URL del archivo Excel a descargar
            
        Returns:
            Optional[BytesIO]: Contenido del archivo o None si falla la descarga
            
        Example:
            >>> file_content = download_excel_file('https://example.com/file.xlsx')
            >>> if file_content:
            ...     print("Archivo descargado exitosamente")
        """
        try:
            logger.info(f"Descargando archivo: {url}")
            response = requests.get(url, headers=ScraperBase.HEADERS, timeout=ScraperBase.DOWNLOAD_TIMEOUT)
            response.raise_for_status()
            
            # Verificar que sea un archivo Excel
            content_type = response.headers.get("Content-Type", "")
            if "html" in content_type:
                logger.warning(f"Servidor devolvió HTML en lugar de Excel: {url}")
                return None
            
            logger.info(f"Archivo descargado exitosamente: {len(response.content)} bytes")
            return BytesIO(response.content)
            
        except requests.exceptions.ConnectionError:
            logger.error(f"Error de conexión al descargar: {url}")
            return None
        except requests.exceptions.Timeout:
            logger.error(f"Timeout al descargar: {url}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code}: {url}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado al descargar {url}: {e}")
            return None
    
    @staticmethod
    def extract_sheets_from_excel(file: BytesIO, date: str, sheets_to_extract: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Extrae hojas específicas de un archivo Excel y las convierte en DataFrames.
        
        Args:
            file (BytesIO): Archivo Excel en memoria
            date (str): Fecha del archivo en formato 'DD-MM-YYYY'
            sheets_to_extract (Optional[List[str]]): Lista de nombres de hojas a extraer. 
                                                   Si es None, extrae todas las hojas.
            
        Returns:
            Dict[str, pd.DataFrame]: Diccionario con DataFrames de cada hoja
            
        Example:
            >>> with open('file.xlsx', 'rb') as f:
            ...     file_content = BytesIO(f.read())
            ...     sheets = extract_sheets_from_excel(file_content, '03-01-2025', ['BB_ResumenGeneralMercado'])
            >>> print(f"Se extrajeron {len(sheets)} hojas")
        """
        sheets_data = {}
        
        try:
            excel_data = pd.ExcelFile(file)
            available_sheets = excel_data.sheet_names
            
            # Si no se especifica lista, extraer todas las hojas
            if sheets_to_extract is None:
                sheets_to_process = available_sheets
                sheet_mapping = {sheet: sheet for sheet in available_sheets}
            else:
                # Crear mapeo entre nombres solicitados y nombres reales de hojas
                sheet_mapping = {}
                sheets_to_process = []
                
                for requested_sheet in sheets_to_extract:
                    # Buscar la hoja exacta primero
                    if requested_sheet in available_sheets:
                        sheets_to_process.append(requested_sheet)
                        sheet_mapping[requested_sheet] = requested_sheet
                        logger.info(f"Encontrada hoja exacta: '{requested_sheet}'")
                    else:
                        # Si no se encuentra, intentar sin prefijo BB_
                        actual_sheet_name = requested_sheet.replace("BB_", "")
                        if actual_sheet_name in available_sheets:
                            sheets_to_process.append(actual_sheet_name)
                            sheet_mapping[actual_sheet_name] = requested_sheet
                            logger.info(f"Mapeando '{actual_sheet_name}' -> '{requested_sheet}'")
                        else:
                            logger.warning(f"Hoja '{requested_sheet}' no encontrada en el archivo")
                
                logger.info(f"Filtrando hojas: {sheets_to_process} de {available_sheets}")
            
            logger.info(f"Procesando archivo con {len(sheets_to_process)} hojas de {len(available_sheets)} disponibles")
            
            for sheet_name in sheets_to_process:
                try:
                    df = pd.read_excel(file, sheet_name=sheet_name)
                    if not df.empty:
                        # Agregar columna de fecha
                        df['Fecha'] = date
                        # Usar el nombre mapeado
                        output_key = sheet_mapping[sheet_name]
                        sheets_data[f"{output_key}_{date}"] = df
                        logger.info(f"Hoja '{sheet_name}' procesada: {df.shape}")
                    else:
                        logger.warning(f"Hoja '{sheet_name}' está vacía")
                except Exception as e:
                    logger.error(f"Error procesando hoja '{sheet_name}': {e}")
                    continue
            
            logger.info(f"Archivo procesado exitosamente: {len(sheets_data)} hojas extraídas")
            return sheets_data
            
        except Exception as e:
            logger.error(f"Error procesando archivo Excel para {date}: {e}")
            return {}


class BVRDScraper:
    """Clase principal que ejecuta el scraping."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def scrape_single_date(self, date_str: str, sheets_to_extract: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Descarga y procesa un archivo Excel para una fecha específica.
        
        Args:
            date_str (str): Fecha en formato 'DD-MM-YYYY'
            sheets_to_extract (Optional[List[str]]): Lista de nombres de hojas a extraer.
                                                   Si es None, extrae todas las hojas.
            
        Returns:
            Dict[str, pd.DataFrame]: DataFrames extraídos o diccionario vacío si falla
            
        Example:
            >>> scraper = BVRDScraper()
            >>> data = scraper.scrape_single_date('03-01-2025', ['ResumenGeneralMercado'])
            >>> if data:
            ...     print(f"Se extrajeron {len(data)} datasets")
        """
        url = ScraperUtils.build_file_url(date_str)
        file_content = ScraperUtils.download_excel_file(url)
        
        if file_content is None:
            self.logger.warning(f"No se pudo descargar archivo para {date_str}")
            return {}
        
        return ScraperUtils.extract_sheets_from_excel(file_content, date_str, sheets_to_extract)
    
    def scrape_date_range(self, start_date: str, end_date: str, sheets_to_extract: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Descarga y procesa archivos Excel para un rango de fechas.
        
        Args:
            start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
            end_date (str): Fecha de fin en formato 'YYYY-MM-DD'
            sheets_to_extract (Optional[List[str]]): Lista de nombres de hojas a extraer.
                                                   Si es None, extrae todas las hojas.
            
        Returns:
            Dict[str, pd.DataFrame]: Todos los DataFrames extraídos del rango
            
        Raises:
            ValueError: Si el rango de fechas es inválido
            
        Example:
            >>> scraper = BVRDScraper()
            >>> all_data = scraper.scrape_date_range('2025-01-01', '2025-01-03', ['ResumenGeneralMercado'])
            >>> print(f"Total de datasets extraídos: {len(all_data)}")
        """
        # Validar rango de fechas
        ScraperUtils.validate_date_range(start_date, end_date)
        
        self.logger.info(f"Iniciando scraping para rango: {start_date} → {end_date}")
        
        all_data = {}
        date_range = ScraperUtils.create_date_range(start_date, end_date)
        
        for date_str in date_range:
            self.logger.info(f"Procesando fecha: {date_str}")
            date_data = self.scrape_single_date(date_str, sheets_to_extract)
            all_data.update(date_data)
            
            # Pequeña pausa para no sobrecargar el servidor
            time.sleep(0.5)
        
        self.logger.info(f"Scraping completado: {len(all_data)} datasets extraídos")
        return all_data


# ===============================================
# FUNCIÓN DE COMPATIBILIDAD (para mantener interfaz existente)
# ===============================================

def main(start_date: str, end_date: str, sheets_to_extract: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
    """
    Función principal para mantener compatibilidad con código existente.
    
    Args:
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'
        sheets_to_extract (Optional[List[str]]): Lista de nombres de hojas a extraer.
                                               Si es None, extrae todas las hojas.
        
    Returns:
        Dict[str, pd.DataFrame]: Todos los DataFrames extraídos
        
    Example:
        >>> data = main('2025-01-01', '2025-01-03', ['ResumenGeneralMercado'])
        >>> for key, df in data.items():
        ...     print(f"{key}: {df.shape}")
    """
    scraper = BVRDScraper()
    return scraper.scrape_date_range(start_date, end_date, sheets_to_extract)


# ===============================================
# CÓDIGO DE EJECUCIÓN
# ===============================================

if __name__ == "__main__":
    # Configurar logging para ejecución directa
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Ejemplo de uso
    start_date = "2025-01-03"
    end_date = date.today().strftime('%Y-%m-%d')
    
    try:
        logger.info("Iniciando scraping de BVRD")
        data = main(start_date, end_date)
        
        # Mostrar resumen de resultados
        logger.info("=" * 50)
        logger.info("RESUMEN DE EXTRACCIÓN")
        logger.info("=" * 50)
        
        for key, df in data.items():
            logger.info(f"{key}: {df.shape}")
        
        logger.info(f"Total de datasets extraídos: {len(data)}")
        
    except ValueError as e:
        logger.error(f"Error de validación: {e}")
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        import traceback
        traceback.print_exc()


