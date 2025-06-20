from extraction.scraper import main
import datetime
import sys
import os

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Importar transformadores y funciones de inserción
from transformers.Sheet_transformers.BB_ResumenGeneralMercado import transform_resumen_general_mercado
from transformers.Sheet_transformers.BB_RFVTransPuestoBolsaMP import transform_rfv_trans_puesto_bolsa_mp
from loading.BB_ResumenGeneralMercado_import import insert_data as insert_resumen_general
from loading.BB_RFVTransPuestoBolsaMP_import import insert_data as insert_rfv_trans_puesto

def get_dataset(start_date, end_date, sheet_name):
    """
    Descarga y retorna solo el DataFrame de la hoja solicitada.
    """
    data = main(start_date, end_date, [sheet_name])
    
    for key, df in data.items():
        if sheet_name in key:
            return df
    return None

def data_manager(start_date, end_date):
    """
    Gestiona la extracción, transformación e inserción de datos de los boletines.
    """
    data_sets = {
        "BB_ResumenGeneralMercado",
        "BB_RFVTransPuestoBolsaMP",
        "BB_RFMPOperDia",
        "BB_RFMPOperDiaFirme",
        "BB_RFMSOperDia",
        "BB_RFMSOperPlazos",
        "BB_RentaFijaOperacionesFuturasA",
        "BB_RFEmisionesCorpV"
    }
    
    # Diccionario de transformadores
    transformers = {
        "BB_ResumenGeneralMercado": transform_resumen_general_mercado,
        "BB_RFVTransPuestoBolsaMP": transform_rfv_trans_puesto_bolsa_mp,
        # Agregar más transformadores aquí cuando estén disponibles
    }
    
    # Diccionario de funciones de inserción
    inserters = {
        "BB_ResumenGeneralMercado": insert_resumen_general,
        "BB_RFVTransPuestoBolsaMP": insert_rfv_trans_puesto,
        # Agregar más funciones de inserción aquí cuando estén disponibles
    }
    
    sheets_to_extract = list(data_sets)
    datos = main(start_date, end_date, sheets_to_extract)
    
    for sheet_name, df in datos.items():
        # Obtener el nombre base de la hoja (sin fecha)
        base_name = next((name for name in data_sets if name in sheet_name), sheet_name)
        
        # Si existe un transformador para esta hoja, aplicarlo
        if base_name in transformers:
            try:
                df_transformed = transformers[base_name](df)
                print(f"\nTransformación exitosa para {base_name}")
                
                # Intentar insertar los datos transformados
                if inserters[base_name](df_transformed):
                    print(f"Datos insertados exitosamente para {base_name}")
                else:
                    print(f"Error al insertar datos para {base_name}")
                    
            except Exception as e:
                print(f"Error al procesar {base_name}: {e}")

if __name__ == "__main__":
    start_date = "2025-03-18"
    end_date = "2025-03-18"
    data_manager(start_date, end_date) 