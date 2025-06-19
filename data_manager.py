from extraction.scraper import main
import datetime
import sys
import os
import pandas as pd
from transformers.Sheet_transformers.BB_ResumenGeneralMercado import transform_resumen_general_mercado
from BB_ResumenGeneralMercado_import import insert_data

# Agregar el directorio raíz del proyecto al path para importaciones
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

def get_dataset(start_date, end_date, sheet_name):
    """
    Descarga y retorna solo el DataFrame de la hoja solicitada.
    Esta funcion solo la utilizamos en las pruebas separadas de cada transformador.
    
    Args:
        start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'
        end_date (str): Fecha de fin en formato 'YYYY-MM-DD'
        sheet_name (str): Nombre de la hoja a extraer (ej: 'BB_ResumenGeneralMercado')
        
    Returns:
        pd.DataFrame: DataFrame de la hoja solicitada o None si no se encuentra
    """
    data = main(start_date, end_date, [sheet_name])
    
    # Buscar la clave que contenga el nombre de la hoja (con fecha)
    for key, df in data.items():
        if sheet_name in key:
            return df
    return None

def data_manager(start_date, end_date):
    """
    Gestiona la extracción de datos de los boletines.
    Retorna todos los datasets configurados.
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

    sheets_to_extract = list(data_sets)
    return main(start_date, end_date, sheets_to_extract)

def apply_transformers_and_consolidate(datos):
    """
    Aplica transformadores a cada DataFrame y los consolida en un solo DataFrame.
    
    Args:
        datos (dict): Diccionario con DataFrames extraídos
        
    Returns:
        dict: Diccionario con DataFrames consolidados por tipo de hoja
    """
    data_transformers = {
        "BB_ResumenGeneralMercado": transform_resumen_general_mercado
    }
    
    consolidated_data = {}
    
    # Agrupar DataFrames por tipo de hoja (sin fecha)
    for key, df in datos.items():
        # Extraer el nombre base de la hoja (sin fecha)
        base_sheet_name = None
        for sheet_name in data_transformers.keys():
            if sheet_name in key:
                base_sheet_name = sheet_name
                break
        
        if base_sheet_name and base_sheet_name in data_transformers:
            try:
                # Aplicar transformador
                transformed_df = data_transformers[base_sheet_name](df)
                
                # Agregar al DataFrame consolidado
                if base_sheet_name not in consolidated_data:
                    consolidated_data[base_sheet_name] = []
                
                consolidated_data[base_sheet_name].append(transformed_df)
                print(f"Transformador aplicado exitosamente a {key}")
                
            except Exception as e:
                print(f"Error al aplicar transformador a {key}: {e}")
    
    # Concatenar todos los DataFrames de cada tipo de hoja
    final_consolidated = {}
    for sheet_name, df_list in consolidated_data.items():
        if df_list:
            final_consolidated[sheet_name] = pd.concat(df_list, ignore_index=True)
            print(f"Consolidado {sheet_name}: {final_consolidated[sheet_name].shape}")
    
    return final_consolidated

def insert_consolidated_data(consolidated_datos):
    """
    Inserta los datos consolidados en la base de datos.
    
    Args:
        consolidated_datos (dict): Diccionario con DataFrames consolidados
    """
    for sheet_name, df in consolidated_datos.items():
        if sheet_name == "BB_ResumenGeneralMercado":
            try:
                if insert_data(df):
                    print(f"\nDatos de {sheet_name} insertados exitosamente en la base de datos")
                else:
                    print(f"\nError al insertar datos de {sheet_name} en la base de datos")
            except Exception as e:
                print(f"\nError durante la inserción de {sheet_name}: {e}")

if __name__ == "__main__":
    # Extraer datos
    datos = data_manager("2025-06-17", "2025-06-18")
    
    # Transformar y consolidar datos
    consolidated_datos = apply_transformers_and_consolidate(datos)
    
    # Insertar datos en la base de datos
    insert_consolidated_data(consolidated_datos)
    
    # Mostrar resultados
    if "BB_ResumenGeneralMercado" in consolidated_datos:
        print("\nBB_ResumenGeneralMercado consolidado:")
        print(consolidated_datos["BB_ResumenGeneralMercado"].head())