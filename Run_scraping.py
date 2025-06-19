import pandas as pd
from scraping import main
import datetime
import re
import pandas as pd
import numpy as np
import re
import importlib

# ===============================================
# MAPEO DE DATAFRAMES A FUNCIONES DE LIMPIEZA
# ===============================================
CLEANING_FUNCTIONS = {
    'ResumenGeneralMercado': {
        'module': 'transformers.ResumenGeneralMercado',
        'function': 'clean_and_build_resumen_general_mercado'
    },
    # Agregar más DataFrames aquí siguiendo el mismo patrón:
    # 'OperacionesDia': {
    #     'module': 'transformers.OperacionesDia', 
    #     'function': 'clean_and_build_operaciones_dia'
    # },
}

def import_cleaning_function(df_name):
    """
    Importa dinámicamente la función de limpieza para un DataFrame específico.
    
    Args:
        df_name (str): Nombre del DataFrame
        
    Returns:
        function: Función de limpieza correspondiente o None si no existe
    """
    try:
        if df_name in CLEANING_FUNCTIONS:
            config = CLEANING_FUNCTIONS[df_name]
            module = importlib.import_module(config['module'])
            cleaning_function = getattr(module, config['function'])
            return cleaning_function
        else:
            return None
    except ImportError as e:
        return None
    except AttributeError as e:
        return None

def apply_cleaning_if_available(df_name, df):
    """
    Aplica limpieza a un DataFrame si tiene función de limpieza disponible.
    
    Args:
        df_name (str): Nombre del DataFrame
        df (pd.DataFrame): DataFrame a limpiar
        
    Returns:
        pd.DataFrame: DataFrame limpio o original si no hay función de limpieza
    """
    cleaning_function = import_cleaning_function(df_name)
    if cleaning_function:
        cleaned_df = cleaning_function(df)
        if cleaned_df is not None:
            return cleaned_df
        else:
            return df
    else:
        return df

def procesar_datos_limpieza():
    start_date = '2025-06-13'
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    datos = main(start_date, end_date)

    # Extraer los reportes únicos
    dic_organizado = {}
    unique_repot = set()  # set para que almacene valores únicos
    for key in datos.keys():  # datos.keys para que solo itere sobre los títulos del diccionario
        base_name = "_".join(key.split("_")[:-1])  # key.split("_") divide el nombre del título cada vez que encuentre "_"
                                                  # [:-1] -> Toma todos los elementos menos el último así nos deshacemos de la fecha
                                                  # "_".join une nuevamente los elementos con un "_"
        unique_repot.add(base_name)

    # Procesamos cada tipo de reporte único
    for repote in unique_repot:  
        dfs_list = []
        for key in datos.keys():
            if key.startswith(repote):  # Revisar si en datos key el nombre empieza con alguno de los valores de la lista de unique_repot
                date_str = key.split("_")[-1]  # Si se cumple la condición extrae el último valor dividido por "_" que es la fecha
                datos_copy = datos[key].copy()  # Creamos una copia de datos para no dañar el original
                datos_copy['fecha'] = date_str  # Agregamos la columna fecha
                dfs_list.append(datos_copy)
        if dfs_list:
            dic_organizado[repote] = pd.concat(dfs_list, ignore_index=True)  # Combinamos todos los DataFrames del mismo tipo

    # Ya tienes los keys como lista
    keys = list(dic_organizado.keys())  # Convierte las claves del diccionario en una lista para poder iterar sobre ellas

    for key in keys:  # Recorre cada clave en la lista de claves
       # Crea un nombre de variable limpio eliminando "BB_" y reemplazando caracteres problemáticos
       # Esto transformará por ejemplo "BB_BB_RFMPOperDia" en "RFMPOperDia"
       df_name = key.replace("BB_", "").replace("-", "_").replace(" ", "_")
       
       # Obtiene el DataFrame correspondiente del diccionario dic_organizado
       # usando la clave original
       df = dic_organizado[key]
       
       # Crea una variable global con el nombre limpio y le asigna el DataFrame original
       globals()[df_name] = df    
       
       # Aplicar limpieza si está disponible
       cleaned_df = apply_cleaning_if_available(df_name, df)
       
       # Crear variable global para el DataFrame limpio
       globals()[f"{df_name}_clean"] = cleaned_df
    
    return dic_organizado


