from .scraper import main
import datetime


def get_dataset(start_date, end_date, sheet_name):
    """
    Descarga y retorna solo el DataFrame de la hoja solicitada.
    
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

if __name__ == "__main__":
    start_date = "2025-06-17"
    end_date = "2025-06-17"
    datos = data_manager(start_date, end_date)

    for key, df in datos.items():
        print(f"{key}: {df.shape}")