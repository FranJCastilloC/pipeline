import pandas as pd
import datetime
import requests
from io import BytesIO

def generate_dates(start_date, end_date):
    """Genera una lista de fechas entre start_date y end_date en el formato requerido."""
    date_list = pd.date_range(start=start_date, end=end_date).strftime('%d-%m-%Y').tolist()
    return date_list

def download_excel(url):
    """Descarga el archivo Excel desde la URL proporcionada con headers para evitar bloqueos."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://boletin.bvrd.com.do/"
    }
    try:
        response = requests.get(url, headers=headers, allow_redirects=True)
        response.raise_for_status()
        
        if "html" in response.headers.get("Content-Type", ""):  # Verifica si la respuesta es HTML (error en descarga)
            print(f"El servidor devolvió una página en lugar del archivo para: {url}")
            return None
        
        return BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        return None

def process_excel(file, date):
    """Lee un archivo Excel y almacena sus hojas en un diccionario de DataFrames."""
    dfs = {}
    try:
        excel_data = pd.ExcelFile(file)
        for sheet_name in excel_data.sheet_names:
            df = pd.read_excel(excel_data, sheet_name=sheet_name)
            clean_sheet_name = sheet_name.replace(" ", "_").replace("-", "_")
            dfs[f"BB_{clean_sheet_name}_{date}"] = df
    except Exception as e:
        print(f"Error procesando el archivo {date}: {e}")
    return dfs

def main(start_date, end_date):
    """Ejecuta el proceso para descargar y almacenar los DataFrames de los boletines."""
    all_data = {}
    meses_es = {
        "January": "Enero", "February": "Febrero", "March": "Marzo", "April": "Abril",
        "May": "Mayo", "June": "Junio", "July": "Julio", "August": "Agosto",
        "September": "Septiembre", "October": "Octubre", "November": "Noviembre", "December": "Diciembre"
    }
    
    dates = generate_dates(start_date, end_date)
    for date in dates:
        date_obj = datetime.datetime.strptime(date, '%d-%m-%Y')
        year = date_obj.year
        month_name = meses_es[date_obj.strftime('%B')]
        month_number1 = f"{date_obj.month:02d}" # Mes con ceros a la izquierda
        month_number2 = date_obj.month # Mes sin ceros a la izquierda
        day_number = f"{date_obj.day:02d}"  # Día con ceros a la izquierda
        
        url = f"https://boletin.bvrd.com.do/BOLETINES+Y+PRECIOS+{year}/Boletin+Consolidado/{month_number2}.+{month_name}/{day_number}-{month_number1}-{year}-Boletin+BVRD+Consolidado+excel.xlsx"
        file = download_excel(url)
        if file:
            extracted_dfs = process_excel(file, date)
            all_data.update(extracted_dfs)
    
    return all_data

if __name__ == "__main__":
    start_date = "2025-03-19"
    end_date = datetime.date.today().strftime('%Y-%m-%d')
    data = main(start_date, end_date)
    
    # Visualizar la información de los DataFrames obtenidos
    for key, df in data.items():
        print(f"{key}: {df.shape}")


