from transformers.Sheet_transformers.data_manager import get_dataset
import pandas as pd

def analyze_sheet_structure(sheet_name: str):
    """
    Analiza la estructura de una hoja específica.
    """
    print(f"\n=== Analizando hoja: {sheet_name} ===")
    
    # Obtener datos de la hoja
    df = get_dataset("2025-03-17", "2025-03-18", sheet_name)
    
    if df is None or df.empty:
        print("No se encontraron datos para esta hoja")
        return
    
    # Información básica
    print(f"\nDimensiones: {df.shape}")
    print("\nColumnas:")
    for col in df.columns:
        print(f"- {col}")
        
    # Muestra de datos
    print("\nPrimeras 5 filas:")
    print(df.head().to_string())
    
    # Tipos de datos
    print("\nTipos de datos:")
    print(df.dtypes)
    
    # Valores únicos en cada columna
    print("\nValores únicos por columna:")
    for col in df.columns:
        unique_values = df[col].nunique()
        print(f"- {col}: {unique_values} valores únicos")
        if unique_values < 10:  # Si hay pocos valores únicos, mostrarlos
            print(f"  Valores: {sorted(df[col].unique())}")

def main():
    sheets_to_analyze = [
        "BB_RFVTransPuestoBolsaMP",
        "BB_RFMPOperDia",
        "BB_RFMPOperDiaFirme",
        "BB_RFMSOperDia",
        "BB_RFMSOperPlazos",
        "BB_RentaFijaOperacionesFuturasA",
        "BB_RFEmisionesCorpV"
    ]
    
    for sheet in sheets_to_analyze:
        analyze_sheet_structure(sheet)

if __name__ == "__main__":
    main() 