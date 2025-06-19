import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Run_scraping import procesar_datos_limpieza

def main():
    """
    Función principal que ejecuta todo el pipeline de scraping y limpieza.
    """
    print("�� INICIANDO PIPELINE DE DATOS")
    
    try:
        # Ejecutar el pipeline completo
        dic_organizado = procesar_datos_limpieza()
        
        # Solo mostrar información del ResumenGeneralMercado procesado
        import Run_scraping
        
        # Verificar si existe ResumenGeneralMercado_clean
        if hasattr(Run_scraping, 'ResumenGeneralMercado_clean'):
            resumen_clean = Run_scraping.ResumenGeneralMercado_clean
            print(f"\n✅ PIPELINE COMPLETADO")
            print(f"📋 ResumenGeneralMercado procesado - Dimensiones: {resumen_clean.shape}")
        else:
            print("❌ No se encontró ResumenGeneralMercado_clean")
        
        return dic_organizado
        
    except Exception as e:
        print(f"❌ ERROR EN EL PIPELINE: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    main()
