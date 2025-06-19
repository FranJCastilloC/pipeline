import pandas as pd
import numpy as np

def clean_and_build_resumen_general_mercado(df):
    """
    Clean and build ResumenGeneralMercado from the input dataframe.
    Extracts specific daily metrics from "Operaciones del Día" section for ALL dates.
    - 4 Markets: Mdo.Secundario RF/RV, Mdo.Primario RF/RV  
    - 4 Columns: USD, USD Equiv DOP, DOP, Total DOP
    
    Args:
        df (pd.DataFrame): Input dataframe containing ResumenGeneralMercado data for multiple dates
        
    Returns:
        pd.DataFrame: Daily metrics structured with one row per date
    """
    try:
        # Create a copy to avoid modifying the original dataframe
        resumen_df = df.copy()
        
        # ===============================================
        # ANÁLISIS DETALLADO DE ESTRUCTURA MULTI-FECHA
        # ===============================================
        print("\n=== ANÁLISIS DETALLADO DE ESTRUCTURA ===")
        print(f"Total filas en DataFrame: {len(resumen_df)}")
        
        # Ver fechas únicas disponibles
        if 'fecha' in resumen_df.columns:
            fechas_unicas = resumen_df['fecha'].unique()
            print(f"Fechas encontradas: {len(fechas_unicas)} días")
            print(f"Fechas: {list(fechas_unicas)}")
            
            # Mostrar distribución de filas por fecha
            print("\n📊 DISTRIBUCIÓN DE FILAS POR FECHA:")
            for fecha in fechas_unicas:
                count = len(resumen_df[resumen_df['fecha'] == fecha])
                print(f"  {fecha}: {count} filas")
                
            # Mostrar estructura de las primeras filas de cada fecha
            print("\n🔍 ESTRUCTURA POR FECHA (primeras 10 filas de cada fecha):")
            for fecha in fechas_unicas:
                df_fecha = resumen_df[resumen_df['fecha'] == fecha].copy()
                print(f"\n--- FECHA {fecha} ---")
                print(f"Rango de índices: {df_fecha.index.min()} - {df_fecha.index.max()}")
                
                # Mostrar primeras 10 filas de esta fecha
                for i, (idx, row) in enumerate(df_fecha.head(10).iterrows()):
                    col_0 = str(row.iloc[0])[:50] if pd.notna(row.iloc[0]) else "NaN"
                    col_2 = str(row.iloc[2])[:50] if pd.notna(row.iloc[2]) else "NaN"
                    print(f"  [{i}] Fila {idx}: Col0='{col_0}' | Col2='{col_2}'")
                    
                    # Buscar "Operaciones del Día" específicamente
                    if any('Operaciones del Día' in str(cell) for cell in row if pd.notna(cell)):
                        print(f"      ⭐ ENCONTRADO 'Operaciones del Día' en fila {idx}")
        else:
            print("❌ No se encontró columna 'fecha'")
            return None
        
        print("=" * 80)
        
        # ===============================================
        # PROCESAR CADA FECHA INDIVIDUALMENTE CORREGIDO
        # ===============================================
        
        todas_las_metricas = []
        
        for fecha in fechas_unicas:
            print(f"\n🗓️ PROCESANDO FECHA: {fecha}")
            
            # Filtrar datos para esta fecha específica Y RESETEAR ÍNDICES
            df_fecha = resumen_df[resumen_df['fecha'] == fecha].copy().reset_index(drop=True)
            
            if len(df_fecha) == 0:
                print(f"   ❌ No hay datos para {fecha}")
                continue
                
            print(f"   📋 Filas para esta fecha: {len(df_fecha)} (índices reseteados 0-{len(df_fecha)-1})")
            
            # Inicializar métricas para esta fecha
            metricas_fecha = {
                'fecha': fecha,
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
            
            # Función auxiliar para extraer valor numérico de una celda
            def extraer_valor_numerico(valor):
                """Convierte un valor a float, manejando formatos de número con comas"""
                try:
                    if pd.isna(valor):
                        return 0
                    valor_str = str(valor).strip()
                    if valor_str == '' or valor_str == '0' or valor_str == 'NaN' or valor_str == 'nan':
                        return 0
                    # Remove commas and convert to float
                    valor_clean = valor_str.replace(',', '')
                    return float(valor_clean)
                except:
                    return 0
            
            # ===============================================
            # BUSCAR SECCIÓN "OPERACIONES DEL DÍA" EN INDICES RESETEADOS
            # ===============================================
            
            operaciones_dia_idx = None
            for idx in range(len(df_fecha)):
                row = df_fecha.iloc[idx]
                if any('Operaciones del Día' in str(cell) for cell in row if pd.notna(cell)):
                    operaciones_dia_idx = idx
                    print(f"   ✅ 'Operaciones del Día' encontrado en ÍNDICE RESETEADO {idx}")
                    break
            
            if operaciones_dia_idx is not None:
                # Mostrar contexto alrededor de "Operaciones del Día"
                print(f"   🔍 CONTEXTO ALREDEDOR DE 'Operaciones del Día' (índice {operaciones_dia_idx}):")
                
                start_context = max(0, operaciones_dia_idx - 2)
                end_context = min(len(df_fecha), operaciones_dia_idx + 10)
                
                for ctx_idx in range(start_context, end_context):
                    row = df_fecha.iloc[ctx_idx]
                    col_2 = str(row.iloc[2])[:60] if len(row) > 2 and pd.notna(row.iloc[2]) else "NaN"
                    prefix = "➤" if ctx_idx == operaciones_dia_idx else " "
                    print(f"   {prefix} [{ctx_idx}] '{col_2}'")
                
                # Buscar las filas de mercados dentro de la sección
                start_search = operaciones_dia_idx + 1
                end_search = min(start_search + 20, len(df_fecha))
                
                mercados_encontrados = 0
                
                print(f"   🔎 BUSCANDO MERCADOS en índices {start_search}-{end_search}:")
                
                for idx in range(start_search, end_search):
                    if idx >= len(df_fecha):
                        break
                        
                    row = df_fecha.iloc[idx]
                    # El nombre del mercado está en la columna 2 (índice 2)
                    nombre_mercado = str(row.iloc[2]) if len(row) > 2 and pd.notna(row.iloc[2]) else ""
                    
                    print(f"   [{idx}] Revisando: '{nombre_mercado.strip()[:50]}...'")
                    
                    # Extraer valores de las columnas específicas
                    col_3 = extraer_valor_numerico(row.iloc[3] if len(row) > 3 else 0)  # USD
                    col_4 = extraer_valor_numerico(row.iloc[4] if len(row) > 4 else 0)  # Posible DOP
                    col_5 = extraer_valor_numerico(row.iloc[5] if len(row) > 5 else 0)  # USD Equiv DOP
                    col_6 = extraer_valor_numerico(row.iloc[6] if len(row) > 6 else 0)  # DOP
                    col_7 = extraer_valor_numerico(row.iloc[7] if len(row) > 7 else 0)  # Total DOP
                    col_8 = extraer_valor_numerico(row.iloc[8] if len(row) > 8 else 0)  # Alternativo
                    
                    # Identificar el tipo de mercado con patrones más flexibles
                    market_found = False
                    
                    # Patrones más flexibles para identificar mercados
                    if any(pattern in nombre_mercado for pattern in ['Mdo. Secundario RF', 'Secundario RF', 'MDO SECUNDARIO RF']):
                        print(f"   ✅📊 Secundario RF - USD: {col_3:,.2f}, DOP: {col_6:,.2f}, Total: {col_7:,.2f}")
                        metricas_fecha['mdo_secundario_rf_usd'] = col_3
                        metricas_fecha['mdo_secundario_rf_usd_equiv_dop'] = col_5
                        metricas_fecha['mdo_secundario_rf_dop'] = col_6
                        metricas_fecha['mdo_secundario_rf_total_dop'] = col_7
                        mercados_encontrados += 1
                        market_found = True
                    
                    elif any(pattern in nombre_mercado for pattern in ['Mdo. Primario RF', 'Primario RF', 'MDO PRIMARIO RF']):
                        print(f"   ✅📊 Primario RF - USD: {col_3:,.2f}, DOP: {col_6:,.2f}, Total: {col_7:,.2f}")
                        metricas_fecha['mdo_primario_rf_usd'] = col_3
                        metricas_fecha['mdo_primario_rf_usd_equiv_dop'] = col_5
                        metricas_fecha['mdo_primario_rf_dop'] = col_6
                        metricas_fecha['mdo_primario_rf_total_dop'] = col_7
                        mercados_encontrados += 1
                        market_found = True
                    
                    elif any(pattern in nombre_mercado for pattern in ['Mdo. Secundario RV', 'Secundario RV', 'MDO SECUNDARIO RV']):
                        print(f"   ✅📈 Secundario RV - USD: {col_3:,.2f}, DOP: {col_6:,.2f}, Total: {col_7:,.2f}")
                        metricas_fecha['mdo_secundario_rv_usd'] = col_3
                        metricas_fecha['mdo_secundario_rv_usd_equiv_dop'] = col_5
                        metricas_fecha['mdo_secundario_rv_dop'] = col_6
                        metricas_fecha['mdo_secundario_rv_total_dop'] = col_7
                        mercados_encontrados += 1
                        market_found = True
                    
                    elif any(pattern in nombre_mercado for pattern in ['Mdo. Primario RV', 'Primario RV', 'MDO PRIMARIO RV']):
                        print(f"   ✅📈 Primario RV - USD: {col_3:,.2f}, DOP: {col_6:,.2f}, Total: {col_7:,.2f}")
                        metricas_fecha['mdo_primario_rv_usd'] = col_3
                        metricas_fecha['mdo_primario_rv_usd_equiv_dop'] = col_5
                        metricas_fecha['mdo_primario_rv_dop'] = col_6
                        metricas_fecha['mdo_primario_rv_total_dop'] = col_7
                        mercados_encontrados += 1
                        market_found = True
                
                print(f"   ✅ Mercados procesados: {mercados_encontrados}/4")
            else:
                print(f"   ❌ No se encontró 'Operaciones del Día' para {fecha}")
            
            # Agregar las métricas de esta fecha al resultado
            todas_las_metricas.append(metricas_fecha)
        
        # ===============================================
        # CREAR DATAFRAME FINAL CON TODAS LAS FECHAS
        # ===============================================
        
        if todas_las_metricas:
            resultado_df = pd.DataFrame(todas_las_metricas)
            
            # Ordenar por fecha
            resultado_df = resultado_df.sort_values('fecha').reset_index(drop=True)
            
            print(f"\n=== RESUMEN FINAL ===")
            print(f"✅ Fechas procesadas: {len(resultado_df)}")
            print(f"📅 Rango: {resultado_df['fecha'].min()} → {resultado_df['fecha'].max()}")
            print(f"📊 Estructura final: {resultado_df.shape}")
            
            # Mostrar resumen por fecha
            print("\n📋 RESUMEN POR FECHA:")
            for _, row in resultado_df.iterrows():
                total_usd = (row['mdo_secundario_rf_usd'] + row['mdo_primario_rf_usd'] + 
                           row['mdo_secundario_rv_usd'] + row['mdo_primario_rv_usd'])
                total_dop = (row['mdo_secundario_rf_total_dop'] + row['mdo_primario_rf_total_dop'] + 
                           row['mdo_secundario_rv_total_dop'] + row['mdo_primario_rv_total_dop'])
                print(f"  {row['fecha']}: USD ${total_usd:,.2f} | DOP ${total_dop:,.2f}")
            
            print("=" * 60)
            
            return resultado_df
        else:
            print("❌ No se procesaron datos para ninguna fecha")
            return None
        
    except Exception as e:
        print(f"Error en limpieza de ResumenGeneralMercado: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
