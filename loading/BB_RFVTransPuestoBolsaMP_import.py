import pandas as pd
import pyodbc
from loading.conexion_db import get_db_connection

def check_table_contents():
    """
    Verifica el contenido de la tabla BB_RFVTransPuestoBolsaMP.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM BB_RFVTransPuestoBolsaMP")
        count = cursor.fetchone()[0]
        print(f"\nTotal de registros en BB_RFVTransPuestoBolsaMP: {count}")
        
        if count > 0:
            cursor.execute("SELECT TOP 5 * FROM BB_RFVTransPuestoBolsaMP ORDER BY fecha DESC")
            rows = cursor.fetchall()
            print("\nÚltimos 5 registros:")
            for row in rows:
                print(row)
        else:
            print("La tabla está vacía.")
            
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error al verificar tabla: {e}")

def insert_data(df: pd.DataFrame) -> bool:
    """
    Inserta los datos en la tabla BB_RFVTransPuestoBolsaMP usando pyodbc.
    Si ya existe un registro con la misma clave primaria, lo actualiza.
    """
    try:
        print("\n--- Iniciando inserción para BB_RFVTransPuestoBolsaMP ---")
        print(f"DataFrame a insertar (primeras 5 filas):")
        print(df.head())
        print(df.info())

        # Conversiones y validaciones
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        df.dropna(subset=['fecha'], inplace=True)
        df['transado_usd'] = pd.to_numeric(df['transado_usd'], errors='coerce').fillna(0)
        df['usd_equivalente_dop'] = pd.to_numeric(df['usd_equivalente_dop'], errors='coerce').fillna(0)
        df['transado_dop'] = pd.to_numeric(df['transado_dop'], errors='coerce').fillna(0)

        conn = get_db_connection()
        cursor = conn.cursor()
        
        processed_count = 0
        skipped_count = 0

        for index, row in df.iterrows():
            try:
                cursor.execute("""
                    MERGE BB_RFVTransPuestoBolsaMP AS target
                    USING (SELECT ? AS participante, ? AS fecha) AS source
                    ON target.participante = source.participante AND target.fecha = source.fecha
                    WHEN MATCHED THEN
                        UPDATE SET 
                            transado_usd = ?,
                            usd_equivalente_dop = ?,
                            transado_dop = ?
                    WHEN NOT MATCHED THEN
                        INSERT (participante, transado_usd, usd_equivalente_dop, transado_dop, fecha)
                        VALUES (?, ?, ?, ?, ?);
                """,
                row['participante'],
                row['fecha'].strftime('%Y-%m-%d'),
                row['transado_usd'],
                row['usd_equivalente_dop'],
                row['transado_dop'],
                row['participante'],
                row['transado_usd'],
                row['usd_equivalente_dop'],
                row['transado_dop'],
                row['fecha'].strftime('%Y-%m-%d'))
                
                if cursor.rowcount > 0:
                    processed_count += 1
            except Exception as row_error:
                print(f"Error en fila {index} ({row['participante']}): {row_error}")
                skipped_count += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("\n--- Proceso de inserción completado ---")
        print(f"Registros procesados (insertados/actualizados): {processed_count}")
        print(f"Registros omitidos por errores: {skipped_count}")
        
        check_table_contents()
        return True
        
    except Exception as e:
        print(f"\nError fatal al insertar datos para BB_RFVTransPuestoBolsaMP: {e}")
        return False
