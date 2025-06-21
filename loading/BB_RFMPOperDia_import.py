import pandas as pd
import pyodbc
from loading.conexion_db import get_db_connection

def check_table_contents():
    """
    Verifica el contenido de la tabla BB_RFMPOperDia.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM BB_RFMPOperDia")
        count = cursor.fetchone()[0]
        
        if count > 0:
            cursor.execute("SELECT TOP 5 * FROM BB_RFMPOperDia ORDER BY fecha DESC")
            rows = cursor.fetchall()
        else:
            pass
            
        cursor.close()
        conn.close()
    except Exception as e:
        pass

def insert_data(df: pd.DataFrame) -> bool:
    """
    Inserta los datos en la tabla BB_RFMPOperDia usando pyodbc.
    Si ya existe un registro con la misma clave primaria, lo actualiza.
    """
    try:
        # Conversiones y validaciones
        # Conversiones de fechas
        df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
        df['Fecha_Venc'] = pd.to_datetime(df['Fecha_Venc'], errors='coerce')
        df['Fecha_Liq'] = pd.to_datetime(df['Fecha_Liq'], errors='coerce')
        df.dropna(subset=['Fecha'], inplace=True)
        
        # Conversiones numéricas
        df['Valor_Negociado'] = pd.to_numeric(df['Valor_Negociado'], errors='coerce').fillna(0)
        df['Precio'] = pd.to_numeric(df['Precio'], errors='coerce').fillna(0)
        df['Frec_Pago'] = pd.to_numeric(df['Frec_Pago'], errors='coerce').fillna(0)
        df['Valor_Transado'] = pd.to_numeric(df['Valor_Transado'], errors='coerce').fillna(0)
        df['Equiv_en_DOP'] = pd.to_numeric(df['Equiv_en_DOP'], errors='coerce').fillna(0)
        df['Tasa_Cupon'] = pd.to_numeric(df['Tasa_Cupon'], errors='coerce').fillna(0)
        df['Rend_Equiv'] = pd.to_numeric(df['Rend_Equiv'], errors='coerce').fillna(0)
        df['Dias_Venc'] = pd.to_numeric(df['Dias_Venc'], errors='coerce').fillna(0)
        df['Nom_Unit'] = pd.to_numeric(df['Nom_Unit'], errors='coerce').fillna(0)

        conn = get_db_connection()
        cursor = conn.cursor()
        
        processed_count = 0
        skipped_count = 0

        for index, row in df.iterrows():
            try:
                cursor.execute("""
                    MERGE BB_RFMPOperDia AS target
                    USING (SELECT ? AS numero_operacion, ? AS fecha) AS source
                    ON target.numero_operacion = source.numero_operacion AND target.fecha = source.fecha
                    WHEN MATCHED THEN
                        UPDATE SET 
                            rueda = ?,
                            Cod_Local = ?,
                            Cod_ISIN = ?,
                            Cod_Emisor = ?,
                            Fecha_Venc = ?,
                            Frec_Pago = ?,
                            Tasa_Cupon = ?,
                            Nom_Unit = ?,
                            Valor_Negociado = ?,
                            Precio = ?,
                            Valor_Transado = ?,
                            Rend_Equiv = ?,
                            Mon = ?,
                            Equiv_en_DOP = ?,
                            Fecha_Liq = ?,
                            Dias_Venc = ?
                    WHEN NOT MATCHED THEN
                        INSERT (numero_operacion, rueda, Cod_Local, Cod_ISIN, Cod_Emisor, Fecha_Venc, Frec_Pago, Tasa_Cupon, Nom_Unit, Valor_Negociado, Precio, Valor_Transado, Rend_Equiv, Mon, Equiv_en_DOP, Fecha_Liq, Dias_Venc, fecha)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                row['numero_operacion'],
                row['Fecha'].strftime('%Y-%m-%d'),
                row['rueda'],
                row['Cod_Local'],
                row['Cod_ISIN'],
                row['Cod_Emisor'],
                row['Fecha_Venc'].strftime('%Y-%m-%d') if pd.notna(row['Fecha_Venc']) else None,
                row['Frec_Pago'],
                row['Tasa_Cupon'],
                row['Nom_Unit'],
                row['Valor_Negociado'],
                row['Precio'],
                row['Valor_Transado'],
                row['Rend_Equiv'],
                row['Mon'],
                row['Equiv_en_DOP'],
                row['Fecha_Liq'].strftime('%Y-%m-%d') if pd.notna(row['Fecha_Liq']) else None,
                row['Dias_Venc'],
                row['numero_operacion'],
                row['rueda'],
                row['Cod_Local'],
                row['Cod_ISIN'],
                row['Cod_Emisor'],
                row['Fecha_Venc'].strftime('%Y-%m-%d') if pd.notna(row['Fecha_Venc']) else None,
                row['Frec_Pago'],
                row['Tasa_Cupon'],
                row['Nom_Unit'],
                row['Valor_Negociado'],
                row['Precio'],
                row['Valor_Transado'],
                row['Rend_Equiv'],
                row['Mon'],
                row['Equiv_en_DOP'],
                row['Fecha_Liq'].strftime('%Y-%m-%d') if pd.notna(row['Fecha_Liq']) else None,
                row['Dias_Venc'],
                row['Fecha'].strftime('%Y-%m-%d'))
                
                if cursor.rowcount > 0:
                    processed_count += 1
            except Exception as row_error:
                skipped_count += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        check_table_contents()
        return True
        
    except Exception as e:
        return False
