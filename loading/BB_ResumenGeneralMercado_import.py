import pandas as pd
import pyodbc

def get_db_connection():
    """
    Crea la conexión a la base de datos usando autenticación de Windows.
    """
    conn_str = (
        "Driver={SQL Server};"
        "Server=F_CASTILLO;"
        "Database=bolsa_valores;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def check_table_contents():
    """
    Verifica el contenido de la tabla BB_ResumenGeneralMercado.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM BB_ResumenGeneralMercado")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        pass

def insert_data(df: pd.DataFrame) -> bool:
    """
    Inserta los datos en la tabla BB_ResumenGeneralMercado usando pyodbc.
    Si ya existe un registro con la misma clave primaria, lo actualiza.
    """
    try:
        # Convertir tipos de datos
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True)
        df['transado_usd'] = pd.to_numeric(df['transado_usd'])
        df['usd_equivalente_dop'] = pd.to_numeric(df['usd_equivalente_dop'])
        df['transado_dop'] = pd.to_numeric(df['transado_dop'])
        df['total_transado_dop'] = pd.to_numeric(df['total_transado_dop'])
        
        # Crear conexión
        conn = get_db_connection()
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        # Insertar datos uno por uno para mejor control de errores
        for index, row in df.iterrows():
            try:
                # Usar MERGE para insertar o actualizar
                cursor.execute("""
                    MERGE BB_ResumenGeneralMercado AS target
                    USING (SELECT ? AS mercado, ? AS fecha) AS source
                    ON target.mercado = source.mercado AND target.fecha = source.fecha
                    WHEN MATCHED THEN
                        UPDATE SET 
                            transado_usd = ?,
                            usd_equivalente_dop = ?,
                            transado_dop = ?,
                            total_transado_dop = ?
                    WHEN NOT MATCHED THEN
                        INSERT (mercado, transado_usd, usd_equivalente_dop, 
                                transado_dop, total_transado_dop, fecha)
                        VALUES (?, ?, ?, ?, ?, ?);
                """, (
                    row['mercado'],
                    row['fecha'].strftime('%Y-%m-%d'),
                    float(row['transado_usd']),
                    float(row['usd_equivalente_dop']),
                    float(row['transado_dop']),
                    float(row['total_transado_dop']),
                    row['mercado'],
                    float(row['transado_usd']),
                    float(row['usd_equivalente_dop']),
                    float(row['transado_dop']),
                    float(row['total_transado_dop']),
                    row['fecha'].strftime('%Y-%m-%d')
                ))
                
                # Verificar si fue una inserción o actualización
                if cursor.rowcount > 0:
                    # No hay forma directa de saber si fue INSERT o UPDATE con MERGE
                    # Asumimos que si no hay error, fue exitoso
                    pass
                
            except Exception as row_error:
                skipped_count += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        check_table_contents()
        
        return True
        
    except Exception as e:
        try:
            cursor.close()
            conn.close()
        except:
            pass
        
        return False
