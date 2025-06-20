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
    Verifica el contenido de la tabla BB_RentaFijaOperacionesFuturasA.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM BB_RentaFijaOperacionesFuturasA")
        count = cursor.fetchone()[0]
        print(f"\nTotal de registros en la tabla: {count}")
        
        # Mostrar algunos registros
        if count > 0:
            cursor.execute("SELECT TOP 5 * FROM BB_RentaFijaOperacionesFuturasA ORDER BY fecha DESC")
            rows = cursor.fetchall()
            print("\nÚltimos 5 registros:")
            for row in rows:
                print(row)
        else:
            print("La tabla está vacía")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error al verificar tabla: {e}")

def insert_data(df: pd.DataFrame) -> bool:
    """
    Inserta los datos en la tabla BB_RentaFijaOperacionesFuturasA usando pyodbc.
    Si ya existe un registro con la misma clave primaria, lo actualiza.
    """
    try:
        # Mostrar información del DataFrame antes de la inserción
        print(f"\nDataFrame a insertar:")
        print(f"- Forma: {df.shape}")
        print(f"- Columnas: {list(df.columns)}")
        print(f"- Primeras filas:")
        print(df.head())
        
        # TODO: Implementar lógica específica para BB_RentaFijaOperacionesFuturasA
        # Convertir tipos de datos según las columnas específicas de esta tabla
        # df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True)
        # df['columna_numerica'] = pd.to_numeric(df['columna_numerica'])
        
        # Crear conexión
        conn = get_db_connection()
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        # TODO: Implementar lógica de inserción específica para esta tabla
        # Insertar datos uno por uno para mejor control de errores
        for index, row in df.iterrows():
            try:
                # TODO: Implementar MERGE específico para BB_RentaFijaOperacionesFuturasA
                # cursor.execute("""
                #     MERGE BB_RentaFijaOperacionesFuturasA AS target
                #     USING (SELECT ? AS campo1, ? AS fecha) AS source
                #     ON target.campo1 = source.campo1 AND target.fecha = source.fecha
                #     WHEN MATCHED THEN
                #         UPDATE SET 
                #             campo2 = ?,
                #             campo3 = ?
                #     WHEN NOT MATCHED THEN
                #         INSERT (campo1, campo2, campo3, fecha)
                #         VALUES (?, ?, ?, ?);
                # """, (
                #     row['campo1'],
                #     row['fecha'].strftime('%Y-%m-%d'),
                #     float(row['campo2']),
                #     float(row['campo3']),
                #     row['campo1'],
                #     float(row['campo2']),
                #     float(row['campo3']),
                #     row['fecha'].strftime('%Y-%m-%d')
                # ))
                
                pass  # Placeholder - implementar lógica real
                
            except Exception as row_error:
                print(f"Error en fila {index}: {row_error}")
                skipped_count += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"\nProceso completado:")
        print(f"- Registros procesados: {len(df)}")
        print(f"- Registros omitidos por errores: {skipped_count}")
        print(f"- Registros procesados exitosamente: {len(df) - skipped_count}")
        
        # Verificar el contenido de la tabla después de la inserción
        print("\nVerificando contenido de la tabla después de la inserción:")
        check_table_contents()
        
        return True
        
    except Exception as e:
        print(f"\nError al insertar datos:")
        print(f"Tipo de error: {type(e).__name__}")
        print(f"Mensaje de error: {str(e)}")
        
        try:
            cursor.close()
            conn.close()
        except:
            pass
        
        return False 