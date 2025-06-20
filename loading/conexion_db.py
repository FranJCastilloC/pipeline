import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__)))
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