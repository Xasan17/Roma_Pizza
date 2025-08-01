import pyodbc

def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=TA_GEO_07\\SQLEXPRESS;'
        'DATABASE=Roma_pizza;'
        'Trusted_Connection=yes;'
    )