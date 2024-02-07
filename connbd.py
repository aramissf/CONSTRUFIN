import pyodbc as db
#Conectando ao banco de dados

server = 'NB021977'
database = 'UAU'
username = 'cef'
password = 'teste'

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try :
    conn = db.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM EMPRESAS')
    rows = cursor.fetchall()

    for row in rows:
        print(row)
    
    cursor.close()
    conn.close()

except Exception as e:
    print(f'erro : {e}')