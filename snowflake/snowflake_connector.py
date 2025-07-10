import snowflake.connector
from utils.config_loader import get_config
import os
account = get_config("snowflake","account")
user = get_config("snowflake","user")
password = get_config("snowflake","password")
role = get_config("snowflake",'role')
warehouse = get_config("snowflake","warehouse")
database = get_config("snowflake","database")
schema = get_config("snowflake","schema")

def connectSnow():
    try:
       conn = snowflake.connector.connect(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema
    )
       return conn
    except Exception as e:
        raise Exception(e)
def executeQuery(query):
    conn=connectSnow()    
    cur=conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()
def executeInsertQuery(query,record):
    conn=connectSnow()    
    cur=conn.cursor()
    cur.executemany(query,record)
    cur.close()
    conn.close()
def execute_sql_file( file_path):
    with open(file_path, 'r') as f:
        sql = f.read()
    print(f"Running: {file_path}")
    return sql 
if __name__=="__main__":
    folder_path=os.path.join("snowflake","createTables")
    sql_files = sorted(f for f in os.listdir(folder_path) if f.endswith(".sql"))

    for file_name in sql_files:
        file_path = os.path.join(folder_path, file_name)
        query=execute_sql_file( file_path)
        executeQuery(query)