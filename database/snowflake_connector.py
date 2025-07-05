import snowflake.connector
from utils.config_loader import get_config
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
def executeSampleQuery(query):
    conn=connectSnow()    
    cur=conn.cursor()
    cur.execute(query)
    cur.close()
    conn.close()
    
if __name__=="__main__":
    executeSampleQuery("""
                          CREATE TABLE  IF NOT EXISTS
                          results(id INT AUTOINCREMENT PRIMARY KEY,totalResults VARCHAR(20),status VARCHAR(10))
                          """)