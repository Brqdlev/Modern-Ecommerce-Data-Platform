from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

odbc_str = (
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=localhost,1433;"
    "DATABASE=master;"
    "UID=sa;"
    "PWD=MyStr0ng!Pass123;"
)

connection_url = "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)

engine = create_engine(connection_url)

try:
    with engine.connect() as conn:
        print("Connected! Result:", conn.execute(text("SELECT 1")).scalar())
except Exception as e:
    print("Connection failed:", e)
