from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:Password@localhost/ecomm")

try:
    engine.connect()
    print("Connected!")
except Exception as e:
    print("Connection error:", e)
