"""
extract_mssql_bronze.py
Extract tables from MSSQL ecomm DB and load to BigQuery bronze tables.

Pre-reqs:
  pip install sqlalchemy pymssql pandas google-cloud-bigquery
  (If pymssql fails to install, see note below about using pyodbc)
  Ensure GOOGLE_APPLICATION_CREDENTIALS env var is set
"""

import os
import time
import uuid
import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

# --------------- CONFIG (edit as needed) ----------------
PROJECT_ID = "my2ndproject-479707"
BQ_DATASET = "dw_ecomm"

MSS_DB_URL = os.getenv("MSS_DB_URL")

MSSQL_TABLES = ["stores", "store_sales"]

CHUNK_SIZE = 5000
# --------------------------------------------------------

odbc_str = (
    f"DRIVER={os.getenv('MSSQL_DRIVER')};"
    f"SERVER={os.getenv('MSSQL_SERVER')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE')};"
    f"UID={os.getenv('MSSQL_USERNAME')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')};"
)

encoded_odbc = quote_plus(odbc_str)

def make_mssql_engine():
    url = f"mssql+pyodbc:///?odbc_connect={encoded_odbc}"
    return create_engine(url, pool_pre_ping=True)

def add_metadata(df, source_name, batch_id):
    df['_extracted_at'] = pd.Timestamp.utcnow()
    df['_source'] = source_name
    df['_batch_id'] = batch_id
    return df

def load_chunk_to_bq(client, df, table_suffix):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_suffix}"
    job_config = LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, autodetect=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    tbl = client.get_table(table_id)
    return tbl.num_rows

def extract_from_engine(engine, table_name, source_label, client, target_table_suffix):
    sql = text(f"SELECT * FROM dbo.{table_name}")  # MSSQL under dbo schema
    total = 0
    batch_id = str(uuid.uuid4())
    with engine.connect() as conn:
        for i, chunk in enumerate(pd.read_sql(sql, conn, chunksize=CHUNK_SIZE)):
            print(f"[{source_label}] {table_name} chunk {i+1} rows={len(chunk)}")
            chunk = add_metadata(chunk, source_label, batch_id)
            rows_now = load_chunk_to_bq(client, chunk, target_table_suffix)
            total += len(chunk)
            print(f" -> loaded; target rows now: {rows_now}")
    return total

def main():
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise RuntimeError("Set GOOGLE_APPLICATION_CREDENTIALS env var to service account JSON path.")

    bq = bigquery.Client(project=PROJECT_ID)
    engine = make_mssql_engine()

    for t in MSSQL_TABLES:
        tgt = f"{t}_bronze"
        print("Starting extract:", t, "->", tgt)
        n = extract_from_engine(engine, t, f"mssql.{t}", bq, tgt)
        print(f"Finished {t}: rows extracted (sum of chunks) = {n}")

    print("MSSQL extraction completed.")

if __name__ == "__main__":
    main()
