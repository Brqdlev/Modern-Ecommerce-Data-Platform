"""
extract_mysql_bronze.py
Extract tables from MySQL ecomm DB and load to BigQuery bronze tables.

Pre-reqs:
  pip install sqlalchemy pymysql pandas google-cloud-bigquery
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

load_dotenv()

# --------------- CONFIG (edit as needed) ----------------
PROJECT_ID = "my2ndproject-479707"
BQ_DATASET = "dw_ecomm"


DB_URL = os.getenv("MYQ_DB_URL")

MYSQL_TABLES = ["customers", "orders", "order_items", "products"]

CHUNK_SIZE = 10000
# --------------------------------------------------------

def make_mysql_engine():
    url = DB_URL
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
    sql = text(f"SELECT * FROM {table_name}")
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
    engine = make_mysql_engine()

    for t in MYSQL_TABLES:
        tgt = f"{t}_bronze"
        print("Starting extract:", t, "->", tgt)
        n = extract_from_engine(engine, t, f"mysql.{t}", bq, tgt)
        print(f"Finished {t}: rows extracted (sum of chunks) = {n}")

    print("MySQL extraction completed.")

if __name__ == "__main__":
    main()
