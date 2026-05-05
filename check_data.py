import pandas as pd
import psycopg2
import os

DWH_CONFIG = {
    "host": "localhost", # Running from host, targetting forwarded port 5437 (postgis_db)
    "port": "5437",
    "user": "dwh",
    "password": "dwh",
    "database": "sawit_dwh",
}

def run_query(sql):
    with psycopg2.connect(**DWH_CONFIG) as conn:
        return pd.read_sql_query(sql, conn)

print("Checking fact_alert_operasional...")
try:
    df = run_query("SELECT count(*) FROM fact_alert_operasional")
    print(df)
    
    df_sample = run_query("SELECT * FROM fact_alert_operasional LIMIT 5")
    print("Sample data:")
    print(df_sample)
except Exception as e:
    print(f"Error: {e}")
