import psycopg2
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

conn = psycopg2.connect("host=localhost port=5437 dbname=sawit_dwh user=dwh password=dwh")

sql = """
    SELECT schemaname, matviewname 
    FROM pg_matviews 
    WHERE matviewname LIKE '%timbun%' OR matviewname LIKE '%dm_%';
"""
df = pd.read_sql_query(sql, conn)
print("Materialized Views:")
print(df)

if not df.empty:
    sql2 = f"""
        SELECT attname as column_name, format_type(atttypid, atttypmod) as data_type
        FROM pg_attribute
        WHERE attrelid = 'datamart.dm_deteksi_penimbunan'::regclass
        AND attnum > 0 AND NOT attisdropped;
    """
    try:
        df2 = pd.read_sql_query(sql2, conn)
        print("\nColumns in dm_deteksi_penimbunan:")
        print(df2)
    except Exception as e:
        print("Error reading columns:", e)
