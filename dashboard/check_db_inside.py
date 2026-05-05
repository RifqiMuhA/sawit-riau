import pandas as pd
from db import run_query

print("Checking fact_alert_operasional INSIDE container...")
df = run_query("SELECT count(*) FROM fact_alert_operasional")
print(df)

df_p = run_query("SELECT count(*) FROM dim_perusahaan")
print("Perusahaan count:", df_p)

df_s = run_query("SELECT periode, sum(total_alert) as tot FROM fact_alert_operasional GROUP BY periode LIMIT 5")
print("Sample data:")
print(df_s)
