import os
import sys
sys.path.append(os.path.join(os.getcwd(), 'dashboard'))
from db import run_query, get_riau_geojson

sql = """
    SELECT p.kode_wilayah, k.nama_kabupaten,
           COUNT(*) AS total_insiden
    FROM datamart.dm_deteksi_penimbunan d
    JOIN dim_perusahaan p ON d.nama_perusahaan = p.nama_perusahaan
    JOIN dim_kabupaten k ON p.kode_wilayah = k.kode_wilayah
    WHERE d.indikasi_timbun = TRUE
    GROUP BY p.kode_wilayah, k.nama_kabupaten
"""
df = run_query(sql)
print("Map Data:")
print(df)

geo = get_riau_geojson()
print("\nGeoJSON Features count:", len(geo.get('features', [])))
if geo.get('features'):
    print("Sample Feature Properties:", geo['features'][0]['properties'])
