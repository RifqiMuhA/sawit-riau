"""
DAG 1 — Ekstraksi NDVI GEE + KNN Imputation
==================================================================
"""

from __future__ import annotations
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Any

import ee
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dateutil.relativedelta import relativedelta
from sklearn.impute import KNNImputer

log = logging.getLogger(__name__)

def task_extract_gee(**context) -> list[dict]:
    # 1. Inisialisasi GEE
    try:
        ee.Initialize()
        log.info("GEE Berhasil diinisialisasi.")
    except Exception as e:
        raise RuntimeError(f"Gagal inisialisasi GEE: {e}")

    exec_date: datetime = context["logical_date"]
    periode = exec_date.strftime("%Y-%m")
    start_date = exec_date.strftime("%Y-%m-%d")
    end_date = (exec_date + relativedelta(months=1)).strftime("%Y-%m-%d")

    dwh_hook = PostgresHook(postgres_conn_id="postgis_dwh")
    
    # 2. Ambil Geometri + Centroid untuk fitur spasial KNN
    cur = dwh_hook.get_cursor()
    cur.execute("""
        SELECT kode_wilayah, nama_kabupaten, ST_AsGeoJSON(geometry),
               ST_X(ST_Centroid(geometry)) as lon, ST_Y(ST_Centroid(geometry)) as lat
        FROM dim_kabupaten
    """)
    rows = cur.fetchall()
    
    # 3. Ekstraksi GEE
    s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")\
        .filterDate(start_date, end_date)\
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 30))
    
    mask = ee.ImageCollection("BIOPAMA/GlobalOilPalm/v1").select("classification").mosaic().eq(1)
    
    def add_ndvi(i): return i.addBands(i.normalizedDifference(["B8", "B4"]).rename("NDVI"))
    img = s2.map(add_ndvi).median().updateMask(mask)

    log.info(f"Memproses {len(rows)} wilayah untuk periode {periode}...")

    results = []
    # Loop Ekstraksi Original
    for row in rows:
        kode_wilayah, nama_kabupaten, geojson_str, lon, lat = row
        try:
            geom = ee.Geometry(json.loads(geojson_str))
            # Hitung Mean dan Count sekaligus
            stats = img.reduceRegion(
                reducer=ee.Reducer.mean().combine(ee.Reducer.count(), sharedInputs=True),
                geometry=geom, scale=100, maxPixels=1e13
            ).getInfo()
            
            mean_val = stats.get("NDVI_mean")
            count_val = stats.get("NDVI_count")
            
            if mean_val is not None:
                log.info(f"  [GEE] {nama_kabupaten}: NDVI {round(mean_val, 4)} | {int(count_val)} Pixels")
                results.append({
                    "kode_wilayah": kode_wilayah,
                    "nama_kabupaten": nama_kabupaten,
                    "lon": lon,
                    "lat": lat,
                    "ndvi_mean": round(mean_val, 4),
                    "pixel_count": int(count_val)
                })
            else:
                log.warning(f"  [GEE] {nama_kabupaten}: No Data (Awan/Masking)")
                results.append({
                    "kode_wilayah": kode_wilayah, "nama_kabupaten": nama_kabupaten,
                    "lon": lon, "lat": lat, "ndvi_mean": None, "pixel_count": 0
                })
        except Exception as e:
            log.error(f"  [GEE] Error di {nama_kabupaten}: {e}")
            results.append({
                "kode_wilayah": kode_wilayah, "nama_kabupaten": nama_kabupaten,
                "lon": lon, "lat": lat, "ndvi_mean": None, "pixel_count": 0
            })

    # Return for XCom
    return results

def task_transform_impute(**context) -> list[dict]:
    ti = context["ti"]
    extracted_data = ti.xcom_pull(task_ids="extract_group.extract_gee")
    
    df = pd.DataFrame(extracted_data)
    
    # 4. Tahap KNN Imputation (Hanya untuk yang masih Null)
    if df['ndvi_mean'].isnull().any():
        if df['ndvi_mean'].notnull().any(): 
            log.info("Menjalankan KNN Imputer untuk mengisi kekosongan...")
            imputer = KNNImputer(n_neighbors=3)
            imputed_vals = imputer.fit_transform(df[['ndvi_mean', 'lon', 'lat']])
            
            for i, val in enumerate(df['ndvi_mean']):
                if pd.isna(val):
                    imputed_ndvi = round(imputed_vals[i, 0], 4)
                    df.at[i, 'ndvi_mean'] = imputed_ndvi
                    log.info(f"  [KNN] {df.iloc[i]['nama_kabupaten']}: Terisi otomatis -> {imputed_ndvi}")
        else:
            log.error("Semua wilayah kosong. KNN tidak dapat dijalankan.")

    # Return for XCom load
    return df.to_dict('records')

def task_load_dwh(**context) -> None:
    ti = context["ti"]
    transformed_data = ti.xcom_pull(task_ids="transform_group.transform_impute")
    
    exec_date: datetime = context["logical_date"]
    periode = exec_date.strftime("%Y-%m")
    
    # 5. Load ke DWH
    dwh_hook = PostgresHook(postgres_conn_id="postgis_dwh")
    conn = dwh_hook.get_conn()
    db_cur = conn.cursor()
    
    for r in transformed_data:
        if r['ndvi_mean'] is not None:
            # Upsert Dim Waktu
            db_cur.execute("""
                INSERT INTO dim_waktu (periode, tahun, bulan, kuartal) 
                VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, (periode, exec_date.year, exec_date.month, (exec_date.month-1)//3 + 1))
            
            # Upsert Fact NDVI (memasukkan ndvi_mean dan pixel_count)
            db_cur.execute("""
                INSERT INTO fact_ndvi (kode_wilayah, periode, ndvi_mean, pixel_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (kode_wilayah, periode) DO UPDATE 
                SET ndvi_mean = EXCLUDED.ndvi_mean,
                    pixel_count = EXCLUDED.pixel_count
            """, (r['kode_wilayah'], periode, r['ndvi_mean'], r.get('pixel_count')))
    
    conn.commit()
    db_cur.close()
    conn.close()
    log.info(f"ETL Selesai. Data periode {periode} berhasil disimpan ke PostGIS.")

# --- DAG Definition ---
from airflow.utils.task_group import TaskGroup

default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="dag1_ndvi_extraction_knn_silent",
    schedule="0 0 1 * *",  # Jadwal Bulanan: berjalan pada tanggal 1 setiap bulan pukul 00:00
    start_date=datetime(2023, 1, 1),
    catchup=True,
    default_args=default_args,
    tags=["ndvi", "knn", "gee", "riau"],
    max_active_runs=1
) as dag:
    
    with TaskGroup("extract_group", tooltip="Ekstraksi Data GEE dan PostGIS") as extract_group:
        t_extract = PythonOperator(
            task_id="extract_gee",
            python_callable=task_extract_gee,
            execution_timeout=timedelta(minutes=30)
        )
        
    with TaskGroup("transform_group", tooltip="Imputasi Data KNN") as transform_group:
        t_transform = PythonOperator(
            task_id="transform_impute",
            python_callable=task_transform_impute
        )
        
    with TaskGroup("load_group", tooltip="Load Data ke DWH") as load_group:
        t_load = PythonOperator(
            task_id="load_dwh",
            python_callable=task_load_dwh
        )

    t_extract >> t_transform >> t_load