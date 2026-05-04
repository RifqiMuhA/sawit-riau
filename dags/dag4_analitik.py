"""
DAG 4 — Kalkulasi Analitik (Machine Learning & Business Logic)
==============================================================
Menjalankan perhitungan analitik menggunakan paradigma ETL murni 
dengan Airflow TaskGroups untuk visualisasi Graph yang jelas.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.cluster import KMeans

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# TUJUAN 2: K-MEANS PRODUKTIVITAS
# ─────────────────────────────────────────────────────────────

def extract_produksi(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = "SELECT perusahaan_id, periode, produktivitas FROM fact_produksi WHERE produktivitas IS NOT NULL"
    df = dwh.get_pandas_df(query)
    # Cast periode back to string in case it's parsed as something else
    df['periode'] = df['periode'].astype(str)
    
    # Airflow XCom requires JSON-serializable types, so we convert to records
    data = df.to_dict('records')
    ti.xcom_push(key="raw_produksi", value=data)
    log.info("Extracted %d rows for K-Means.", len(data))

def transform_kmeans(ti, **kwargs):
    raw_data = ti.xcom_pull(task_ids="tujuan_2_kmeans.extract_produksi", key="raw_produksi")
    if not raw_data:
        return
    
    df = pd.DataFrame(raw_data)
    updates = []
    
    for periode, group in df.groupby("periode"):
        if len(group) < 3:
            continue
            
        X = group[["produktivitas"]].values
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X)
        centers = kmeans.cluster_centers_.flatten()
        
        sorted_idx = np.argsort(centers)
        label_map = {
            sorted_idx[0]: "underperform",
            sorted_idx[1]: "average",
            sorted_idx[2]: "overperform"
        }
        
        group_labeled = group.copy()
        group_labeled["cluster_label"] = [label_map[l] for l in labels]
        
        for _, row in group_labeled.iterrows():
            updates.append({
                "perusahaan_id": row["perusahaan_id"],
                "periode": row["periode"],
                "cluster_produksi": row["cluster_label"]
            })
            
    ti.xcom_push(key="transformed_clusters", value=updates)
    log.info("Transformed %d rows via K-Means.", len(updates))

def load_clusters(ti, **kwargs):
    updates = ti.xcom_pull(task_ids="tujuan_2_kmeans.transform_kmeans", key="transformed_clusters")
    if not updates:
        return
        
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    sql = """
        UPDATE fact_produksi 
        SET cluster_produksi = %(cluster_produksi)s
        WHERE perusahaan_id = %(perusahaan_id)s AND periode = %(periode)s
    """
    conn = dwh.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, updates)
    conn.commit()
    log.info("Loaded %d cluster updates to DWH.", len(updates))

# ─────────────────────────────────────────────────────────────
# TUJUAN 3: DETEKSI PENIMBUNAN (HOARDING)
# ─────────────────────────────────────────────────────────────

def extract_operasional(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = """
        SELECT o.perusahaan_id, o.periode, o.stok_akhir_ton, o.volume_penjualan_ton, h.harga_cpo
        FROM fact_operasional o
        LEFT JOIN fact_harga_cpo h ON o.periode = h.periode
        ORDER BY o.perusahaan_id, o.periode
    """
    df = dwh.get_pandas_df(query)
    df['periode'] = df['periode'].astype(str)
    # Mengganti nilai NaN menjadi None agar bisa di-serialize ke JSON XCom
    df = df.replace({np.nan: None})
    data = df.to_dict('records')
    ti.xcom_push(key="raw_operasional", value=data)
    log.info("Extracted %d rows for Hoarding detection.", len(data))

def transform_hoarding(ti, **kwargs):
    raw_data = ti.xcom_pull(task_ids="tujuan_3_penimbunan.extract_operasional", key="raw_operasional")
    if not raw_data:
        return
        
    df = pd.DataFrame(raw_data)
    # Pastikan data diurutkan berdasarkan perusahaan dan periode
    df = df.sort_values(by=["perusahaan_id", "periode"]).reset_index(drop=True)
    
    # Shift stok akhir (bulan lalu) per perusahaan
    df['prev_stok'] = df.groupby('perusahaan_id')['stok_akhir_ton'].shift(1)
    
    # Rata-rata historis penjualan (3 bulan ke belakang, tidak termasuk bulan ini)
    # Kita menggunakan min_periods=1 agar jika baru ada 1-2 bulan history, tetap bisa dihitung
    df['rata_rata_historis_ton'] = df.groupby('perusahaan_id')['volume_penjualan_ton'].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    
    # Harga CPO sebelumnya (karena harga CPO sama se-Riau per periode, kita cukup urutkan unik periode)
    # Tapi cara termudahnya, buat dataframe harga unik dulu
    harga_df = df[['periode', 'harga_cpo']].drop_duplicates().sort_values('periode')
    harga_df['prev_harga'] = harga_df['harga_cpo'].shift(1)
    
    # Gabung kembali
    df = pd.merge(df, harga_df[['periode', 'prev_harga']], on='periode', how='left')
    
    # Kondisi Indikasi Timbun (Semua kondisi harus terpenuhi / TRUE)
    kondisi_harga = df['harga_cpo'] < df['prev_harga']
    kondisi_jual  = df['volume_penjualan_ton'] < df['rata_rata_historis_ton']
    kondisi_stok  = df['stok_akhir_ton'] > df['prev_stok']
    
    df['indikasi_timbun'] = kondisi_harga & kondisi_jual & kondisi_stok
    
    # Hilangkan nilai NaN agar Postgres aman
    df = df.replace({np.nan: None})
    
    updates = df[['perusahaan_id', 'periode', 'rata_rata_historis_ton', 'indikasi_timbun']].to_dict('records')
    ti.xcom_push(key="transformed_hoarding", value=updates)
    log.info("Transformed %d rows for Hoarding detection.", len(updates))

def load_hoarding(ti, **kwargs):
    updates = ti.xcom_pull(task_ids="tujuan_3_penimbunan.transform_hoarding", key="transformed_hoarding")
    if not updates:
        return
        
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    sql = """
        UPDATE fact_operasional 
        SET rata_rata_historis_ton = %(rata_rata_historis_ton)s,
            indikasi_timbun = %(indikasi_timbun)s
        WHERE perusahaan_id = %(perusahaan_id)s AND periode = %(periode)s
    """
    conn = dwh.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, updates)
    conn.commit()
    log.info("Loaded %d hoarding flags to DWH.", len(updates))

# ─────────────────────────────────────────────────────────────
# TUJUAN 1: STATUS KEBUN KRITIS (NDVI)
# ─────────────────────────────────────────────────────────────

def extract_ndvi(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = "SELECT kode_wilayah, periode, ndvi_mean FROM fact_ndvi WHERE ndvi_mean IS NOT NULL"
    df = dwh.get_pandas_df(query)
    df['periode'] = df['periode'].astype(str)
    
    data = df.to_dict('records')
    ti.xcom_push(key="raw_ndvi", value=data)
    log.info("Extracted %d rows for NDVI.", len(data))

def transform_ndvi(ti, **kwargs):
    raw_data = ti.xcom_pull(task_ids="tujuan_1_status_kebun.extract_ndvi", key="raw_ndvi")
    if not raw_data:
        return
        
    df = pd.DataFrame(raw_data)
    
    # Hitung persentil per periode menggunakan pandas transform
    df['p33'] = df.groupby('periode')['ndvi_mean'].transform(lambda x: x.quantile(0.33))
    df['p66'] = df.groupby('periode')['ndvi_mean'].transform(lambda x: x.quantile(0.66))
    
    # Gunakan np.select untuk kondisi berjenjang
    conditions = [
        df['ndvi_mean'] < df['p33'],
        df['ndvi_mean'] >= df['p66']
    ]
    choices = ['kritis', 'normal']
    df['status_kebun'] = np.select(conditions, choices, default='menurun')
    
    updates = df[['kode_wilayah', 'periode', 'status_kebun']].to_dict('records')
    ti.xcom_push(key="transformed_ndvi", value=updates)
    log.info("Transformed %d rows for NDVI status.", len(updates))

def load_ndvi(ti, **kwargs):
    updates = ti.xcom_pull(task_ids="tujuan_1_status_kebun.transform_ndvi", key="transformed_ndvi")
    if not updates:
        return
        
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    sql = """
        UPDATE fact_ndvi 
        SET status_kebun = %(status_kebun)s
        WHERE kode_wilayah = %(kode_wilayah)s AND periode = %(periode)s
    """
    conn = dwh.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, updates)
    conn.commit()
    log.info("Loaded %d NDVI status updates to DWH.", len(updates))


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner"            : "airflow",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id      = "dag4_analitik",
    description = "DAG Analitik dengan ETL terpisah dan TaskGroups",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["analitik", "machine_learning", "kpi"],
) as dag:

    # --- TaskGroup 1: K-Means Cluster ---
    with TaskGroup("tujuan_2_kmeans", tooltip="Klastering Produktivitas dengan K-Means") as tg_kmeans:
        e1 = PythonOperator(task_id="extract_produksi", python_callable=extract_produksi)
        t1 = PythonOperator(task_id="transform_kmeans", python_callable=transform_kmeans)
        l1 = PythonOperator(task_id="load_clusters", python_callable=load_clusters)
        e1 >> t1 >> l1

    # --- TaskGroup 2: Deteksi Penimbunan ---
    with TaskGroup("tujuan_3_penimbunan", tooltip="Deteksi Indikasi Penahanan Stok CPO") as tg_timbun:
        e2 = PythonOperator(task_id="extract_operasional", python_callable=extract_operasional)
        t2 = PythonOperator(task_id="transform_hoarding", python_callable=transform_hoarding)
        l2 = PythonOperator(task_id="load_hoarding", python_callable=load_hoarding)
        e2 >> t2 >> l2

    # --- TaskGroup 3: Status Lahan ---
    with TaskGroup("tujuan_1_status_kebun", tooltip="Label Lahan Kritis Berdasarkan NDVI") as tg_ndvi:
        e3 = PythonOperator(task_id="extract_ndvi", python_callable=extract_ndvi)
        t3 = PythonOperator(task_id="transform_ndvi", python_callable=transform_ndvi)
        l3 = PythonOperator(task_id="load_ndvi", python_callable=load_ndvi)
        e3 >> t3 >> l3

    # Alur Eksekusi Antar Grup
    # Karena ketiganya independen, kita bisa jalankan paralel.
    [tg_kmeans, tg_timbun, tg_ndvi]
