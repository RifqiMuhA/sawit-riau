"""
====================================================================================================
DAG ID          : dag6_analitik
Deskripsi       : Kalkulasi analitik tingkat lanjut (K-Means Clustering, Hoarding Detection, NDVI Status).
Jadwal          : Bulanan (@monthly)
Sumber Data     : PostGIS (fact_produksi, fact_operasional, fact_ndvi, dim_periode)
Target Data     : PostGIS (update kolom cluster_produksi, indikasi_timbun, status_kebun)
====================================================================================================
Alur Proses:
1. Menunggu penyelesaian DAG hulu (NDVI, Produksi, Harga CPO) menggunakan PythonSensors.
2. Segmentasi produktivitas perusahaan menggunakan algoritma K-Means (3 cluster).
3. Deteksi indikasi penimbunan stok (hoarding) berdasarkan anomali stok vs harga & penjualan.
4. Klasifikasi kondisi kebun (kritis/normal) berdasarkan distribusi persentil NDVI.
====================================================================================================
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sklearn.cluster import KMeans

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# SENSOR HELPERS
# ─────────────────────────────────────────────────────────────

SENSOR_TIMEOUT_SECONDS = 60 * 60 * 6  # 6 jam
SENSOR_POKE_INTERVAL   = 60           # cek tiap 1 menit

def _check_latest_run(external_dag_id: str, **kwargs) -> bool:
    """
    Mengecek apakah eksekusi terakhir dari sebuah DAG berstatus SUCCESS.
    Menggunakan pendekatan "latest run" agar tidak terikat pada execution_date
    yang sama persis.
    """
    runs = DagRun.find(dag_id=external_dag_id)
    if not runs:
        log.warning("[Sensor] Belum ada run untuk DAG: %s", external_dag_id)
        return False
    latest_run = sorted(runs, key=lambda x: x.execution_date, reverse=True)[0]
    log.info(
        "[Sensor] DAG %s — run terakhir: %s, state: %s",
        external_dag_id, latest_run.execution_date, latest_run.state
    )
    return latest_run.state == DagRunState.SUCCESS

def _on_sensor_timeout(context):
    """Callback saat sensor timeout — upstream DAG tidak selesai tepat waktu."""
    dag_id  = context["task"].op_kwargs["external_dag_id"]
    task_id = context["task_instance"].task_id
    log.error("[ALERT] Sensor %s timeout: %s tidak selesai dalam batas waktu.", task_id, dag_id)

# ─────────────────────────────────────────────────────────────
# K-MEANS PRODUKTIVITAS
# ─────────────────────────────────────────────────────────────

def extract_produksi(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = "SELECT perusahaan_id, periode, produktivitas FROM fact_produksi WHERE produktivitas IS NOT NULL"
    df = dwh.get_pandas_df(query)
    # Pastikan periode dalam format string
    df['periode'] = df['periode'].astype(str)
    
    # XCom butuh format JSON-serializable, jadi kita ubah ke records (list of dict)
    data = df.to_dict('records')
    ti.xcom_push(key="raw_produksi", value=data)
    log.info("Berhasil ekstrak %d baris data untuk K-Means.", len(data))

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
# DETEKSI PENIMBUNAN (HOARDING)
# ─────────────────────────────────────────────────────────────

def extract_operasional(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = """
        SELECT o.perusahaan_id, o.periode, o.stok_akhir_ton, o.volume_penjualan_ton, p.harga_cpo
        FROM fact_operasional o
        LEFT JOIN dim_periode p ON o.periode = p.periode
        ORDER BY o.perusahaan_id, o.periode
    """
    df = dwh.get_pandas_df(query)
    df['periode'] = df['periode'].astype(str)
    # Ganti NaN jadi None agar bisa masuk XCom (JSON-serializable)
    df = df.replace({np.nan: None})
    data = df.to_dict('records')
    ti.xcom_push(key="raw_operasional", value=data)
    log.info("Berhasil ekstrak %d baris data untuk deteksi penimbunan.", len(data))

def transform_hoarding(ti, **kwargs):
    raw_data = ti.xcom_pull(task_ids="tujuan_3_penimbunan.extract_operasional", key="raw_operasional")
    if not raw_data:
        return
        
    df = pd.DataFrame(raw_data)
    # Pastikan data diurutkan berdasarkan perusahaan dan periode
    df = df.sort_values(by=["perusahaan_id", "periode"]).reset_index(drop=True)
    
    # Geser data stok akhir (bulan lalu) per perusahaan
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
# STATUS KEBUN (NDVI)
# ─────────────────────────────────────────────────────────────

def extract_ndvi(ti, **kwargs):
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    query = "SELECT kode_wilayah, periode, ndvi_mean FROM fact_ndvi WHERE ndvi_mean IS NOT NULL"
    df = dwh.get_pandas_df(query)
    df['periode'] = df['periode'].astype(str)
    
    data = df.to_dict('records')
    ti.xcom_push(key="raw_ndvi", value=data)
    log.info("Berhasil ekstrak %d baris data NDVI.", len(data))

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
    dag_id      = "dag6_analitik",
    description = "DAG Analitik dengan ETL terpisah dan TaskGroups",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["analytics"],
) as dag:

    # ─────────────────────────────────────────────────────────
    # UPSTREAM SENSORS — Tunggu pipeline data selesai
    # ─────────────────────────────────────────────────────────

    # Data NDVI diisi oleh DAG 1 (dibutuhkan untuk status_kebun)
    wait_for_dag1 = PythonSensor(
        task_id             = "wait_for_dag1_ndvi",
        python_callable     = _check_latest_run,
        op_kwargs           = {"external_dag_id": "dag1_ndvi_extraction"},
        mode                = "reschedule",
        poke_interval       = SENSOR_POKE_INTERVAL,
        timeout             = SENSOR_TIMEOUT_SECONDS,
        soft_fail           = False,
        on_failure_callback = _on_sensor_timeout,
    )

    # Data Produksi & Operasional diisi oleh DAG 2
    wait_for_dag2 = PythonSensor(
        task_id             = "wait_for_dag2_produksi",
        python_callable     = _check_latest_run,
        op_kwargs           = {"external_dag_id": "dag2_produksi_etl"},
        mode                = "reschedule",   # tidak memblok worker slot
        poke_interval       = SENSOR_POKE_INTERVAL,
        timeout             = SENSOR_TIMEOUT_SECONDS,
        soft_fail           = False,
        on_failure_callback = _on_sensor_timeout,
    )

    # Data Harga CPO diisi oleh DAG 4 (dibutuhkan untuk deteksi penimbunan)
    wait_for_dag4 = PythonSensor(
        task_id             = "wait_for_dag4_harga_cpo",
        python_callable     = _check_latest_run,
        op_kwargs           = {"external_dag_id": "dag4_harga_cpo"},
        mode                = "reschedule",
        poke_interval       = SENSOR_POKE_INTERVAL,
        timeout             = SENSOR_TIMEOUT_SECONDS,
        soft_fail           = False,
        on_failure_callback = _on_sensor_timeout,
    )

    # ─────────────────────────────────────────────────────────
    # TASK GROUPS ANALITIK (berjalan paralel setelah sensor OK)
    # ─────────────────────────────────────────────────────────

    # --- TaskGroup 1: K-Means Cluster Produktivitas (Tujuan 2) ---
    with TaskGroup("tujuan_2_kmeans", tooltip="Klastering Produktivitas dengan K-Means") as tg_kmeans:
        e1 = PythonOperator(task_id="extract_produksi", python_callable=extract_produksi)
        t1 = PythonOperator(task_id="transform_kmeans", python_callable=transform_kmeans)
        l1 = PythonOperator(task_id="load_clusters", python_callable=load_clusters)
        e1 >> t1 >> l1

    # --- TaskGroup 2: Deteksi Penimbunan (Tujuan 3) ---
    with TaskGroup("tujuan_3_penimbunan", tooltip="Deteksi Indikasi Penahanan Stok CPO") as tg_timbun:
        e2 = PythonOperator(task_id="extract_operasional", python_callable=extract_operasional)
        t2 = PythonOperator(task_id="transform_hoarding", python_callable=transform_hoarding)
        l2 = PythonOperator(task_id="load_hoarding", python_callable=load_hoarding)
        e2 >> t2 >> l2

    # --- TaskGroup 3: Status Lahan Berdasarkan NDVI (Tujuan 1) ---
    with TaskGroup("tujuan_1_status_kebun", tooltip="Label Lahan Kritis Berdasarkan NDVI") as tg_ndvi:
        e3 = PythonOperator(task_id="extract_ndvi", python_callable=extract_ndvi)
        t3 = PythonOperator(task_id="transform_ndvi", python_callable=transform_ndvi)
        l3 = PythonOperator(task_id="load_ndvi", python_callable=load_ndvi)
        e3 >> t3 >> l3

    # ─────────────────────────────────────────────────────────
    # ALUR DEPENDENSI
    # Sensor (paralel) → selesai semua → TaskGroup analitik (paralel)
    # ─────────────────────────────────────────────────────────

    # Ketiga sensor berjalan paralel (tidak ada dependency antar sensor)
    sensors = [wait_for_dag1, wait_for_dag2, wait_for_dag4]

    # Setelah SEMUA sensor sukses, baru ketiga TaskGroup analitik berjalan paralel
    sensors >> tg_kmeans
    sensors >> tg_timbun
    sensors >> tg_ndvi
