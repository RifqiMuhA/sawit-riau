"""
DAG 5 — ETL Panen: OLTP → DWH (fact_panen)
============================================
Sumber  : 4 tipe tabel panen dari 12 OLTP database
  A (MySQL) : jadwal_panen   → periode CHAR(7), satuan TON
  B (MySQL) : rencana_panen  → periode (bulan CHAR(7)), satuan TON
  C (PgSQL) : target_panen   → tgl_laporan DATE → YYYY-MM, satuan TON
  D (PgSQL) : realisasi_panen → tahun+bulan terpisah → YYYY-MM, satuan TON
Target  : fact_panen di sawit_dwh (PostGIS)
Jadwal  : @monthly (setelah DAG 2 selesai, agar dim_kebun sudah terisi)

Airflow Tasks:
1. extract_mysql_a, extract_mysql_b, extract_pg_c, extract_pg_d (Paralel)
2. transform_panen_data : Menggabungkan dan menormalisasi seluruh data mentah
3. load_fact_panen      : Upsert ke PostGIS DWH
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# KONSTANTA
# ─────────────────────────────────────────────────────────────

MYSQL_A_DATABASES = ["a-01-kampar",        "a-02-pelalawan",      "a-03-siak"]
MYSQL_B_DATABASES = ["b-04-indragiri-hulu", "b-05-kuansing",       "b-06-indragiri-hilir"]
PG_C_SCHEMAS      = ["c-07-bengkalis",      "c-08-rokan-hilir",    "c-09-meranti"]
PG_D_SCHEMAS      = ["d-10-rokan-hulu",     "d-11-pekanbaru",      "d-12-dumai"]

# ─────────────────────────────────────────────────────────────
# HELPER (Transformasi)
# ─────────────────────────────────────────────────────────────

def _build_row(kebun_id: str, periode: str, perusahaan_id: str,
               target: float, realisasi: float | None, status: str) -> dict:
    """Normalisasi 1 baris panen + hitung gap_persen."""
    real = float(realisasi) if realisasi is not None and pd.notna(realisasi) else None
    tgt  = float(target) if target is not None and pd.notna(target) else 0.0
    
    if tgt > 0 and real is not None:
        gap = round((real - tgt) / tgt * 100, 2)
    else:
        gap = None
        
    status_norm = str(status).lower().strip()
    if status_norm not in ("selesai", "tertunda", "batal"):
        status_norm = "tertunda"
        
    return {
        "kebun_id"            : kebun_id,
        "periode"             : periode,
        "perusahaan_id"       : perusahaan_id,
        "target_panen_ton"    : tgt,
        "realisasi_panen_ton" : real,
        "gap_persen"          : gap,
        "status_id"           : status_norm,
    }

# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS (Extract)
# ─────────────────────────────────────────────────────────────

def extract_mysql_a(ti, **kwargs):
    mysql = MySqlHook(mysql_conn_id="mysql_oltp")
    raw_data = []
    for db in MYSQL_A_DATABASES:
        df = mysql.get_pandas_df(
            f"""
            SELECT kebun_id, perusahaan_id, DATE_FORMAT(tanggal_mulai, '%%Y-%%m') AS periode,
                   target_panen_ton, realisasi_panen_ton, status
            FROM `{db}`.jadwal_panen
            """
        )
        # Convert ke dict agar aman masuk XCom
        for _, r in df.iterrows():
            raw_data.append({
                "type": "A",
                "kebun_id": r["kebun_id"],
                "periode": str(r["periode"]).strip(),
                "perusahaan_id": r["perusahaan_id"],
                "target": r["target_panen_ton"],
                "realisasi": r.get("realisasi_panen_ton"),
                "status": r["status"]
            })
    ti.xcom_push(key="raw_a", value=raw_data)
    log.info("Extracted %d rows from MySQL A", len(raw_data))

def extract_mysql_b(ti, **kwargs):
    mysql = MySqlHook(mysql_conn_id="mysql_oltp")
    raw_data = []
    for db in MYSQL_B_DATABASES:
        df = mysql.get_pandas_df(
            f"""
            SELECT blok_id, id_pks, DATE_FORMAT(tgl_mulai, '%%Y-%%m') AS bulan,
                   target_ton, realisasi_ton, status_panen
            FROM `{db}`.rencana_panen
            """
        )
        for _, r in df.iterrows():
            raw_data.append({
                "type": "B",
                "kebun_id": r["blok_id"],
                "periode": str(r["bulan"]).strip(),
                "perusahaan_id": r["id_pks"],
                "target": r["target_ton"],
                "realisasi": r.get("realisasi_ton"),
                "status": r["status_panen"]
            })
    ti.xcom_push(key="raw_b", value=raw_data)
    log.info("Extracted %d rows from MySQL B", len(raw_data))

def extract_pg_c(ti, **kwargs):
    oltp_pg = PostgresHook(postgres_conn_id="postgres_oltp")
    raw_data = []
    for schema in PG_C_SCHEMAS:
        df = oltp_pg.get_pandas_df(
            f"""
            SELECT lahan_id, kode_perusahaan,
                   TO_CHAR(tgl_mulai, 'YYYY-MM') AS periode,
                   target_panen, realisasi_panen, status
            FROM "{schema}".target_panen
            """
        )
        for _, r in df.iterrows():
            raw_data.append({
                "type": "C",
                "kebun_id": r["lahan_id"],
                "periode": r["periode"],
                "perusahaan_id": r["kode_perusahaan"],
                "target": r["target_panen"],
                "realisasi": r.get("realisasi_panen"),
                "status": r["status"]
            })
    ti.xcom_push(key="raw_c", value=raw_data)
    log.info("Extracted %d rows from Postgres C", len(raw_data))

def extract_pg_d(ti, **kwargs):
    oltp_pg = PostgresHook(postgres_conn_id="postgres_oltp")
    raw_data = []
    for schema in PG_D_SCHEMAS:
        df = oltp_pg.get_pandas_df(
            f"""
            SELECT kebun_id, perusahaan_id,
                   TO_CHAR(tgl_mulai, 'YYYY-MM') AS periode,
                   target_panen_ton, realisasi_panen_ton, status_panen
            FROM "{schema}".realisasi_panen
            """
        )
        for _, r in df.iterrows():
            raw_data.append({
                "type": "D",
                "kebun_id": r["kebun_id"],
                "periode": r["periode"],
                "perusahaan_id": r["perusahaan_id"],
                "target": r["target_panen_ton"],
                "realisasi": r.get("realisasi_panen_ton"),
                "status": r["status_panen"]
            })
    ti.xcom_push(key="raw_d", value=raw_data)
    log.info("Extracted %d rows from Postgres D", len(raw_data))

# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS (Transform & Load)
# ─────────────────────────────────────────────────────────────

def transform_panen_data(ti, **kwargs):
    """Menerima raw data dari ke-4 source, lalu menormalisasi semuanya."""
    raw_a = ti.xcom_pull(task_ids="extract_mysql_a", key="raw_a") or []
    raw_b = ti.xcom_pull(task_ids="extract_mysql_b", key="raw_b") or []
    raw_c = ti.xcom_pull(task_ids="extract_pg_c", key="raw_c") or []
    raw_d = ti.xcom_pull(task_ids="extract_pg_d", key="raw_d") or []
    
    all_raw = raw_a + raw_b + raw_c + raw_d
    transformed_rows = []
    
    for r in all_raw:
        transformed_rows.append(_build_row(
            kebun_id=r["kebun_id"],
            periode=r["periode"],
            perusahaan_id=r["perusahaan_id"],
            target=r["target"],
            realisasi=r["realisasi"],
            status=r["status"]
        ))
        
    ti.xcom_push(key="transformed_data", value=transformed_rows)
    log.info("Berhasil menormalisasi total %d baris data panen gabungan.", len(transformed_rows))

def load_fact_panen(ti, **kwargs):
    """Upsert data gabungan yang sudah bersih ke fact_panen DWH."""
    rows = ti.xcom_pull(task_ids="transform_panen_data", key="transformed_data")
    
    if not rows:
        log.info("Tidak ada data untuk di-load.")
        return

    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    sql = """
        INSERT INTO fact_panen
            (kebun_id, periode, perusahaan_id,
             target_panen_ton, realisasi_panen_ton, gap_persen, status_id)
        VALUES (%(kebun_id)s, %(periode)s, %(perusahaan_id)s,
                %(target_panen_ton)s, %(realisasi_panen_ton)s,
                %(gap_persen)s, %(status_id)s)
        ON CONFLICT (kebun_id, periode) DO UPDATE SET
            perusahaan_id        = EXCLUDED.perusahaan_id,
            target_panen_ton     = EXCLUDED.target_panen_ton,
            realisasi_panen_ton  = EXCLUDED.realisasi_panen_ton,
            gap_persen           = EXCLUDED.gap_persen,
            status_id            = EXCLUDED.status_id
    """
    conn = dwh.get_conn()
    cur  = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    log.info("Berhasil menyimpan %d baris ke tabel fact_panen di DWH.", len(rows))


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner"            : "airflow",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id      = "dag5_panen_etl",
    description = "ETL Panen (Extract paralel -> Transform gabungan -> Load DWH)",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["etl", "panen", "tujuan4"],
) as dag:

    # 1. TAHAP EXTRACT (PARALEL 4 DATABASE)
    t_ext_a = PythonOperator(task_id="extract_mysql_a", python_callable=extract_mysql_a)
    t_ext_b = PythonOperator(task_id="extract_mysql_b", python_callable=extract_mysql_b)
    t_ext_c = PythonOperator(task_id="extract_pg_c", python_callable=extract_pg_c)
    t_ext_d = PythonOperator(task_id="extract_pg_d", python_callable=extract_pg_d)

    # 2. TAHAP TRANSFORM
    t_transform = PythonOperator(task_id="transform_panen_data", python_callable=transform_panen_data)

    # 3. TAHAP LOAD
    t_load = PythonOperator(task_id="load_fact_panen", python_callable=load_fact_panen)

    # ALUR GRAFIK (Graph View)
    [t_ext_a, t_ext_b, t_ext_c, t_ext_d] >> t_transform >> t_load
