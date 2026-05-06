"""
====================================================================================================
DAG ID          : dag5_alert_etl
Deskripsi       : ETL Data Alert Operasional dari MongoDB ke Data Warehouse.
Jadwal          : Bulanan (@monthly)
Sumber Data     : MongoDB (sawit_alerts.log_alert_harian)
Target Data     : PostGIS (fact_alert_operasional)
====================================================================================================
Alur Proses:
1. Ekstraksi data mentah alert harian dari MongoDB.
2. Pembersihan data dan ekstraksi periode (YYYY-MM).
3. Agregasi metrik (total alert, status penanganan) per perusahaan dan periode.
4. Load hasil agregasi ke tabel fakta fact_alert_operasional di DWH.
====================================================================================================
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# KONSTANTA
# ─────────────────────────────────────────────────────────────

MONGO_URI    = "mongodb://mongodb:27017/"
MONGO_DB     = "sawit_alerts"
MONGO_COL    = "log_alert_harian"

# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS (Menggunakan PythonOperator & XCom)
# ─────────────────────────────────────────────────────────────

def extract_mongodb(ti, **kwargs):
    """Mengambil data mentah dari MongoDB."""
    try:
        from pymongo import MongoClient
    except ImportError as e:
        raise ImportError("pymongo tidak terinstall.") from e

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    col    = client[MONGO_DB][MONGO_COL]

    raw_data = []
    for doc in col.find({}, {
        "perusahaan_id": 1, "tanggal": 1,
        "jenis_alert": 1, "status_penanganan": 1, "_id": 0
    }):
        raw_data.append(doc)

    client.close()
    log.info("Berhasil ekstrak %d dokumen dari MongoDB.", len(raw_data))
    
    ti.xcom_push(key="raw_data", value=raw_data)


def transform_clean_data(ti, **kwargs):
    """Pembersihan data dan ekstraksi periode (YYYY-MM)."""
    raw_data = ti.xcom_pull(task_ids="extract_mongodb", key="raw_data")
    
    cleaned_data = []
    for doc in raw_data:
        pid = doc.get("perusahaan_id", "")
        tanggal = str(doc.get("tanggal", ""))

        # Ekstrak YYYY-MM dan buang data yang tidak valid
        if len(tanggal) >= 7:
            periode = tanggal[:7]
            cleaned_data.append({
                "perusahaan_id": pid,
                "periode": periode,
                "jenis_alert": doc.get("jenis_alert", "unknown"),
                "status_penanganan": str(doc.get("status_penanganan", "")).lower()
            })
    
    log.info("Pembersihan selesai. Sisa data valid: %d", len(cleaned_data))
    ti.xcom_push(key="cleaned_data", value=cleaned_data)


def transform_aggregate_metrics(ti, **kwargs):
    """Menghitung total alert, status penanganan, & jenis terbanyak."""
    cleaned_data = ti.xcom_pull(task_ids="transform_clean_data", key="cleaned_data")
    
    agg: dict[tuple, dict] = {}

    # Proses Agregasi
    for row in cleaned_data:
        key = (row["perusahaan_id"], row["periode"])
        if key not in agg:
            agg[key] = {"total": 0, "ditangani": 0, "tidak": 0, "jenis": []}

        agg[key]["total"] += 1
        if row["status_penanganan"] == "sudah_ditangani":
            agg[key]["ditangani"] += 1
        else:
            agg[key]["tidak"] += 1
        
        agg[key]["jenis"].append(row["jenis_alert"])

    # Format output akhir
    transformed_rows = []
    for (pid, periode), data in agg.items():
        jenis_counter  = Counter(data["jenis"])
        jenis_terbanyak = jenis_counter.most_common(1)[0][0] if jenis_counter else None

        transformed_rows.append({
            "perusahaan_id"        : pid,
            "periode"              : periode,
            "total_alert"          : data["total"],
            "alert_ditangani"      : data["ditangani"],
            "alert_tidak_ditangani": data["tidak"],
            "jenis_alert_terbanyak": jenis_terbanyak,
        })

    log.info("Agregasi selesai. Menghasilkan %d baris agregasi.", len(transformed_rows))
    ti.xcom_push(key="aggregated_data", value=transformed_rows)


def load_fact_alert_operasional(ti, **kwargs):
    """Loading data agregasi ke fact_alert_operasional DWH."""
    transformed_rows = ti.xcom_pull(task_ids="transform_aggregate_metrics", key="aggregated_data")
    
    if not transformed_rows:
        log.info("Tidak ada data untuk di-load.")
        return

    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    sql = """
        INSERT INTO fact_alert_operasional
            (perusahaan_id, periode, total_alert,
             alert_ditangani, alert_tidak_ditangani, jenis_alert_terbanyak)
        VALUES (%(perusahaan_id)s, %(periode)s, %(total_alert)s,
                %(alert_ditangani)s, %(alert_tidak_ditangani)s, %(jenis_alert_terbanyak)s)
        ON CONFLICT (perusahaan_id, periode) DO UPDATE SET
            total_alert            = EXCLUDED.total_alert,
            alert_ditangani        = EXCLUDED.alert_ditangani,
            alert_tidak_ditangani  = EXCLUDED.alert_tidak_ditangani,
            jenis_alert_terbanyak  = EXCLUDED.jenis_alert_terbanyak
    """
    conn = dwh.get_conn()
    cur = conn.cursor()
    cur.executemany(sql, transformed_rows)
    conn.commit()
    log.info("Berhasil menyimpan %d baris ke tabel fact_alert_operasional.", len(transformed_rows))


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner"            : "airflow",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id      = "dag5_alert_etl",
    description = "ETL MongoDB log_alert_harian → fact_alert_operasional (agregasi bulanan per perusahaan)",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["etl", "alert"],
) as dag:

    t1 = PythonOperator(
        task_id         = "extract_mongodb",
        python_callable = extract_mongodb,
    )

    t2 = PythonOperator(
        task_id         = "transform_clean_data",
        python_callable = transform_clean_data,
    )

    t3 = PythonOperator(
        task_id         = "transform_aggregate_metrics",
        python_callable = transform_aggregate_metrics,
    )

    t4 = PythonOperator(
        task_id         = "load_fact_alert_operasional",
        python_callable = load_fact_alert_operasional,
    )

    t1 >> t2 >> t3 >> t4
