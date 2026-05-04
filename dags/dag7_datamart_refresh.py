"""
DAG 7 — Datamart Refresh
========================
Membuat tabel denormalisasi siap saji (datamart) dari tabel-tabel Fact dan Dimension.
Tabel-tabel ini akan berada di schema `datamart` dan digunakan langsung oleh Dashboard.

Datamarts:
1. dm_kondisi_kebun (Tujuan 1)
2. dm_gap_produksi (Tujuan 2)
3. dm_deteksi_penimbunan (Tujuan 3)
4. dm_realisasi_panen (Tujuan 4)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.python import PythonSensor
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

def _check_latest_run(external_dag_id: str, **kwargs) -> bool:
    """
    Fungsi kustom untuk mengecek apakah eksekusi terakhir dari sebuah DAG berstatus SUCCESS.
    Ini menghilangkan masalah perbedaan mikrodetik (execution_date) saat trigger manual.
    """
    runs = DagRun.find(dag_id=external_dag_id)
    if not runs:
        return False
    # Urutkan berdasarkan waktu eksekusi paling akhir
    latest_run = sorted(runs, key=lambda x: x.execution_date, reverse=True)[0]
    return latest_run.state == DagRunState.SUCCESS

# Definisikan konstanta agar konsisten dan mudah diubah
SENSOR_TIMEOUT_SECONDS  = 60 * 60 * 6   # 6 jam — sesuaikan dengan SLA pipeline
SENSOR_POKE_INTERVAL    = 60             # cek setiap 1 menit

def _on_sensor_timeout(context):
    """Kirim notifikasi jika sensor timeout — upstream DAG tidak selesai tepat waktu."""
    dag_id   = context["task"].op_kwargs["external_dag_id"]
    task_id  = context["task_instance"].task_id
    # Ganti dengan mekanisme notifikasi yang dipakai (Slack, email, dll.)
    print(f"[ALERT] Sensor {task_id} timeout: {dag_id} tidak selesai dalam batas waktu.")


default_args = {
    "owner"            : "airflow",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id      = "dag7_datamart_refresh",
    description = "Refresh 4 tabel Datamart untuk kebutuhan Dashboard",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["datamart"],
) as dag:

    # ─────────────────────────────────────────────────────────────
    # INISIALISASI SCHEMA DATAMART
    # ─────────────────────────────────────────────────────────────
    
    init_schema = PostgresOperator(
        task_id="init_schema_datamart",
        postgres_conn_id="postgis_dwh",
        sql="CREATE SCHEMA IF NOT EXISTS datamart;",
    )

    # ─────────────────────────────────────────────────────────────
    # DATAMART 1: KONDISI KEBUN (NDVI)
    # ─────────────────────────────────────────────────────────────
    
    sql_dm_kondisi_kebun = """
        -- 1. Buat versi baru dengan nama sementara
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_kondisi_kebun_new;
        CREATE MATERIALIZED VIEW datamart.dm_kondisi_kebun_new AS
        SELECT
            f.periode,
            w.tahun,
            w.bulan,
            k.nama_kabupaten,
            f.ndvi_mean,
            f.status_kebun,
            NOW() AS last_refreshed
        FROM fact_ndvi f
        JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
        JOIN dim_periode w ON f.periode = w.periode;

        -- 2. Buat index di versi baru SEBELUM swap
        CREATE UNIQUE INDEX idx_dm_kondisi_grain_{{ ts_nodash }} 
            ON datamart.dm_kondisi_kebun_new (nama_kabupaten, periode);
        CREATE INDEX idx_dm_kondisi_periode_{{ ts_nodash }} 
            ON datamart.dm_kondisi_kebun_new (periode);
        CREATE INDEX idx_dm_kondisi_kabupaten_{{ ts_nodash }} 
            ON datamart.dm_kondisi_kebun_new (nama_kabupaten);

        -- 3. Swap atomik: rename lama -> old, baru -> aktif
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'datamart' 
                AND matviewname = 'dm_kondisi_kebun'
            ) THEN
                ALTER MATERIALIZED VIEW datamart.dm_kondisi_kebun 
                    RENAME TO dm_kondisi_kebun_old;
            END IF;
        END $$;

        ALTER MATERIALIZED VIEW datamart.dm_kondisi_kebun_new 
            RENAME TO dm_kondisi_kebun;

        -- 4. Buang yang lama
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_kondisi_kebun_old;
    """
    
    task_dm_kondisi_kebun = PostgresOperator(
        task_id="task_dm_kondisi_kebun",
        postgres_conn_id="postgis_dwh",
        sql=sql_dm_kondisi_kebun,
    )

    # ─────────────────────────────────────────────────────────────
    # DATAMART 2: GAP PRODUKSI & CLUSTER
    # ─────────────────────────────────────────────────────────────
    
    sql_dm_gap_produksi = """
        -- 1. Buat versi baru dengan nama sementara
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_gap_produksi_new;
        CREATE MATERIALIZED VIEW datamart.dm_gap_produksi_new AS
        SELECT
            f.periode,
            w.tahun, 
            w.bulan,
            p.nama_perusahaan,
            k.nama_kabupaten,
            f.produksi_tbs_ton,
            f.luas_panen_ha,
            f.produktivitas,
            f.cluster_produksi,
            NOW() AS last_refreshed
        FROM fact_produksi f
        JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
        LEFT JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
        JOIN dim_periode w ON f.periode = w.periode;
        
        -- 2. Buat index di versi baru SEBELUM swap
        CREATE UNIQUE INDEX idx_dm_gap_prod_grain_{{ ts_nodash }} 
            ON datamart.dm_gap_produksi_new (nama_perusahaan, periode);
        CREATE INDEX idx_dm_gap_prod_periode_{{ ts_nodash }} 
            ON datamart.dm_gap_produksi_new (periode);
        CREATE INDEX idx_dm_gap_prod_perusahaan_{{ ts_nodash }} 
            ON datamart.dm_gap_produksi_new (nama_perusahaan);

        -- 3. Swap atomik: rename lama -> old, baru -> aktif
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'datamart' 
                AND matviewname = 'dm_gap_produksi'
            ) THEN
                ALTER MATERIALIZED VIEW datamart.dm_gap_produksi 
                    RENAME TO dm_gap_produksi_old;
            END IF;
        END $$;

        ALTER MATERIALIZED VIEW datamart.dm_gap_produksi_new 
            RENAME TO dm_gap_produksi;

        -- 4. Buang yang lama
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_gap_produksi_old;
    """
    
    task_dm_gap_produksi = PostgresOperator(
        task_id="task_dm_gap_produksi",
        postgres_conn_id="postgis_dwh",
        sql=sql_dm_gap_produksi,
    )

    # ─────────────────────────────────────────────────────────────
    # DATAMART 3: DETEKSI PENIMBUNAN
    # ─────────────────────────────────────────────────────────────
    
    sql_dm_deteksi_penimbunan = """
        -- 1. Buat versi baru dengan nama sementara
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_deteksi_penimbunan_new;
        CREATE MATERIALIZED VIEW datamart.dm_deteksi_penimbunan_new AS
        SELECT
            f.periode,
            w.tahun, 
            w.bulan,
            p.nama_perusahaan,
            f.volume_penjualan_ton,
            f.rata_rata_historis_ton,
            f.stok_akhir_ton,
            w.harga_cpo,
            f.indikasi_timbun,
            NOW() AS last_refreshed
        FROM fact_operasional f
        JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
        JOIN dim_periode w ON f.periode = w.periode;
        
        -- 2. Buat index di versi baru SEBELUM swap
        CREATE UNIQUE INDEX idx_dm_timbun_grain_{{ ts_nodash }} 
            ON datamart.dm_deteksi_penimbunan_new (nama_perusahaan, periode);
        CREATE INDEX idx_dm_timbun_periode_{{ ts_nodash }} 
            ON datamart.dm_deteksi_penimbunan_new (periode);
        CREATE INDEX idx_dm_timbun_perusahaan_{{ ts_nodash }} 
            ON datamart.dm_deteksi_penimbunan_new (nama_perusahaan);

        -- 3. Swap atomik: rename lama -> old, baru -> aktif
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'datamart' 
                AND matviewname = 'dm_deteksi_penimbunan'
            ) THEN
                ALTER MATERIALIZED VIEW datamart.dm_deteksi_penimbunan 
                    RENAME TO dm_deteksi_penimbunan_old;
            END IF;
        END $$;

        ALTER MATERIALIZED VIEW datamart.dm_deteksi_penimbunan_new 
            RENAME TO dm_deteksi_penimbunan;

        -- 4. Buang yang lama
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_deteksi_penimbunan_old;
    """
    
    task_dm_deteksi_penimbunan = PostgresOperator(
        task_id="task_dm_deteksi_penimbunan",
        postgres_conn_id="postgis_dwh",
        sql=sql_dm_deteksi_penimbunan,
    )

    # ─────────────────────────────────────────────────────────────
    # DATAMART 4: REALISASI VS TARGET PANEN
    # ─────────────────────────────────────────────────────────────
    
    sql_dm_realisasi_panen = """
        -- 1. Buat versi baru dengan nama sementara
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_realisasi_panen_new;
        CREATE MATERIALIZED VIEW datamart.dm_realisasi_panen_new AS
        SELECT
            f.periode,
            w.tahun, 
            w.bulan,
            k.nama_kebun,
            v.nama_varietas,
            p.nama_perusahaan,
            s.status_label AS status_panen,
            f.target_panen_ton,
            f.realisasi_panen_ton,
            f.gap_persen,
            NOW() AS last_refreshed
        FROM fact_panen f
        JOIN dim_kebun k ON f.kebun_id = k.kebun_id
        LEFT JOIN dim_varietas v ON k.varietas_id = v.varietas_id
        JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
        JOIN dim_status_panen s ON f.status_id = s.status_id
        JOIN dim_periode w ON f.periode = w.periode;
        
        -- 2. Buat index di versi baru SEBELUM swap
        CREATE UNIQUE INDEX idx_dm_panen_grain_{{ ts_nodash }} 
            ON datamart.dm_realisasi_panen_new (nama_kebun, nama_perusahaan, periode);
        CREATE INDEX idx_dm_panen_periode_{{ ts_nodash }} 
            ON datamart.dm_realisasi_panen_new (periode);
        CREATE INDEX idx_dm_panen_kebun_{{ ts_nodash }} 
            ON datamart.dm_realisasi_panen_new (nama_kebun);

        -- 3. Swap atomik: rename lama -> old, baru -> aktif
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_matviews 
                WHERE schemaname = 'datamart' 
                AND matviewname = 'dm_realisasi_panen'
            ) THEN
                ALTER MATERIALIZED VIEW datamart.dm_realisasi_panen 
                    RENAME TO dm_realisasi_panen_old;
            END IF;
        END $$;

        ALTER MATERIALIZED VIEW datamart.dm_realisasi_panen_new 
            RENAME TO dm_realisasi_panen;

        -- 4. Buang yang lama
        DROP MATERIALIZED VIEW IF EXISTS datamart.dm_realisasi_panen_old;
    """
    
    task_dm_realisasi_panen = PostgresOperator(
        task_id="task_dm_realisasi_panen",
        postgres_conn_id="postgis_dwh",
        sql=sql_dm_realisasi_panen,
    )

    # ─────────────────────────────────────────────────────────────
    # EXTERNAL TASK SENSORS (Dependency Forgiving)
    # ─────────────────────────────────────────────────────────────
    
    t_sensor_4 = PythonSensor(
        task_id          = "wait_for_dag6_analitik",
        python_callable  = _check_latest_run,
        op_kwargs        = {"external_dag_id": "dag6_analitik"},
        mode             = "reschedule",
        poke_interval    = SENSOR_POKE_INTERVAL,
        timeout          = SENSOR_TIMEOUT_SECONDS,
        soft_fail        = False,
        on_failure_callback = _on_sensor_timeout,
    )

    t_sensor_5 = PythonSensor(
        task_id          = "wait_for_dag4_panen",
        python_callable  = _check_latest_run,
        op_kwargs        = {"external_dag_id": "dag4_panen_etl"},
        mode             = "reschedule",
        poke_interval    = SENSOR_POKE_INTERVAL,
        timeout          = SENSOR_TIMEOUT_SECONDS,
        soft_fail        = False,
        on_failure_callback = _on_sensor_timeout,
    )

    t_sensor_6 = PythonSensor(
        task_id          = "wait_for_dag5_alert",
        python_callable  = _check_latest_run,
        op_kwargs        = {"external_dag_id": "dag5_alert_etl"},
        mode             = "reschedule",
        poke_interval    = SENSOR_POKE_INTERVAL,
        timeout          = SENSOR_TIMEOUT_SECONDS,
        soft_fail        = False,
        on_failure_callback = _on_sensor_timeout,
    )

    # ─────────────────────────────────────────────────────────────
    # ALUR DEPENDENSI
    # ─────────────────────────────────────────────────────────────
    
    # Tunggu ketiga DAG di atas selesai, baru inisialisasi Datamart
    [t_sensor_4, t_sensor_5, t_sensor_6] >> init_schema
    
    init_schema >> [
        task_dm_kondisi_kebun,
        task_dm_gap_produksi,
        task_dm_deteksi_penimbunan,
        task_dm_realisasi_panen
    ]
