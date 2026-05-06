"""
====================================================================================================
DAG ID          : dag8_minio_backup
Deskripsi       : Pencadangan (Backup) database Data Warehouse PostGIS ke MinIO.
Jadwal          : Bulanan (@monthly)
Sumber Data     : PostGIS (sawit_dwh)
Target Data     : MinIO Bucket (dwh-backups)
====================================================================================================
Alur Proses:
1. Menunggu penyelesaian DAG Datamart Refresh.
2. Dump seluruh database PostGIS ke file terkompresi (.sql.gz).
3. Unggah file backup ke storage MinIO.
4. Hapus file lokal sementara dan lakukan rotasi backup di MinIO (> 6 bulan).
====================================================================================================
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

# Konfigurasi Koneksi (diambil dari env docker-compose)
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_ACCESS_KEY  = os.environ.get("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY  = os.environ.get("MINIO_ROOT_PASSWORD", "minio123")
BUCKET_NAME       = "dwh-backups"

DWH_HOST     = "postgis_db"
DWH_USER     = os.environ.get("DWH_USER", "dwh")
DWH_PASSWORD = os.environ.get("DWH_PASSWORD", "dwh")
DWH_DB       = "sawit_dwh"

SENSOR_TIMEOUT_SECONDS  = 60 * 60 * 6
SENSOR_POKE_INTERVAL    = 60

def _check_latest_run(external_dag_id: str, **kwargs) -> bool:
    """Mengecek suksesnya eksekusi terakhir dari DAG tertentu."""
    runs = DagRun.find(dag_id=external_dag_id)
    if not runs:
        return False
    latest_run = sorted(runs, key=lambda x: x.execution_date, reverse=True)[0]
    return latest_run.state == DagRunState.SUCCESS

def _on_sensor_timeout(context):
    dag_id = context["task"].op_kwargs["external_dag_id"]
    print(f"[ALERT] Sensor timeout: {dag_id} tidak selesai dalam batas waktu.")

def _upload_to_minio(**context):
    """Mengunggah file backup lokal ke bucket MinIO."""
    logical_date = context["logical_date"].strftime("%Y%m%d_%H%M%S")
    local_file = f"/tmp/backup_dwh_{logical_date}.sql.gz"
    s3_key = f"backup_dwh_{logical_date}.sql.gz"
    
    # Inisialisasi client boto3 (kompatibel dengan MinIO)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4")
    )
    
    # Buat bucket jika belum ada
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        print(f"Menciptakan bucket baru: {BUCKET_NAME}")
        s3.create_bucket(Bucket=BUCKET_NAME)
        
    print(f"Mengunggah {local_file} ke s3://{BUCKET_NAME}/{s3_key} ...")
    s3.upload_file(local_file, BUCKET_NAME, s3_key)
    print("Upload selesai.")

def _cleanup_and_rotate(**context):
    """Menghapus file sementara dan membuang backup berusia lebih dari 6 bulan di MinIO."""
    logical_date = context["logical_date"].strftime("%Y%m%d_%H%M%S")
    local_file = f"/tmp/backup_dwh_{logical_date}.sql.gz"
    
    # 1. Hapus file lokal
    if os.path.exists(local_file):
        os.remove(local_file)
        print(f"File lokal {local_file} telah dihapus.")
        
    # 2. Rotasi MinIO (> 6 bulan)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4")
    )
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=180) # ~6 bulan
    print(f"Mencari file backup lebih lama dari {cutoff_date}...")
    
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff_date:
                print(f"[ROTASI] Menghapus backup usang: {obj['Key']} (Modifikasi: {obj['LastModified']})")
                s3.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
    print("Proses pembersihan selesai.")


default_args = {
    "owner"            : "airflow",
    "retries"          : 1,
    "retry_delay"      : timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id      = "dag8_minio_backup",
    description = "Backup DWH PostGIS bulanan dan upload ke MinIO",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["backup"],
) as dag:

    wait_for_dag7 = PythonSensor(
        task_id          = "wait_for_dag7",
        python_callable  = _check_latest_run,
        op_kwargs        = {"external_dag_id": "dag7_datamart_refresh"},
        mode             = "reschedule",
        poke_interval    = SENSOR_POKE_INTERVAL,
        timeout          = SENSOR_TIMEOUT_SECONDS,
        soft_fail        = False,
        on_failure_callback = _on_sensor_timeout,
    )

    dump_command = (
        "set -o pipefail; pg_dump -h {{ params.host }} -U {{ params.user }} --format=plain {{ params.db }} "
        "| gzip > /tmp/backup_dwh_{{ logical_date.strftime('%Y%m%d_%H%M%S') }}.sql.gz"
    )

    pg_dump_task = BashOperator(
        task_id="pg_dump_dwh",
        bash_command=dump_command,
        params={
            "host": DWH_HOST,
            "user": DWH_USER,
            "db": DWH_DB,
        },
        env={"PGPASSWORD": DWH_PASSWORD, "PATH": os.environ.get("PATH", "/usr/bin:/bin")},
    )

    upload_to_minio_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=_upload_to_minio,
    )

    cleanup_and_rotate_task = PythonOperator(
        task_id="cleanup_and_rotate",
        python_callable=_cleanup_and_rotate,
    )

    # ALUR
    wait_for_dag7 >> pg_dump_task >> upload_to_minio_task >> cleanup_and_rotate_task
