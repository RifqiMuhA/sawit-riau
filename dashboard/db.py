"""
db.py — Koneksi Pool ke sawit_dwh (PostGIS)
Semua halaman dashboard import dari sini.
"""

import os
import json
import pandas as pd
import psycopg2

DWH_CONFIG = {
    "host":     os.getenv("DWH_HOST",     "localhost"),
    "port":     os.getenv("DWH_PORT",     "5437"),
    "dbname":   os.getenv("DWH_DB",       "sawit_dwh"),
    "user":     os.getenv("DWH_USER",     "dwh"),
    "password": os.getenv("DWH_PASSWORD", "dwh"),
}


def run_query(sql: str, params=None) -> pd.DataFrame:
    """Jalankan SQL dan kembalikan hasilnya sebagai DataFrame."""
    with psycopg2.connect(**DWH_CONFIG) as conn:
        return pd.read_sql_query(sql, conn, params=params)


def get_riau_geojson() -> dict:
    """
    Ambil GeoJSON seluruh 12 kabupaten Riau dari PostGIS.
    Digunakan oleh halaman Analytics - Kondisi Kebun.
    """
    sql = """
        SELECT
            kode_wilayah,
            nama_kabupaten,
            ST_AsGeoJSON(geometry)::json AS geom
        FROM dim_kabupaten
        WHERE geometry IS NOT NULL
        ORDER BY kode_wilayah
    """
    df = run_query(sql)
    features = []
    for _, row in df.iterrows():
        features.append({
            "type": "Feature",
            "id":   row["kode_wilayah"],
            "properties": {
                "kode_wilayah":  row["kode_wilayah"],
                "nama_kabupaten": row["nama_kabupaten"],
            },
            "geometry": row["geom"],
        })
    return {"type": "FeatureCollection", "features": features}


def get_periode_options() -> list[dict]:
    """Kembalikan list dropdown options untuk filter periode."""
    df = run_query(
        "SELECT DISTINCT periode FROM dim_periode "
        "WHERE tahun >= 2023 ORDER BY periode"
    )
    return [{"label": p, "value": p} for p in df["periode"].tolist()]


def get_perusahaan_options() -> list[dict]:
    """Kembalikan list dropdown options untuk filter perusahaan."""
    df = run_query(
        "SELECT perusahaan_id, nama_perusahaan FROM dim_perusahaan ORDER BY nama_perusahaan"
    )
    opts = [{"label": "Semua Perusahaan", "value": "ALL"}]
    opts += [
        {"label": row["nama_perusahaan"], "value": row["perusahaan_id"]}
        for _, row in df.iterrows()
    ]
    return opts


def get_kabupaten_options() -> list[dict]:
    """Kembalikan list dropdown options untuk filter kabupaten."""
    df = run_query(
        "SELECT kode_wilayah, nama_kabupaten FROM dim_kabupaten ORDER BY nama_kabupaten"
    )
    opts = [{"label": "Semua Kabupaten", "value": "ALL"}]
    opts += [
        {"label": row["nama_kabupaten"], "value": row["kode_wilayah"]}
        for _, row in df.iterrows()
    ]
    return opts
