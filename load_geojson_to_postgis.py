"""
load_geojson_to_postgis.py
==========================
Update dim_kabupaten.geometry di PostGIS DWH
menggunakan file geojson/kabkota_riau.geojson.

Dijalankan SEKALI dari host, sebelum DAG berjalan.
Syarat: psycopg2 terinstall di Python host
  pip install psycopg2-binary
"""

import json
import sys

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 belum terinstall. Jalankan: pip install psycopg2-binary")
    sys.exit(1)

GEOJSON_PATH = "geojson/kabkota_riau.geojson"
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5433,       # PostGIS DWH
    "dbname"  : "sawit_dwh",
    "user"    : "dwh",
    "password": "dwh",
}

def main():
    # 1. Load GeoJSON
    print(f"Membaca {GEOJSON_PATH}...")
    with open(GEOJSON_PATH, encoding="utf-8") as f:
        geojson = json.load(f)

    features = geojson["features"]
    print(f"  -> {len(features)} features ditemukan")

    # 2. Connect ke PostGIS
    print("Connecting ke PostGIS DWH...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # 3. Update geometry per kode_wilayah
    updated = 0
    skipped = 0
    for feat in features:
        kode     = feat["properties"].get("kode_wilayah")
        geom_str = json.dumps(feat["geometry"])

        if not kode:
            print(f"  [SKIP] Feature tanpa kode_wilayah: {feat['properties']}")
            skipped += 1
            continue

        cur.execute("""
            UPDATE dim_kabupaten
            SET    geometry = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
            WHERE  kode_wilayah = %s
            RETURNING kode_wilayah, nama_kabupaten
        """, (geom_str, kode))

        row = cur.fetchone()
        if row:
            print(f"  [OK] Updated: {row[0]} - {row[1]}")
            updated += 1
        else:
            print(f"  [WARN] Kode {kode} tidak ditemukan di dim_kabupaten, INSERT baru...")
            # Coba insert kalau kode tidak ada di DWH (fallback)
            cur.execute("""
                INSERT INTO dim_kabupaten (kode_wilayah, nama_kabupaten, geometry)
                VALUES (%s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                ON CONFLICT (kode_wilayah) DO UPDATE
                SET geometry = EXCLUDED.geometry
            """, (kode, feat["properties"].get("shapeName", kode), geom_str))
            updated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nSelesai: {updated} geometry di-update, {skipped} dilewati.")

    # 4. Verifikasi
    print("\nVerifikasi dim_kabupaten.geometry...")
    conn2 = psycopg2.connect(**DB_CONFIG)
    cur2  = conn2.cursor()
    cur2.execute("""
        SELECT kode_wilayah, nama_kabupaten,
               CASE WHEN geometry IS NOT NULL THEN 'OK' ELSE 'NULL' END as geom_status,
               ST_AsText(ST_Centroid(geometry)) as centroid
        FROM dim_kabupaten
        ORDER BY kode_wilayah
    """)
    rows = cur2.fetchall()
    print(f"\n{'Kode':<8} {'Kabupaten':<25} {'Geom':<6} {'Centroid (lon, lat)'}")
    print("-" * 80)
    for r in rows:
        kode, nama, status, centroid = r
        centroid_str = centroid if centroid else "—"
        print(f"{kode:<8} {nama:<25} {status:<6} {centroid_str}")
    cur2.close()
    conn2.close()

if __name__ == "__main__":
    main()
