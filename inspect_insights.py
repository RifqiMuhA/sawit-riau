import pandas as pd
import psycopg2

DWH_CONFIG = {
    "host": "localhost",
    "port": "5437",
    "user": "dwh",
    "password": "dwh",
    "database": "sawit_dwh",
}

def run_query(sql):
    with psycopg2.connect(**DWH_CONFIG) as conn:
        return pd.read_sql_query(sql, conn)

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

# ── 4.1 NDVI / Kondisi Kebun ─────────────────────────────────
section("4.1 NDVI - Kondisi Kebun (fact_ndvi: kode_wilayah, periode)")

print("\n[STATUS KEBUN RINGKASAN - SEMUA PERIODE]")
print(run_query("""
    SELECT status_kebun, COUNT(*) AS jumlah_record
    FROM fact_ndvi
    GROUP BY status_kebun ORDER BY jumlah_record DESC
"""))

print("\n[NDVI PER KABUPATEN - LATEST PERIODE]")
print(run_query("""
    SELECT k.nama_kabupaten, f.status_kebun, ROUND(f.ndvi_mean::numeric, 4) AS ndvi_mean
    FROM fact_ndvi f
    JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
    WHERE f.periode = (SELECT MAX(periode) FROM fact_ndvi)
    ORDER BY k.nama_kabupaten, f.status_kebun
"""))

print("\n[TREN STATUS NDVI PER PERIODE]")
print(run_query("""
    SELECT periode, status_kebun, COUNT(*) AS n, ROUND(AVG(ndvi_mean)::numeric, 4) AS avg_ndvi
    FROM fact_ndvi
    GROUP BY periode, status_kebun
    ORDER BY periode, status_kebun
"""))

print("\n[KABUPATEN STATUS KRITIS - LATEST]")
print(run_query("""
    SELECT k.nama_kabupaten, ROUND(f.ndvi_mean::numeric, 4) AS ndvi_mean, f.status_kebun, f.periode
    FROM fact_ndvi f
    JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
    WHERE f.status_kebun = 'kritis'
    AND f.periode = (SELECT MAX(periode) FROM fact_ndvi)
    ORDER BY f.ndvi_mean ASC
"""))

# ── 4.2 Produktivitas / K-Means ──────────────────────────────
section("4.2 Produktivitas K-Means (fact_produksi: cluster_produksi)")

print("\n[DISTRIBUSI CLUSTER - SEMUA PERIODE]")
print(run_query("""
    SELECT cluster_produksi, COUNT(*) AS jumlah, 
           ROUND(AVG(produktivitas)::numeric, 3) AS avg_produktivitas,
           ROUND(MIN(produktivitas)::numeric, 3) AS min_prod,
           ROUND(MAX(produktivitas)::numeric, 3) AS max_prod
    FROM fact_produksi
    WHERE cluster_produksi IS NOT NULL
    GROUP BY cluster_produksi ORDER BY avg_produktivitas DESC
"""))

print("\n[PERUSAHAAN PER CLUSTER - LATEST]")
print(run_query("""
    SELECT p.nama_perusahaan, k.nama_kabupaten, f.cluster_produksi, 
           ROUND(f.produktivitas::numeric, 3) AS produktivitas,
           f.produksi_tbs_ton, f.periode
    FROM fact_produksi f
    JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
    JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
    WHERE f.periode = (SELECT MAX(periode) FROM fact_produksi)
    ORDER BY f.cluster_produksi, f.produktivitas DESC
"""))

print("\n[TREN CLUSTER PER PERIODE - semua perusahaan]")
print(run_query("""
    SELECT f.periode, p.nama_perusahaan, f.cluster_produksi, ROUND(f.produktivitas::numeric, 3) AS produktivitas
    FROM fact_produksi f
    JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
    ORDER BY f.periode, p.nama_perusahaan
"""))

# ── 4.3 Penimbunan ───────────────────────────────────────────
section("4.3 Deteksi Penimbunan (fact_operasional)")

print("\n[RINGKASAN PENIMBUNAN]")
print(run_query("""
    SELECT 
        COUNT(*) AS total_records,
        SUM(CASE WHEN indikasi_timbun THEN 1 ELSE 0 END) AS total_timbun,
        ROUND(100.0 * SUM(CASE WHEN indikasi_timbun THEN 1 ELSE 0 END) / COUNT(*)::numeric, 1) AS pct_timbun
    FROM fact_operasional
"""))

print("\n[DETAIL PENIMBUNAN PER PERUSAHAAN]")
print(run_query("""
    SELECT p.nama_perusahaan, 
           COUNT(*) AS total_periode,
           SUM(CASE WHEN fo.indikasi_timbun THEN 1 ELSE 0 END) AS timbun_count,
           ROUND(AVG(fo.volume_penjualan_ton)::numeric, 1) AS avg_penjualan,
           ROUND(AVG(fo.stok_akhir_ton)::numeric, 1) AS avg_stok
    FROM fact_operasional fo
    JOIN dim_perusahaan p ON fo.perusahaan_id = p.perusahaan_id
    GROUP BY p.nama_perusahaan
    ORDER BY timbun_count DESC
"""))

print("\n[PERIODE PENIMBUNAN TERJADI]")
print(run_query("""
    SELECT fo.periode, p.nama_perusahaan, fo.stok_akhir_ton, fo.volume_penjualan_ton, fo.rata_rata_historis_ton
    FROM fact_operasional fo
    JOIN dim_perusahaan p ON fo.perusahaan_id = p.perusahaan_id
    WHERE fo.indikasi_timbun = TRUE
    ORDER BY fo.periode, p.nama_perusahaan
"""))

print("\n[HARGA CPO TREN]")
print(run_query("""
    SELECT periode, harga_cpo FROM dim_periode
    WHERE harga_cpo IS NOT NULL ORDER BY periode
"""))

# ── 4.4 Realisasi vs Target Panen ────────────────────────────
section("4.4 Realisasi vs Target (fact_panen: kebun_id, periode)")

print("\n[SCHEMA FACT_PANEN CHECK - RINGKASAN]")
print(run_query("""
    SELECT 
        COUNT(*) AS total_records,
        SUM(CASE WHEN status_id = 'selesai' THEN 1 ELSE 0 END) AS selesai,
        SUM(CASE WHEN status_id = 'tertunda' THEN 1 ELSE 0 END) AS tertunda,
        SUM(CASE WHEN status_id = 'batal' THEN 1 ELSE 0 END) AS batal,
        ROUND(AVG(gap_persen)::numeric, 2) AS avg_gap_pct,
        ROUND(MIN(gap_persen)::numeric, 2) AS min_gap,
        ROUND(MAX(gap_persen)::numeric, 2) AS max_gap
    FROM fact_panen
"""))

print("\n[GAP PER PERUSAHAAN]")
print(run_query("""
    SELECT p.nama_perusahaan,
           COUNT(*) AS total_blok,
           SUM(CASE WHEN fp.status_id = 'selesai' THEN 1 ELSE 0 END) AS selesai,
           SUM(CASE WHEN fp.status_id = 'tertunda' THEN 1 ELSE 0 END) AS tertunda,
           ROUND(AVG(fp.gap_persen)::numeric, 2) AS avg_gap_pct,
           ROUND(SUM(fp.realisasi_panen_ton)::numeric, 1) AS total_realisasi,
           ROUND(SUM(fp.target_panen_ton)::numeric, 1) AS total_target
    FROM fact_panen fp
    JOIN dim_perusahaan p ON fp.perusahaan_id = p.perusahaan_id
    GROUP BY p.nama_perusahaan
    ORDER BY avg_gap_pct ASC
"""))

print("\n[GAP PER VARIETAS]")
print(run_query("""
    SELECT COALESCE(v.nama_varietas, 'Tidak Diketahui') AS varietas,
           COUNT(*) AS n,
           ROUND(AVG(fp.gap_persen)::numeric, 2) AS avg_gap,
           SUM(CASE WHEN fp.status_id = 'selesai' THEN 1 ELSE 0 END) AS selesai
    FROM fact_panen fp
    JOIN dim_kebun k ON fp.kebun_id = k.kebun_id
    LEFT JOIN dim_varietas v ON k.varietas_id = v.varietas_id
    GROUP BY v.nama_varietas
    ORDER BY avg_gap ASC
"""))

print("\n[WORST 10 BLOK - GAP TERBURUK (tertunda)]")
print(run_query("""
    SELECT k.nama_kebun, p.nama_perusahaan, fp.periode, fp.gap_persen, fp.target_panen_ton, fp.realisasi_panen_ton
    FROM fact_panen fp
    JOIN dim_kebun k ON fp.kebun_id = k.kebun_id
    JOIN dim_perusahaan p ON fp.perusahaan_id = p.perusahaan_id
    WHERE fp.status_id = 'tertunda'
    ORDER BY fp.gap_persen ASC
    LIMIT 10
"""))

# ── 4.5 Reporting Umum ───────────────────────────────────────
section("4.5 Reporting Perusahaan Sawit Riau")

print("\n[GAMBARAN UMUM LAHAN]")
print(run_query("""
    SELECT status_lahan, COUNT(*) AS jumlah, ROUND(SUM(luas_ha)::numeric, 1) AS total_ha
    FROM dim_kebun GROUP BY status_lahan ORDER BY total_ha DESC
"""))

print("\n[PRODUKSI TBS PER TAHUN]")
print(run_query("""
    SELECT EXTRACT(YEAR FROM TO_DATE(periode, 'YYYY-MM')) AS tahun,
           ROUND(SUM(produksi_tbs_ton)::numeric, 1) AS total_produksi
    FROM fact_produksi
    GROUP BY tahun ORDER BY tahun
"""))

print("\n[TOP PKS PRODUKSI TOTAL]")
print(run_query("""
    SELECT p.nama_perusahaan, ROUND(SUM(f.produksi_tbs_ton)::numeric, 1) AS total_ton
    FROM fact_produksi f JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
    GROUP BY p.nama_perusahaan ORDER BY total_ton DESC
"""))

print("\n[ALERT OPERASIONAL RINGKASAN]")
print(run_query("""
    SELECT p.nama_perusahaan,
           SUM(a.total_alert) AS total,
           SUM(a.alert_ditangani) AS ditangani,
           SUM(a.alert_tidak_ditangani) AS tidak_ditangani,
           ROUND(100.0 * SUM(a.alert_ditangani) / NULLIF(SUM(a.total_alert), 0)::numeric, 1) AS pct_ditangani
    FROM fact_alert_operasional a
    JOIN dim_perusahaan p ON a.perusahaan_id = p.perusahaan_id
    GROUP BY p.nama_perusahaan
    ORDER BY total DESC
"""))

print("\n[VARIETAS TERBANYAK]")
print(run_query("""
    SELECT COALESCE(v.nama_varietas, 'Tidak Diketahui') AS varietas, COUNT(*) AS jumlah_kebun
    FROM dim_kebun k LEFT JOIN dim_varietas v ON k.varietas_id = v.varietas_id
    GROUP BY varietas ORDER BY jumlah_kebun DESC
"""))

print("\nDone!")
