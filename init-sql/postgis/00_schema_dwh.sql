-- ══════════════════════════════════════════════════════════════
-- DDL Data Warehouse — sawit_dwh (PostGIS)
-- Sistem Monitoring Perkebunan Sawit Riau
-- Dijalankan otomatis Docker saat postgis container pertama start
-- ══════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS postgis;

-- ─────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ─────────────────────────────────────────────────────────────

-- dim_kabupaten: 12 wilayah Riau + polygon (dari GeoJSON, load terpisah)
CREATE TABLE IF NOT EXISTS dim_kabupaten (
    kode_wilayah    VARCHAR(10)                    PRIMARY KEY,
    nama_kabupaten  VARCHAR(100)                   NOT NULL,
    geometry        GEOMETRY(MULTIPOLYGON, 4326)   -- diisi via GeoJSON load, nullable sampai tersedia
);

-- Seed 12 kabupaten/kota Riau (kode BPS, geometry menyusul)
INSERT INTO dim_kabupaten (kode_wilayah, nama_kabupaten) VALUES
    ('1401', 'Kab. Kuantan Singingi'),
    ('1402', 'Kab. Indragiri Hulu'),
    ('1403', 'Kab. Indragiri Hilir'),
    ('1404', 'Kab. Pelalawan'),
    ('1405', 'Kab. Siak'),
    ('1406', 'Kab. Kampar'),
    ('1407', 'Kab. Rokan Hulu'),
    ('1408', 'Kab. Bengkalis'),
    ('1409', 'Kab. Rokan Hilir'),
    ('1410', 'Kab. Kepulauan Meranti'),
    ('1471', 'Kota Pekanbaru'),
    ('1472', 'Kota Dumai')
ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_dim_kabupaten_geom
    ON dim_kabupaten USING GIST (geometry);

-- ──

-- map_kabupaten_alias: variasi nama dari sumber berbeda → kode standar
-- Dipakai setiap DAG sebelum data masuk fact table
CREATE TABLE IF NOT EXISTS map_kabupaten_alias (
    nama_alias   VARCHAR(100)  PRIMARY KEY,
    kode_wilayah VARCHAR(10)   NOT NULL REFERENCES dim_kabupaten(kode_wilayah),
    sumber       VARCHAR(50)   -- 'excel_pks' / 'mysql' / 'postgres'
);

INSERT INTO map_kabupaten_alias (nama_alias, kode_wilayah, sumber) VALUES
    -- Kuantan Singingi
    ('Kuantan Singingi',      '1401', 'excel_pks'),
    ('Kab. Kuantan Singingi', '1401', 'excel_pks'),
    ('KUANTAN SINGINGI',      '1401', 'excel_pks'),
    ('Kuansing',              '1401', 'excel_pks'),
    -- Indragiri Hulu
    ('Indragiri Hulu',        '1402', 'excel_pks'),
    ('Kab. Indragiri Hulu',   '1402', 'excel_pks'),
    ('INDRAGIRI HULU',        '1402', 'excel_pks'),
    ('Inhu',                  '1402', 'excel_pks'),
    ('INHU',                  '1402', 'excel_pks'),
    -- Indragiri Hilir
    ('Indragiri Hilir',       '1403', 'excel_pks'),
    ('Kab. Indragiri Hilir',  '1403', 'excel_pks'),
    ('INDRAGIRI HILIR',       '1403', 'excel_pks'),
    ('Inhil',                 '1403', 'excel_pks'),
    ('INHIL',                 '1403', 'excel_pks'),
    -- Pelalawan
    ('Pelalawan',             '1404', 'excel_pks'),
    ('Kab. Pelalawan',        '1404', 'excel_pks'),
    ('PELALAWAN',             '1404', 'excel_pks'),
    -- Siak
    ('Siak',                  '1405', 'excel_pks'),
    ('Kab. Siak',             '1405', 'excel_pks'),
    ('SIAK',                  '1405', 'excel_pks'),
    -- Kampar
    ('Kampar',                '1406', 'excel_pks'),
    ('Kab. Kampar',           '1406', 'excel_pks'),
    ('KAMPAR',                '1406', 'excel_pks'),
    ('Kampar Regency',        '1406', 'excel_pks'),
    -- Rokan Hulu
    ('Rokan Hulu',            '1407', 'excel_pks'),
    ('Kab. Rokan Hulu',       '1407', 'excel_pks'),
    ('ROKAN HULU',            '1407', 'excel_pks'),
    ('Rohul',                 '1407', 'excel_pks'),
    ('ROHUL',                 '1407', 'excel_pks'),
    -- Bengkalis
    ('Bengkalis',             '1408', 'excel_pks'),
    ('Kab. Bengkalis',        '1408', 'excel_pks'),
    ('BENGKALIS',             '1408', 'excel_pks'),
    -- Rokan Hilir
    ('Rokan Hilir',           '1409', 'excel_pks'),
    ('Kab. Rokan Hilir',      '1409', 'excel_pks'),
    ('ROKAN HILIR',           '1409', 'excel_pks'),
    ('Rohil',                 '1409', 'excel_pks'),
    ('ROHIL',                 '1409', 'excel_pks'),
    -- Kepulauan Meranti
    ('Kepulauan Meranti',     '1410', 'excel_pks'),
    ('Kab. Kepulauan Meranti','1410', 'excel_pks'),
    ('KEPULAUAN MERANTI',     '1410', 'excel_pks'),
    ('Meranti',               '1410', 'excel_pks'),
    ('MERANTI',               '1410', 'excel_pks'),
    -- Kota Pekanbaru
    ('Pekanbaru',             '1471', 'excel_pks'),
    ('Kota Pekanbaru',        '1471', 'excel_pks'),
    ('PEKANBARU',             '1471', 'excel_pks'),
    -- Kota Dumai
    ('Dumai',                 '1472', 'excel_pks'),
    ('Kota Dumai',            '1472', 'excel_pks'),
    ('DUMAI',                 '1472', 'excel_pks')
ON CONFLICT DO NOTHING;

-- ──

-- dim_perusahaan: 12 PKS
CREATE TABLE IF NOT EXISTS dim_perusahaan (
    perusahaan_id      VARCHAR(20)   PRIMARY KEY,
    nama_perusahaan    VARCHAR(100)  NOT NULL,
    kode_wilayah       VARCHAR(10)   REFERENCES dim_kabupaten(kode_wilayah),
    kapasitas_olah_ton NUMERIC(10,2)
);

INSERT INTO dim_perusahaan VALUES
    ('PKS-A-01', 'PT Sawit Makmur Kampar',   '1406', 55.50),
    ('PKS-A-02', 'PT Pelalawan Agro',         '1404', 48.96),
    ('PKS-A-03', 'PT Siak Palma Sejahtera',   '1405', 59.69),
    ('PKS-B-04', 'PT Inhu Lestari',           '1402', 35.50),
    ('PKS-B-05', 'PT Kuansing Mas',           '1401', 40.80),
    ('PKS-B-06', 'PT Inhil Gemilang',         '1403', 49.81),
    ('PKS-C-07', 'CV Bengkalis Sawit',        '1408', 30.00),
    ('PKS-C-08', 'PT Rohil Abadi',            '1409', 45.78),
    ('PKS-C-09', 'PT Meranti Jaya',           '1410', 33.58),
    ('PKS-D-10', 'PT Rohul Palma',            '1407', 70.00),
    ('PKS-D-11', 'PT Pekanbaru Mill',         '1471', 25.35),
    ('PKS-D-12', 'PT Dumai Indah',            '1472', 25.74)
ON CONFLICT DO NOTHING;

-- ──

-- dim_karyawan: SDM per perusahaan (diisi DAG 2)
CREATE TABLE IF NOT EXISTS dim_karyawan (
    karyawan_id   VARCHAR(30)  PRIMARY KEY,
    perusahaan_id VARCHAR(20)  REFERENCES dim_perusahaan(perusahaan_id),
    nama          VARCHAR(100) NOT NULL,
    jabatan       VARCHAR(50),
    status        VARCHAR(20)  -- 'aktif' / 'non-aktif' (setelah normalisasi ETL)
);

-- ──

-- dim_waktu: periode YYYY-MM, di-generate 2023-01 s/d 2025-12
CREATE TABLE IF NOT EXISTS dim_waktu (
    periode  CHAR(7)   PRIMARY KEY,           -- Format YYYY-MM
    tahun    SMALLINT  NOT NULL,
    bulan    SMALLINT  NOT NULL CHECK (bulan   BETWEEN 1  AND 12),
    kuartal  SMALLINT  NOT NULL CHECK (kuartal BETWEEN 1  AND 4)
);

INSERT INTO dim_waktu (periode, tahun, bulan, kuartal)
SELECT
    TO_CHAR(d, 'YYYY-MM')                        AS periode,
    EXTRACT(YEAR  FROM d)::SMALLINT              AS tahun,
    EXTRACT(MONTH FROM d)::SMALLINT              AS bulan,
    CEIL(EXTRACT(MONTH FROM d) / 3.0)::SMALLINT  AS kuartal
FROM generate_series('2023-01-01'::DATE, '2030-12-01'::DATE, INTERVAL '1 month') AS d
ON CONFLICT DO NOTHING;

-- ──

-- dim_varietas: referensi varietas kelapa sawit (Baru)
CREATE TABLE IF NOT EXISTS dim_varietas (
    varietas_id          VARCHAR(20)   PRIMARY KEY,
    nama_varietas        VARCHAR(100)  NOT NULL,
    produsen             VARCHAR(100),
    potensi_cpo          NUMERIC(4,2),  -- % CPO dari TBS
    tahan_ganoderma      VARCHAR(20),   -- 'Tinggi' / 'Sedang' / 'Rentan'
    rerata_tandan_kg     NUMERIC(5,2),  -- kg per tandan
    umur_produktif_tahun SMALLINT,
    karakteristik_yield  VARCHAR(20),   -- 'tinggi' / 'sedang' / 'rendah'
    ketahanan_cuaca      VARCHAR(20),   -- 'tahan' / 'tidak tahan'
    asal_bibit           VARCHAR(100)
);

-- Seed 3 varietas standar (dari data-perusahaan)
INSERT INTO dim_varietas (varietas_id, nama_varietas, produsen, potensi_cpo, tahan_ganoderma, rerata_tandan_kg) VALUES
    ('V-01', 'DxP PPKS 540',   'PPKS',    8.5, 'Tinggi', 16.5),
    ('V-02', 'DxP Simalungun', 'PPKS',    7.8, 'Sedang', 18.2),
    ('V-03', 'DxP Yangambi',   'Socfindo', 8.2, 'Rentan', 15.0)
ON CONFLICT DO NOTHING;

-- ──

-- dim_kebun: blok kebun fisik per perusahaan (Baru)
-- Diisi dari tabel kebun/blok_kebun/lahan/kebun_produksi di OLTP
CREATE TABLE IF NOT EXISTS dim_kebun (
    kebun_id      VARCHAR(20)   PRIMARY KEY,
    perusahaan_id VARCHAR(20)   NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    nama_kebun    VARCHAR(100)  NOT NULL,
    kode_wilayah  VARCHAR(10)   REFERENCES dim_kabupaten(kode_wilayah),
    luas_ha       NUMERIC(10,2),
    tahun_tanam   SMALLINT,
    varietas_id   VARCHAR(20)   REFERENCES dim_varietas(varietas_id),
    status_lahan  VARCHAR(20)   -- 'produktif' / 'replanting' / 'TBM'
);

CREATE INDEX IF NOT EXISTS idx_dim_kebun_perusahaan ON dim_kebun (perusahaan_id);

-- ──

-- dim_status_panen: lookup status pelaksanaan panen (Baru)
CREATE TABLE IF NOT EXISTS dim_status_panen (
    status_id    VARCHAR(20)   PRIMARY KEY,
    status_label VARCHAR(20)   NOT NULL,
    keterangan   VARCHAR(200)
);

INSERT INTO dim_status_panen VALUES
    ('selesai',  'selesai',  'Panen selesai dilaksanakan sesuai jadwal'),
    ('tertunda', 'tertunda', 'Panen belum selesai, realisasi masih 0 atau parsial'),
    ('batal',    'batal',    'Panen dibatalkan karena kondisi tertentu')
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────────────────────
-- FACT TABLES
-- ─────────────────────────────────────────────────────────────

-- fact_ndvi: Tujuan 1 — status & kondisi kebun per kabupaten per bulan
-- Sumber: DAG 1 (GEE Sentinel-2)
-- status_kebun diisi DAG 4 (analitik), bukan DAG 1
CREATE TABLE IF NOT EXISTS fact_ndvi (
    kode_wilayah VARCHAR(10)   NOT NULL REFERENCES dim_kabupaten(kode_wilayah),
    periode      CHAR(7)       NOT NULL REFERENCES dim_waktu(periode),
    ndvi_mean    NUMERIC(5,4)  NOT NULL,
    ndvi_stddev  NUMERIC(5,4),
    pixel_count  INTEGER,
    status_kebun VARCHAR(20),  -- 'normal' / 'menurun' / 'kritis' — dihitung DAG 4
    PRIMARY KEY (kode_wilayah, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_ndvi_periode ON fact_ndvi (periode);
CREATE INDEX IF NOT EXISTS idx_fact_ndvi_status  ON fact_ndvi (status_kebun);

-- ──

-- fact_produksi: Tujuan 2 — produktivitas & K-Means cluster per perusahaan
-- Dua jalur: Excel PKS (dinas) & DB operasional perusahaan
CREATE TABLE IF NOT EXISTS fact_produksi (
    perusahaan_id    VARCHAR(20)   NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    periode          CHAR(7)       NOT NULL REFERENCES dim_waktu(periode),
    kode_wilayah     VARCHAR(10)   REFERENCES dim_kabupaten(kode_wilayah),
    produksi_tbs_ton NUMERIC(12,2) NOT NULL,
    luas_panen_ha    NUMERIC(10,2) NOT NULL,
    produktivitas    NUMERIC(8,2),   -- ton/ha, dihitung ETL: produksi / luas
    cluster_produksi VARCHAR(15),    -- 'underperform'/'average'/'overperform', DAG 4 K-Means k=3
    PRIMARY KEY (perusahaan_id, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_produksi_periode ON fact_produksi (periode);
CREATE INDEX IF NOT EXISTS idx_fact_produksi_cluster ON fact_produksi (cluster_produksi);

-- ──

-- fact_operasional: Tujuan 3 — stok & penjualan untuk deteksi penimbunan
-- Sumber: DB operasional perusahaan saja (bukan Excel PKS)
CREATE TABLE IF NOT EXISTS fact_operasional (
    perusahaan_id          VARCHAR(20)   NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    periode                CHAR(7)       NOT NULL REFERENCES dim_waktu(periode),
    stok_akhir_ton         NUMERIC(12,2),          -- nullable: Perusahaan D kadang NULL
    volume_penjualan_ton   NUMERIC(12,2) NOT NULL,
    rata_rata_historis_ton NUMERIC(12,2),           -- rolling avg 3 bulan, dihitung ETL
    indikasi_timbun        BOOLEAN,                 -- TRUE jika 3 kondisi serentak terpenuhi
    stok_flag              VARCHAR(20),             -- 'missing' jika stok NULL (Perusahaan D)
    PRIMARY KEY (perusahaan_id, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_operasional_periode ON fact_operasional (periode);
CREATE INDEX IF NOT EXISTS idx_fact_operasional_timbun  ON fact_operasional (indikasi_timbun);

-- ──

-- fact_harga_cpo: sinyal pasar untuk Tujuan 3
-- Sumber: DAG 3, scraping PDF Disbun Riau per tahun (retrospektif, bukan real-time)
CREATE TABLE IF NOT EXISTS fact_harga_cpo (
    periode   CHAR(7)        PRIMARY KEY REFERENCES dim_waktu(periode),
    harga_cpo NUMERIC(10,2)  NOT NULL    -- Rp/kg, rata-rata bulanan CPO plasma Disbun Riau
);

-- ──

-- fact_panen: Tujuan 4 — realisasi vs target panen per kebun (Baru)
-- Sumber: DAG 5 (tabel jadwal_panen/rencana_panen/target_panen/realisasi_panen di OLTP)
CREATE TABLE IF NOT EXISTS fact_panen (
    kebun_id             VARCHAR(20)   NOT NULL REFERENCES dim_kebun(kebun_id),
    periode              CHAR(7)       NOT NULL REFERENCES dim_waktu(periode),
    perusahaan_id        VARCHAR(20)   NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    target_panen_ton     NUMERIC(12,2) NOT NULL,
    realisasi_panen_ton  NUMERIC(12,2),            -- NULL jika belum selesai
    gap_persen           NUMERIC(8,2),             -- (realisasi - target) / target * 100, dihitung ETL
    status_id            VARCHAR(20)   REFERENCES dim_status_panen(status_id),
    PRIMARY KEY (kebun_id, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_panen_perusahaan ON fact_panen (perusahaan_id);
CREATE INDEX IF NOT EXISTS idx_fact_panen_periode    ON fact_panen (periode);
CREATE INDEX IF NOT EXISTS idx_fact_panen_status     ON fact_panen (status_id);

-- ──

-- fact_rendemen: agregat bulanan dari MongoDB log_alert_harian (Baru)
-- Sumber: DAG 6 (MongoDB per perusahaan)
CREATE TABLE IF NOT EXISTS fact_rendemen (
    perusahaan_id         VARCHAR(20)  NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    periode               CHAR(7)      NOT NULL REFERENCES dim_waktu(periode),
    total_alert           INTEGER      NOT NULL,
    alert_ditangani       INTEGER,
    alert_tidak_ditangani INTEGER,
    jenis_alert_terbanyak VARCHAR(50),
    PRIMARY KEY (perusahaan_id, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_rendemen_periode ON fact_rendemen (periode);
