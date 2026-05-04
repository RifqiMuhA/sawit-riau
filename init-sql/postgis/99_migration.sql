-- ══════════════════════════════════════════════════════════════
-- MIGRATION SCRIPT — sawit_dwh
-- Jalankan di database yang sudah berjalan tanpa reset volume
-- ══════════════════════════════════════════════════════════════

-- ── 1. Rename dim_waktu → dim_periode ────────────────────────
ALTER TABLE IF EXISTS dim_waktu RENAME TO dim_periode;

-- Rename index terkait
ALTER INDEX IF EXISTS idx_dim_waktu_pkey RENAME TO dim_periode_pkey;

-- Tambah kolom harga_cpo (nullable, diisi DAG 3)
ALTER TABLE dim_periode ADD COLUMN IF NOT EXISTS harga_cpo NUMERIC(10,2);

-- ── 2. Hapus ndvi_stddev dari fact_ndvi ──────────────────────
ALTER TABLE fact_ndvi DROP COLUMN IF EXISTS ndvi_stddev;

-- ── 3. Hapus fact_harga_cpo ──────────────────────────────────
-- Perlu drop FK di fact_harga_cpo dulu jika ada
DROP TABLE IF EXISTS fact_harga_cpo CASCADE;

-- ── 4. Rename fact_rendemen → fact_alert_operasional ─────────
ALTER TABLE IF EXISTS fact_rendemen RENAME TO fact_alert_operasional;

-- Rename index terkait
ALTER INDEX IF EXISTS idx_fact_rendemen_periode RENAME TO idx_fact_alert_periode;

-- Tambah index baru untuk perusahaan_id jika belum ada
CREATE INDEX IF NOT EXISTS idx_fact_alert_perusahaan ON fact_alert_operasional (perusahaan_id);

-- ── 5. Tambah dim_karyawan index (jika belum ada) ────────────
CREATE INDEX IF NOT EXISTS idx_dim_karyawan_perusahaan ON dim_karyawan (perusahaan_id);

-- ── 6. Buat fact_tenaga_kerja (baru) ─────────────────────────
CREATE TABLE IF NOT EXISTS fact_tenaga_kerja (
    perusahaan_id      VARCHAR(20)  NOT NULL REFERENCES dim_perusahaan(perusahaan_id),
    periode            CHAR(7)      NOT NULL REFERENCES dim_periode(periode),
    total_karyawan     INTEGER      NOT NULL,
    karyawan_aktif     INTEGER,
    karyawan_non_aktif INTEGER,
    PRIMARY KEY (perusahaan_id, periode)
);

CREATE INDEX IF NOT EXISTS idx_fact_tenaga_periode    ON fact_tenaga_kerja (periode);
CREATE INDEX IF NOT EXISTS idx_fact_tenaga_perusahaan ON fact_tenaga_kerja (perusahaan_id);

-- ── 7. Lengkapi seed dim_varietas (kolom yang sebelumnya NULL) ─
UPDATE dim_varietas SET
    umur_produktif_tahun = 25,
    karakteristik_yield  = 'tinggi',
    ketahanan_cuaca      = 'tahan',
    asal_bibit           = 'Indonesia (PPKS, Marihat)'
WHERE varietas_id = 'V-01';

UPDATE dim_varietas SET
    umur_produktif_tahun = 25,
    karakteristik_yield  = 'sedang',
    ketahanan_cuaca      = 'tahan',
    asal_bibit           = 'Indonesia (PPKS, Simalungun)'
WHERE varietas_id = 'V-02';

UPDATE dim_varietas SET
    umur_produktif_tahun = 22,
    karakteristik_yield  = 'sedang',
    ketahanan_cuaca      = 'tidak tahan',
    asal_bibit           = 'Kongo / Socfindo'
WHERE varietas_id = 'V-03';

-- ── Verifikasi ────────────────────────────────────────────────
SELECT 'dim_periode'           AS tabel, COUNT(*) AS rows FROM dim_periode
UNION ALL
SELECT 'fact_ndvi'             , COUNT(*) FROM fact_ndvi
UNION ALL
SELECT 'fact_produksi'         , COUNT(*) FROM fact_produksi
UNION ALL
SELECT 'fact_operasional'      , COUNT(*) FROM fact_operasional
UNION ALL
SELECT 'fact_panen'            , COUNT(*) FROM fact_panen
UNION ALL
SELECT 'fact_alert_operasional', COUNT(*) FROM fact_alert_operasional
UNION ALL
SELECT 'fact_tenaga_kerja'     , COUNT(*) FROM fact_tenaga_kerja
UNION ALL
SELECT 'dim_karyawan'          , COUNT(*) FROM dim_karyawan
UNION ALL
SELECT 'dim_varietas'          , COUNT(*) FROM dim_varietas;
