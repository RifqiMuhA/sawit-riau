-- SCHEMA POSTGRESQL: d-11-pekanbaru
CREATE SCHEMA IF NOT EXISTS "d-11-pekanbaru";
SET search_path TO "d-11-pekanbaru";

CREATE TABLE IF NOT EXISTS data_perusahaan (perusahaan_id VARCHAR(20) PRIMARY KEY, nama_perusahaan VARCHAR(100), lokasi_kabupaten VARCHAR(10), kapasitas NUMERIC(10,2));
CREATE TABLE IF NOT EXISTS data_karyawan (karyawan_id VARCHAR(30) PRIMARY KEY, perusahaan_id VARCHAR(20), nama_karyawan VARCHAR(100), jabatan VARCHAR(50), status VARCHAR(20));
CREATE TABLE IF NOT EXISTS varietas (id_varietas VARCHAR(10) PRIMARY KEY, nama_varietas VARCHAR(50), produsen VARCHAR(50), potensi_cpo DECIMAL(4,2), tahan_ganoderma VARCHAR(20), rerata_tandan_kg DECIMAL(4,2));
CREATE TABLE IF NOT EXISTS kebun_produksi (kebun_id VARCHAR(20) PRIMARY KEY, perusahaan_id VARCHAR(20), nama_kebun VARCHAR(100), lokasi_kabupaten VARCHAR(10), luas_ha NUMERIC(10,2), tahun_tanam SMALLINT, varietas_id VARCHAR(20), status VARCHAR(20));
CREATE TABLE IF NOT EXISTS produksi_bulanan (id VARCHAR(30) PRIMARY KEY, perusahaan_id VARCHAR(20), tahun SMALLINT, bulan SMALLINT, produksi_tbs_ton NUMERIC(12,2), luas_panen_ha NUMERIC(10,2), stok_akhir_ton NUMERIC(12,2), volume_penjualan NUMERIC(12,2));
CREATE TABLE IF NOT EXISTS realisasi_panen (realisasi_id VARCHAR(30) PRIMARY KEY, kebun_id VARCHAR(20), perusahaan_id VARCHAR(20), target_panen_ton NUMERIC(12,2), realisasi_panen_ton NUMERIC(12,2), tgl_mulai DATE, tgl_selesai DATE, status_panen VARCHAR(20));

INSERT INTO data_perusahaan VALUES ('PKS-D-11', 'PT Pekanbaru Mill', '1471', 20);
INSERT INTO data_karyawan VALUES
('PKS-D-11-KRY-01', 'PKS-D-11', 'Emil Laila', 'Manager', 'resign'),
('PKS-D-11-KRY-02', 'PKS-D-11', 'Aminah Siddiq', 'Manager', 'resign'),
('PKS-D-11-KRY-03', 'PKS-D-11', 'Nur Ali', 'Admin', 'resign'),
('PKS-D-11-KRY-04', 'PKS-D-11', 'Ghazali Hasan', 'Operator', 'aktif'),
('PKS-D-11-KRY-05', 'PKS-D-11', 'Aminah Abdullah', 'Mandor', 'aktif'),
('PKS-D-11-KRY-06', 'PKS-D-11', 'Ghazali Sulaiman', 'Mandor', 'pensiun'),
('PKS-D-11-KRY-07', 'PKS-D-11', 'Jamal Abdullah', 'Security', 'pensiun'),
('PKS-D-11-KRY-08', 'PKS-D-11', 'Ghazali Syafiq', 'Manager', 'pensiun'),
('PKS-D-11-KRY-09', 'PKS-D-11', 'Ihsan Hasan', 'Supervisor', 'pensiun'),
('PKS-D-11-KRY-10', 'PKS-D-11', 'Ghazali Siddiq', 'Security', 'aktif');
INSERT INTO varietas VALUES
('V-01', 'DxP PPKS 540', 'PPKS', 8.5, 'Tinggi', 16.5),
('V-02', 'DxP Simalungun', 'PPKS', 7.8, 'Sedang', 18.2),
('V-03', 'DxP Yangambi', 'Socfindo', 8.2, 'Rentan', 15.0);
INSERT INTO kebun_produksi VALUES
('KBN-11-1', 'PKS-D-11', 'Kebun Afdeling 1', '1471', 1474.82, 2012, 'V-03', 'produktif'),
('KBN-11-2', 'PKS-D-11', 'Kebun Afdeling 2', '1471', 705.36, 2013, 'V-03', 'TBM'),
('KBN-11-3', 'PKS-D-11', 'Kebun Afdeling 3', '1471', 819.82, 2010, 'V-01', 'TBM');
INSERT INTO produksi_bulanan VALUES
('ID-PKS-D-11-2301', 'PKS-D-11', 2023, 1, 5443.68, 3000, 1239.74, 4765.41),
('ID-PKS-D-11-2302', 'PKS-D-11', 2023, 2, 4412.88, 3000, 1515.19, 4137.43),
('ID-PKS-D-11-2303', 'PKS-D-11', 2023, 3, 5090.69, 3000, 1665.47, 4940.42),
('ID-PKS-D-11-2304', 'PKS-D-11', 2023, 4, 7213.78, 3000, 2248.57, 6630.67),
('ID-PKS-D-11-2305', 'PKS-D-11', 2023, 5, 5701.89, 3000, 2742.75, 5207.71),
('ID-PKS-D-11-2306', 'PKS-D-11', 2023, 6, 5917.43, 3000, 2988.19, 5671.99),
('ID-PKS-D-11-2307', 'PKS-D-11', 2023, 7, 6784.1, 3000, 3580.5, 6191.8),
('ID-PKS-D-11-2308', 'PKS-D-11', 2023, 8, 5875.44, 3000, 4409.79, 5046.16),
('ID-PKS-D-11-2309', 'PKS-D-11', 2023, 9, 7145.79, 3000, 4763.54, 6792.04),
('ID-PKS-D-11-2310', 'PKS-D-11', 2023, 10, 7255.65, 3000, 5024.61, 6994.57),
('ID-PKS-D-11-2311', 'PKS-D-11', 2023, 11, 4458.44, 3000, 5173.59, 4309.45),
('ID-PKS-D-11-2312', 'PKS-D-11', 2023, 12, 5343.99, 3000, 5319.21, 5198.38),
('ID-PKS-D-11-2401', 'PKS-D-11', 2024, 1, 5488.92, 3000, 6064.54, 4743.59),
('ID-PKS-D-11-2402', 'PKS-D-11', 2024, 2, 4510.12, 3000, 6619.32, 3955.34),
('ID-PKS-D-11-2403', 'PKS-D-11', 2024, 3, 6215.59, 3000, 7406.11, 5428.8),
('ID-PKS-D-11-2404', 'PKS-D-11', 2024, 4, 5691.03, 3000, 8169.66, 4927.49),
('ID-PKS-D-11-2405', 'PKS-D-11', 2024, 5, 8056.26, 3000, 8559.53, 7666.39),
('ID-PKS-D-11-2406', 'PKS-D-11', 2024, 6, 6877.43, 3000, 8844.11, 6592.85),
('ID-PKS-D-11-2407', 'PKS-D-11', 2024, 7, 7788.78, 3000, 9409.65, 7223.24),
('ID-PKS-D-11-2408', 'PKS-D-11', 2024, 8, 8143.21, 3000, 9597.97, 7954.89),
('ID-PKS-D-11-2409', 'PKS-D-11', 2024, 9, 6067.78, 3000, 10307.37, 5358.37),
('ID-PKS-D-11-2410', 'PKS-D-11', 2024, 10, 5784.34, 3000, 10472.59, 5619.13),
('ID-PKS-D-11-2411', 'PKS-D-11', 2024, 11, 4544.25, 3000, 10655.63, 4361.21),
('ID-PKS-D-11-2412', 'PKS-D-11', 2024, 12, 4737.16, 3000, 11266.13, 4126.66),
('ID-PKS-D-11-2501', 'PKS-D-11', 2025, 1, 5740.94, 3000, 12074.64, 4932.43),
('ID-PKS-D-11-2502', 'PKS-D-11', 2025, 2, 5423.86, 3000, 12618.31, 4880.2),
('ID-PKS-D-11-2503', 'PKS-D-11', 2025, 3, 5327.48, 3000, 13285.69, 4660.1),
('ID-PKS-D-11-2504', 'PKS-D-11', 2025, 4, 6801.45, 3000, 14051.02, 6036.12),
('ID-PKS-D-11-2505', 'PKS-D-11', 2025, 5, 6798.23, 3000, 14993.71, 5855.53),
('ID-PKS-D-11-2506', 'PKS-D-11', 2025, 6, 6108.16, 3000, 15842.57, 5259.3),
('ID-PKS-D-11-2507', 'PKS-D-11', 2025, 7, 7295.19, 3000, 16063.11, 7074.65),
('ID-PKS-D-11-2508', 'PKS-D-11', 2025, 8, 7392.9, 3000, 16824.18, 6631.83),
('ID-PKS-D-11-2509', 'PKS-D-11', 2025, 9, 5980.48, 3000, 17629.18, 5175.48),
('ID-PKS-D-11-2510', 'PKS-D-11', 2025, 10, 4933.28, 3000, 17748.8, 4813.66),
('ID-PKS-D-11-2511', 'PKS-D-11', 2025, 11, 4982.83, 3000, 18168.65, 4562.98),
('ID-PKS-D-11-2512', 'PKS-D-11', 2025, 12, 4820.44, 3000, 18305.67, 4683.42),
('ID-PKS-D-11-2601', 'PKS-D-11', 2026, 1, 4922.35, 3000, 18828.92, 4399.1),
('ID-PKS-D-11-2602', 'PKS-D-11', 2026, 2, 6462.59, 3000, 19669.14, 5622.37),
('ID-PKS-D-11-2603', 'PKS-D-11', 2026, 3, 5260.89, 3000, 20288.45, 4641.58),
('ID-PKS-D-11-2604', 'PKS-D-11', 2026, 4, 6851.02, 3000, 20744.71, 6394.76);
INSERT INTO realisasi_panen VALUES
('PN-KBN-11-1-2301', 'KBN-11-1', 'PKS-D-11', 2570.09, 1844.7, '2023-01-04', '2023-01-10', 'selesai'),
('PN-KBN-11-1-2302', 'KBN-11-1', 'PKS-D-11', 3157.98, 3148.02, '2023-02-03', '2023-02-09', 'selesai'),
('PN-KBN-11-1-2303', 'KBN-11-1', 'PKS-D-11', 2296.43, 2498.38, '2023-03-04', '2023-03-11', 'selesai'),
('PN-KBN-11-1-2304', 'KBN-11-1', 'PKS-D-11', 2396.74, 2462.94, '2023-04-04', '2023-04-09', 'selesai'),
('PN-KBN-11-1-2305', 'KBN-11-1', 'PKS-D-11', 2219.12, 0.0, '2023-05-05', '2023-05-15', 'tertunda'),
('PN-KBN-11-1-2306', 'KBN-11-1', 'PKS-D-11', 2691.85, 2687.48, '2023-06-05', '2023-06-13', 'selesai'),
('PN-KBN-11-1-2307', 'KBN-11-1', 'PKS-D-11', 3177.6, 3119.05, '2023-07-05', '2023-07-10', 'selesai'),
('PN-KBN-11-1-2308', 'KBN-11-1', 'PKS-D-11', 2229.86, 2403.04, '2023-08-05', '2023-08-19', 'selesai'),
('PN-KBN-11-1-2309', 'KBN-11-1', 'PKS-D-11', 2457.48, 0.0, '2023-09-05', '2023-09-19', 'tertunda'),
('PN-KBN-11-1-2310', 'KBN-11-1', 'PKS-D-11', 2734.01, 2026.96, '2023-10-06', '2023-10-11', 'selesai'),
('PN-KBN-11-1-2311', 'KBN-11-1', 'PKS-D-11', 3148.85, 3141.28, '2023-11-03', '2023-11-18', 'selesai'),
('PN-KBN-11-1-2312', 'KBN-11-1', 'PKS-D-11', 2394.25, 2563.82, '2023-12-05', '2023-12-19', 'selesai'),
('PN-KBN-11-1-2401', 'KBN-11-1', 'PKS-D-11', 2300.11, 2466.25, '2024-01-02', '2024-01-09', 'selesai'),
('PN-KBN-11-1-2402', 'KBN-11-1', 'PKS-D-11', 2657.04, 1636.13, '2024-02-03', '2024-02-18', 'selesai'),
('PN-KBN-11-1-2403', 'KBN-11-1', 'PKS-D-11', 3108.56, 3271.36, '2024-03-02', '2024-03-11', 'selesai'),
('PN-KBN-11-1-2404', 'KBN-11-1', 'PKS-D-11', 2584.85, 2540.63, '2024-04-03', '2024-04-17', 'selesai'),
('PN-KBN-11-1-2405', 'KBN-11-1', 'PKS-D-11', 2631.72, 2548.52, '2024-05-02', '2024-05-12', 'selesai'),
('PN-KBN-11-1-2406', 'KBN-11-1', 'PKS-D-11', 3075.2, 3365.35, '2024-06-03', '2024-06-15', 'selesai'),
('PN-KBN-11-1-2407', 'KBN-11-1', 'PKS-D-11', 3147.62, 3001.08, '2024-07-06', '2024-07-18', 'selesai'),
('PN-KBN-11-1-2408', 'KBN-11-1', 'PKS-D-11', 3017.5, 3116.61, '2024-08-02', '2024-08-08', 'selesai'),
('PN-KBN-11-1-2409', 'KBN-11-1', 'PKS-D-11', 2895.75, 2840.23, '2024-09-06', '2024-09-16', 'selesai'),
('PN-KBN-11-1-2410', 'KBN-11-1', 'PKS-D-11', 2604.45, 2487.27, '2024-10-05', '2024-10-14', 'selesai'),
('PN-KBN-11-1-2411', 'KBN-11-1', 'PKS-D-11', 2976.18, 0.0, '2024-11-06', '2024-11-11', 'tertunda'),
('PN-KBN-11-1-2412', 'KBN-11-1', 'PKS-D-11', 3078.34, 3353.35, '2024-12-04', '2024-12-14', 'selesai'),
('PN-KBN-11-1-2501', 'KBN-11-1', 'PKS-D-11', 3059.65, 1680.28, '2025-01-06', '2025-01-13', 'selesai'),
('PN-KBN-11-1-2502', 'KBN-11-1', 'PKS-D-11', 3004.4, 2999.81, '2025-02-02', '2025-02-15', 'selesai'),
('PN-KBN-11-1-2503', 'KBN-11-1', 'PKS-D-11', 2695.18, 1999.73, '2025-03-02', '2025-03-09', 'selesai'),
('PN-KBN-11-1-2504', 'KBN-11-1', 'PKS-D-11', 2402.12, 2532.86, '2025-04-02', '2025-04-14', 'selesai'),
('PN-KBN-11-1-2505', 'KBN-11-1', 'PKS-D-11', 2661.98, 0.0, '2025-05-05', '2025-05-16', 'tertunda'),
('PN-KBN-11-1-2506', 'KBN-11-1', 'PKS-D-11', 2503.81, 2653.77, '2025-06-06', '2025-06-12', 'selesai'),
('PN-KBN-11-1-2507', 'KBN-11-1', 'PKS-D-11', 3183.58, 3312.37, '2025-07-03', '2025-07-10', 'selesai'),
('PN-KBN-11-1-2508', 'KBN-11-1', 'PKS-D-11', 3215.62, 1820.43, '2025-08-06', '2025-08-12', 'selesai'),
('PN-KBN-11-1-2509', 'KBN-11-1', 'PKS-D-11', 2319.16, 1463.04, '2025-09-06', '2025-09-16', 'selesai'),
('PN-KBN-11-1-2510', 'KBN-11-1', 'PKS-D-11', 2905.35, 2862.85, '2025-10-03', '2025-10-10', 'selesai'),
('PN-KBN-11-1-2511', 'KBN-11-1', 'PKS-D-11', 2648.0, 2855.51, '2025-11-02', '2025-11-14', 'selesai'),
('PN-KBN-11-1-2512', 'KBN-11-1', 'PKS-D-11', 2365.57, 2287.61, '2025-12-05', '2025-12-19', 'selesai'),
('PN-KBN-11-1-2601', 'KBN-11-1', 'PKS-D-11', 2559.23, 2624.36, '2026-01-02', '2026-01-12', 'selesai'),
('PN-KBN-11-1-2602', 'KBN-11-1', 'PKS-D-11', 2561.64, 2666.67, '2026-02-03', '2026-02-12', 'selesai'),
('PN-KBN-11-1-2603', 'KBN-11-1', 'PKS-D-11', 2535.29, 1742.26, '2026-03-03', '2026-03-08', 'selesai'),
('PN-KBN-11-1-2604', 'KBN-11-1', 'PKS-D-11', 2503.92, 2671.87, '2026-04-03', '2026-04-11', 'selesai');
