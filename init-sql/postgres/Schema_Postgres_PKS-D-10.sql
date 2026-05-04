-- SCHEMA POSTGRESQL: d-10-rokan-hulu
CREATE SCHEMA IF NOT EXISTS "d-10-rokan-hulu";
SET search_path TO "d-10-rokan-hulu";

CREATE TABLE IF NOT EXISTS data_perusahaan (perusahaan_id VARCHAR(20) PRIMARY KEY, nama_perusahaan VARCHAR(100), lokasi_kabupaten VARCHAR(10), kapasitas NUMERIC(10,2));
CREATE TABLE IF NOT EXISTS data_karyawan (karyawan_id VARCHAR(30) PRIMARY KEY, perusahaan_id VARCHAR(20), nama_karyawan VARCHAR(100), jabatan VARCHAR(50), status VARCHAR(20));
CREATE TABLE IF NOT EXISTS varietas (id_varietas VARCHAR(10) PRIMARY KEY, nama_varietas VARCHAR(50), produsen VARCHAR(50), potensi_cpo DECIMAL(4,2), tahan_ganoderma VARCHAR(20), rerata_tandan_kg DECIMAL(4,2));
CREATE TABLE IF NOT EXISTS kebun_produksi (kebun_id VARCHAR(20) PRIMARY KEY, perusahaan_id VARCHAR(20), nama_kebun VARCHAR(100), lokasi_kabupaten VARCHAR(10), luas_ha NUMERIC(10,2), tahun_tanam SMALLINT, varietas_id VARCHAR(20), status VARCHAR(20));
CREATE TABLE IF NOT EXISTS produksi_bulanan (id VARCHAR(30) PRIMARY KEY, perusahaan_id VARCHAR(20), tahun SMALLINT, bulan SMALLINT, produksi_tbs_ton NUMERIC(12,2), luas_panen_ha NUMERIC(10,2), stok_akhir_ton NUMERIC(12,2), volume_penjualan NUMERIC(12,2));
CREATE TABLE IF NOT EXISTS realisasi_panen (realisasi_id VARCHAR(30) PRIMARY KEY, kebun_id VARCHAR(20), perusahaan_id VARCHAR(20), target_panen_ton NUMERIC(12,2), realisasi_panen_ton NUMERIC(12,2), tgl_mulai DATE, tgl_selesai DATE, status_panen VARCHAR(20));

INSERT INTO data_perusahaan VALUES ('PKS-D-10', 'PT Rohul Palma', '1407', 75);
INSERT INTO data_karyawan VALUES
('PKS-D-10-KRY-01', 'PKS-D-10', 'Chairil Rahman', 'Supervisor', 'aktif'),
('PKS-D-10-KRY-02', 'PKS-D-10', 'Ihsan Hasan', 'Supervisor', 'resign'),
('PKS-D-10-KRY-03', 'PKS-D-10', 'Siti Hamzah', 'Security', 'pensiun'),
('PKS-D-10-KRY-04', 'PKS-D-10', 'Emil Ibrahim', 'Supervisor', 'resign'),
('PKS-D-10-KRY-05', 'PKS-D-10', 'Emil Siddiq', 'Mandor', 'pensiun'),
('PKS-D-10-KRY-06', 'PKS-D-10', 'Andi Syafiq', 'Mekanik', 'aktif'),
('PKS-D-10-KRY-07', 'PKS-D-10', 'Dahnil Sulaiman', 'Operator', 'aktif'),
('PKS-D-10-KRY-08', 'PKS-D-10', 'Aminah Hamzah', 'Manager', 'resign'),
('PKS-D-10-KRY-09', 'PKS-D-10', 'Aminah Hasan', 'Security', 'aktif'),
('PKS-D-10-KRY-10', 'PKS-D-10', 'Ihsan Sari', 'Manager', 'aktif');
INSERT INTO varietas VALUES
('V-01', 'DxP PPKS 540', 'PPKS', 8.5, 'Tinggi', 16.5),
('V-02', 'DxP Simalungun', 'PPKS', 7.8, 'Sedang', 18.2),
('V-03', 'DxP Yangambi', 'Socfindo', 8.2, 'Rentan', 15.0);
INSERT INTO kebun_produksi VALUES
('KBN-10-1', 'PKS-D-10', 'Kebun Afdeling 1', '1407', 7487.28, 2012, 'V-01', 'produktif'),
('KBN-10-2', 'PKS-D-10', 'Kebun Afdeling 2', '1407', 3196.34, 2010, 'V-03', 'TBM'),
('KBN-10-3', 'PKS-D-10', 'Kebun Afdeling 3', '1407', 5316.38, 2013, 'V-02', 'TBM');
INSERT INTO produksi_bulanan VALUES
('ID-PKS-D-10-2301', 'PKS-D-10', 2023, 1, 27130.53, 16000, 4535.24, 23135.19),
('ID-PKS-D-10-2302', 'PKS-D-10', 2023, 2, 28402.24, 16000, 8082.76, 24854.73),
('ID-PKS-D-10-2303', 'PKS-D-10', 2023, 3, 31391.49, 16000, 11256.03, 28218.22),
('ID-PKS-D-10-2304', 'PKS-D-10', 2023, 4, 41261.49, 16000, 13990.16, 38527.35),
('ID-PKS-D-10-2305', 'PKS-D-10', 2023, 5, 34334.18, 16000, 18472.3, 29852.04),
('ID-PKS-D-10-2306', 'PKS-D-10', 2023, 6, 32290.48, 16000, 20997.23, 29765.55),
('ID-PKS-D-10-2307', 'PKS-D-10', 2023, 7, 34358.24, 16000, 22028.5, 33326.98),
('ID-PKS-D-10-2308', 'PKS-D-10', 2023, 8, 44110.09, 16000, 28475.22, 37663.37),
('ID-PKS-D-10-2309', 'PKS-D-10', 2023, 9, 37923.29, 16000, 32304.02, 34094.49),
('ID-PKS-D-10-2310', 'PKS-D-10', 2023, 10, 35807.55, 16000, 33274.38, 34837.18),
('ID-PKS-D-10-2311', 'PKS-D-10', 2023, 11, 33786.08, 16000, 36089.61, 30970.85),
('ID-PKS-D-10-2312', 'PKS-D-10', 2023, 12, 26548.44, 16000, 39509.78, 23128.27),
('ID-PKS-D-10-2401', 'PKS-D-10', 2024, 1, 28367.46, 16000, 43062.19, 24815.05),
('ID-PKS-D-10-2402', 'PKS-D-10', 2024, 2, 25265.81, 16000, 44296.12, 24031.88),
('ID-PKS-D-10-2403', 'PKS-D-10', 2024, 3, 30009.76, 16000, 48146.4, 26159.48),
('ID-PKS-D-10-2404', 'PKS-D-10', 2024, 4, 36481.96, 16000, 50874.6, 33753.76),
('ID-PKS-D-10-2405', 'PKS-D-10', 2024, 5, 33108.87, 16000, 52739.04, 31244.43),
('ID-PKS-D-10-2406', 'PKS-D-10', 2024, 6, 40882.1, 16000, 57651.88, 35969.26),
('ID-PKS-D-10-2407', 'PKS-D-10', 2024, 7, 32024.55, 16000, 59061.06, 30615.37),
('ID-PKS-D-10-2408', 'PKS-D-10', 2024, 8, 38045.81, 16000, 63592.66, 33514.21),
('ID-PKS-D-10-2409', 'PKS-D-10', 2024, 9, 33149.67, 16000, 67984.05, 28758.28),
('ID-PKS-D-10-2410', 'PKS-D-10', 2024, 10, 36753.78, 16000, 72484.55, 32253.28),
('ID-PKS-D-10-2411', 'PKS-D-10', 2024, 11, 28212.14, 16000, 73200.76, 27495.94),
('ID-PKS-D-10-2412', 'PKS-D-10', 2024, 12, 21711.3, 16000, 74026.64, 20885.42),
('ID-PKS-D-10-2501', 'PKS-D-10', 2025, 1, 24518.51, 16000, 76263.56, 22281.6),
('ID-PKS-D-10-2502', 'PKS-D-10', 2025, 2, 25630.83, 16000, 78083.07, 23811.32),
('ID-PKS-D-10-2503', 'PKS-D-10', 2025, 3, 36687.1, 16000, 83053.9, 31716.27),
('ID-PKS-D-10-2504', 'PKS-D-10', 2025, 4, 37381.65, 16000, 84228.45, 36207.1),
('ID-PKS-D-10-2505', 'PKS-D-10', 2025, 5, 43235.49, 16000, 89386.61, 38077.33),
('ID-PKS-D-10-2506', 'PKS-D-10', 2025, 6, 33957.52, 16000, 90296.43, 33047.7),
('ID-PKS-D-10-2507', 'PKS-D-10', 2025, 7, 35842.21, 16000, 93876.97, 32261.66),
('ID-PKS-D-10-2508', 'PKS-D-10', 2025, 8, 30332.35, 16000, 95500.07, 28709.25),
('ID-PKS-D-10-2509', 'PKS-D-10', 2025, 9, 35954.13, 16000, 97639.26, 33814.95),
('ID-PKS-D-10-2510', 'PKS-D-10', 2025, 10, 28687.89, 16000, 99091.52, 27235.63),
('ID-PKS-D-10-2511', 'PKS-D-10', 2025, 11, 29551.43, 16000, 103281.53, 25361.41),
('ID-PKS-D-10-2512', 'PKS-D-10', 2025, 12, 21241.71, 16000, 105522.62, 19000.63),
('ID-PKS-D-10-2601', 'PKS-D-10', 2026, 1, 21530.47, 16000, 106164.51, 20888.59),
('ID-PKS-D-10-2602', 'PKS-D-10', 2026, 2, 32366.62, 16000, 108900.33, 29630.79),
('ID-PKS-D-10-2603', 'PKS-D-10', 2026, 3, 29861.63, 16000, 111102.73, 27659.24),
('ID-PKS-D-10-2604', 'PKS-D-10', 2026, 4, 28818.91, 16000, 114102.46, 25819.17);
INSERT INTO realisasi_panen VALUES
('PN-KBN-10-1-2301', 'KBN-10-1', 'PKS-D-10', 13644.9, 14371.42, '2023-01-06', '2023-01-14', 'selesai'),
('PN-KBN-10-1-2302', 'KBN-10-1', 'PKS-D-10', 15071.68, 0.0, '2023-02-06', '2023-02-15', 'tertunda'),
('PN-KBN-10-1-2303', 'KBN-10-1', 'PKS-D-10', 13694.1, 13785.37, '2023-03-02', '2023-03-09', 'selesai'),
('PN-KBN-10-1-2304', 'KBN-10-1', 'PKS-D-10', 14213.49, 13986.89, '2023-04-03', '2023-04-16', 'selesai'),
('PN-KBN-10-1-2305', 'KBN-10-1', 'PKS-D-10', 15800.03, 16526.91, '2023-05-06', '2023-05-16', 'selesai'),
('PN-KBN-10-1-2306', 'KBN-10-1', 'PKS-D-10', 15405.37, 15068.22, '2023-06-04', '2023-06-10', 'selesai'),
('PN-KBN-10-1-2307', 'KBN-10-1', 'PKS-D-10', 15950.74, 16311.49, '2023-07-06', '2023-07-16', 'selesai'),
('PN-KBN-10-1-2308', 'KBN-10-1', 'PKS-D-10', 12385.91, 11779.32, '2023-08-06', '2023-08-18', 'selesai'),
('PN-KBN-10-1-2309', 'KBN-10-1', 'PKS-D-10', 15173.08, 15903.29, '2023-09-03', '2023-09-08', 'selesai'),
('PN-KBN-10-1-2310', 'KBN-10-1', 'PKS-D-10', 13558.74, 7297.25, '2023-10-05', '2023-10-18', 'selesai'),
('PN-KBN-10-1-2311', 'KBN-10-1', 'PKS-D-10', 14500.89, 13788.72, '2023-11-04', '2023-11-13', 'selesai'),
('PN-KBN-10-1-2312', 'KBN-10-1', 'PKS-D-10', 12710.26, 13342.24, '2023-12-02', '2023-12-14', 'selesai'),
('PN-KBN-10-1-2401', 'KBN-10-1', 'PKS-D-10', 14877.67, 14870.01, '2024-01-06', '2024-01-21', 'selesai'),
('PN-KBN-10-1-2402', 'KBN-10-1', 'PKS-D-10', 11852.7, 12082.84, '2024-02-05', '2024-02-14', 'selesai'),
('PN-KBN-10-1-2403', 'KBN-10-1', 'PKS-D-10', 15219.09, 15420.17, '2024-03-03', '2024-03-18', 'selesai'),
('PN-KBN-10-1-2404', 'KBN-10-1', 'PKS-D-10', 15921.12, 16037.65, '2024-04-02', '2024-04-12', 'selesai'),
('PN-KBN-10-1-2405', 'KBN-10-1', 'PKS-D-10', 11712.84, 12130.34, '2024-05-03', '2024-05-16', 'selesai'),
('PN-KBN-10-1-2406', 'KBN-10-1', 'PKS-D-10', 11934.13, 12780.02, '2024-06-05', '2024-06-10', 'selesai'),
('PN-KBN-10-1-2407', 'KBN-10-1', 'PKS-D-10', 15044.31, 15523.56, '2024-07-04', '2024-07-17', 'selesai'),
('PN-KBN-10-1-2408', 'KBN-10-1', 'PKS-D-10', 13338.21, 14051.43, '2024-08-04', '2024-08-11', 'selesai'),
('PN-KBN-10-1-2409', 'KBN-10-1', 'PKS-D-10', 12419.51, 13043.6, '2024-09-02', '2024-09-09', 'selesai'),
('PN-KBN-10-1-2410', 'KBN-10-1', 'PKS-D-10', 14324.45, 13864.29, '2024-10-05', '2024-10-20', 'selesai'),
('PN-KBN-10-1-2411', 'KBN-10-1', 'PKS-D-10', 12218.23, 13325.48, '2024-11-06', '2024-11-13', 'selesai'),
('PN-KBN-10-1-2412', 'KBN-10-1', 'PKS-D-10', 14633.86, 15782.59, '2024-12-03', '2024-12-14', 'selesai'),
('PN-KBN-10-1-2501', 'KBN-10-1', 'PKS-D-10', 12052.62, 11629.54, '2025-01-02', '2025-01-12', 'selesai'),
('PN-KBN-10-1-2502', 'KBN-10-1', 'PKS-D-10', 13452.72, 9840.62, '2025-02-04', '2025-02-15', 'selesai'),
('PN-KBN-10-1-2503', 'KBN-10-1', 'PKS-D-10', 12626.58, 12807.91, '2025-03-06', '2025-03-11', 'selesai'),
('PN-KBN-10-1-2504', 'KBN-10-1', 'PKS-D-10', 12090.92, 11767.14, '2025-04-06', '2025-04-21', 'selesai'),
('PN-KBN-10-1-2505', 'KBN-10-1', 'PKS-D-10', 14812.09, 11764.85, '2025-05-03', '2025-05-13', 'selesai'),
('PN-KBN-10-1-2506', 'KBN-10-1', 'PKS-D-10', 14589.37, 8714.77, '2025-06-03', '2025-06-11', 'selesai'),
('PN-KBN-10-1-2507', 'KBN-10-1', 'PKS-D-10', 15335.26, 16577.5, '2025-07-06', '2025-07-12', 'selesai'),
('PN-KBN-10-1-2508', 'KBN-10-1', 'PKS-D-10', 13090.7, 13076.95, '2025-08-03', '2025-08-11', 'selesai'),
('PN-KBN-10-1-2509', 'KBN-10-1', 'PKS-D-10', 16106.1, 17374.61, '2025-09-05', '2025-09-15', 'selesai'),
('PN-KBN-10-1-2510', 'KBN-10-1', 'PKS-D-10', 13600.82, 13217.83, '2025-10-04', '2025-10-18', 'selesai'),
('PN-KBN-10-1-2511', 'KBN-10-1', 'PKS-D-10', 15631.39, 8760.5, '2025-11-02', '2025-11-09', 'selesai'),
('PN-KBN-10-1-2512', 'KBN-10-1', 'PKS-D-10', 11757.23, 8103.36, '2025-12-06', '2025-12-20', 'selesai'),
('PN-KBN-10-1-2601', 'KBN-10-1', 'PKS-D-10', 11353.79, 11017.92, '2026-01-05', '2026-01-18', 'selesai'),
('PN-KBN-10-1-2602', 'KBN-10-1', 'PKS-D-10', 15807.13, 16062.74, '2026-02-05', '2026-02-15', 'selesai'),
('PN-KBN-10-1-2603', 'KBN-10-1', 'PKS-D-10', 15128.02, 15182.99, '2026-03-03', '2026-03-12', 'selesai'),
('PN-KBN-10-1-2604', 'KBN-10-1', 'PKS-D-10', 16078.47, 16037.01, '2026-04-06', '2026-04-20', 'selesai');
