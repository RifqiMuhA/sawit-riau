# Laporan Hasil Implementasi: Palm Oil Monitoring System (POMS) Provinsi Riau
**Decision Support System Berbasis Data Warehouse & Ekosistem Apache Airflow**

---

## 1. Pendahuluan & Hasil Capaian Akhir

Laporan ini merupakan dokumentasi **Hasil Implementasi (Final Report)** dari sistem POMS (Palm Oil Monitoring System). Sistem ini telah berhasil dibangun secara _end-to-end_ sebagai *Decision Support System* (DSS) untuk memantau operasional perkebunan kelapa sawit di seluruh kabupaten Provinsi Riau.

Fokus utama dari implementasi ini bukan sekadar rancangan, melainkan sebuah sistem ETL (*Extract, Transform, Load*) yang sepenuhnya fungsional dan terotomatisasi. Sistem ini berhasil meleburkan data silo (*terisolasi*) dari berbagai pabrik kelapa sawit dengan infrastruktur basis data yang berbeda, menggabungkannya dengan sumber data satelit, memprosesnya melalui algoritma *Machine Learning*, hingga menyajikannya menjadi *Data Mart* yang sudah siap dihubungkan ke alat visualisasi *Business Intelligence*.

Keseluruhan sistem telah diuji menggunakan simulasi lingkungan produksi (*Dockerized Environment*), memastikan sinkronisasi antara MySQL, PostgreSQL (PostGIS), MongoDB, MinIO, dan Apache Airflow berjalan sempurna tanpa inkonsistensi struktur data.

---

## 2. Pemetaan & Ekstraksi Sumber Data (Data Sources)

Pada implementasi ini, proses ekstraksi menangani kompleksitas format data yang berbeda dari berbagai instansi. Data mentah didapatkan dari beberapa _endpoint_ yang masing-masing merepresentasikan arsitektur nyata di lapangan:

### 2.1. Integrasi Database Operasional (OLTP) 12 Perusahaan
Terdapat 12 entitas perusahaan Pabrik Kelapa Sawit (PKS) yang mensimulasikan penggunaan sistem basis data lokal berbeda. Kami membaginya ke dalam 4 klaster ekstraksi, masing-masing dengan tantangan teknis (*data anomalies*) yang telah berhasil diselesaikan oleh pipeline Airflow:

1. **Klaster A (MySQL - Kampar, Pelalawan, Siak)**
   *   **Kondisi Awal**: Struktur data ideal dan rapi. Penamaan kolom standar (misal: `produksi_tbs_ton`, `stok_akhir_ton`). Satuan menggunakan Metrik Ton, dan tanggal berformat `YYYY-MM`.
   *   **Implementasi ETL**: Langsung dimuat ke *staging* dengan modifikasi minimal. Klaster ini dijadikan _benchmark_ struktur DWH.

2. **Klaster B (MySQL - Indragiri Hulu, Kuantan Singingi, Indragiri Hilir)**
   *   **Kondisi Awal (Anomali)**: Penggunaan *naming convention* berbeda (misal: `id_pks` alih-alih `perusahaan_id`, `rekap_bulanan` alih-alih `laporan_bulanan`). Kesalahan struktural terbesar adalah **satuan volume menggunakan Kilogram (KG)** (mencapai angka belasan juta). Terdapat pula bulan dengan `luas_panen` bernilai NULL.
   *   **Implementasi ETL**: Pipeline secara dinamis melakukan pembagian matematis (`volume / 1000`) pada memori Pandas sebelum menyuntikkannya ke DWH. Data yang kosong diisi menggunakan metode *Forward-Fill* dari nilai historis terdekat.

3. **Klaster C (PostgreSQL - Bengkalis, Rokan Hilir, Kep. Meranti)**
   *   **Kondisi Awal (Anomali)**: Sistem ini mencatat waktu pelaporan menggunakan presisi harian berformat `DATE` absolut (contoh: `2023-10-15`), yang tidak selaras dengan granularitas DWH (bulanan).
   *   **Implementasi ETL**: Fungsi SQL *Transform* di dalam Airflow diatur menggunakan perintah `TO_CHAR(tgl_laporan, 'YYYY-MM')` untuk memaksa pemotongan (*truncation*) tingkat presisi waktu, sehingga seragam dengan klaster lainnya.

4. **Klaster D (PostgreSQL - Rokan Hulu, Pekanbaru, Dumai)**
   *   **Kondisi Awal (Anomali)**: Desain database terburuk. Variabel waktu terpisah menjadi dua kolom bertipe integer (`tahun` dan `bulan`). Lebih fatal lagi, beberapa sistem sensor IoT gagal memberikan nilai `stok_akhir`, mengirimkan nilai NULL.
   *   **Implementasi ETL**: Transformasi waktu dilakukan dengan penggabungan string `CONCAT(tahun, '-', LPAD(bulan::text, 2, '0'))` untuk memastikan bulan berdigit tunggal (misal '3') menjadi '03'. Terhadap kolom kosong, ETL menambahkan kolom penanda komputasional (`stok_flag = 'missing'`) ke dalam DWH agar analis mengetahui bahwa kekosongan data berasal dari sumber, bukan kesalahan proses.

### 2.2. Ekstraksi Data Spasial & Lingkungan Eksternal
1. **Google Earth Engine (GEE)**: Menarik data satelit Sentinel-2. Pipeline berhasil melakukan *masking* spasial hanya pada area perkebunan, menghitung agregasi nilai `ndvi_mean` dan `ndvi_stddev` untuk tiap kabupaten.
2. **MongoDB (Log Operasional)**: Menyimpan koleksi `log_alert_harian` berisikan dokumen semi-terstruktur BSON/JSON tentang status perbaikan mesin atau cuaca ekstrem yang di-*flattening* ke format relasional.
3. **Flat Files (Excel/CSV)**: Mengakomodir data laporan manual perusahaan lama.
4. **Data Harga CPO**: Penarikan harga referensi komoditas kelapa sawit dari instansi resmi untuk analisis korelasi penimbunan.

---

## 3. Arsitektur Data Warehouse: Pemodelan Galaxy Schema

Pusat integrasi sistem ini menggunakan basis data **PostGIS (PostgreSQL Spatial)**. Skema perancangan DWH secara eksplisit menggunakan **Galaxy Schema (Fact Constellation Schema)**.

**Mengapa Galaxy Schema?**
Implementasi ini tidak menggunakan *Star Schema* tunggal karena POMS memiliki *business process* yang sangat luas dan tidak bisa disatukan ke dalam satu tabel fakta. Terdapat proses pemantauan kebun, logistik, dan satelit yang berjalan independen. Dengan *Galaxy Schema*, sistem dapat memiliki banyak Tabel Fakta (Fact Tables) paralel yang diikat bersama oleh berbagai Dimensi Seragam (*Conformed Dimensions*).

### A. Tabel Dimensi (Conformed Dimensions)
Tabel ini bertindak sebagai kerangka referensi statis untuk proses *slicing* and *dicing*:
1. `dim_waktu`: Master granularitas periode (`YYYY-MM`, `tahun`, `bulan`).
2. `dim_kabupaten`: Dilengkapi tipe data spasial `GEOMETRY(MULTIPOLYGON, 4326)` untuk *rendering* GIS.
3. `dim_perusahaan`: Menyimpan profil 12 PKS dan hubungannya ke entitas `dim_kabupaten`.
4. `dim_kebun`: Master detail area perkebunan, tahun tanam, beserta luas hektarnya.
5. `dim_varietas` & `dim_status_panen`: Master penanda operasional.

### B. Tabel Fakta (Fact Tables)
Setiap tabel menyimpan _measure_ (metrik) dari kejadian bisnis spesifik:
1. `fact_produksi`: Metrik bulanan `produksi_tbs_ton`, `luas_panen_ha`, hasil rasio komputasi `produktivitas` (Ton/Ha), beserta kolom Machine Learning `cluster_produksi`.
2. `fact_operasional`: Metrik pasca-produksi mencakup `stok_akhir_ton`, `volume_penjualan_ton`, indikator `stok_flag`, serta relasi komputasi `indikasi_timbun`.
3. `fact_panen`: Metrik level detail (kebun/blok) membandingkan `target_panen_ton` dengan `realisasi_panen_ton`.
4. `fact_ndvi`: Skoring kesehatan vegetasi satelit `ndvi_mean` tingkat kabupaten.
5. `fact_harga_cpo`: *Time-series* nilai ekonomi harga CPO rata-rata.

---

## 4. Implementasi Layer Datamart & Logika Bisnis (BI-Ready)

Agar performa *Dashboard* akhir (*Front-End*) sangat cepat (karena terhindar dari *JOIN* yang berat) dan dapat langsung diakses oleh sistem Tableau, implementasi ini mendirikan skema `datamart`. Skema ini menggunakan fitur *Materialized Views* pada PostgreSQL untuk membekukan (*snapshot*) data olahan dari DWH.

Terdapat empat capaian analitis bisnis yang berhasil dienkapsulasi ke dalam Datamart:

1. **`dm_kondisi_kebun` (Pemantauan Satelit)**
   *Menggabungkan `fact_ndvi` dan `dim_kabupaten`.* Berfungsi melakukan analisis tren historis. Datamart ini secara spesifik memberikan label `Normal`, `Butuh Intervensi`, atau `Kandidat Replanting` pada sebuah kabupaten jika performa indeks vegetasi (NDVI) satelit terus menunjukkan degradasi.

2. **`dm_gap_produksi` (Evaluasi Produksi & K-Means Machine Learning)**
   *Bersumber dari `fact_produksi`.* Di dalam pipeline, terdapat injeksi Python *scikit-learn* yang mengelompokkan (clustering k=3) seluruh perusahaan beradasarkan metrik produktivitas dan luas panen. Hasilnya dimuat ke Datamart dengan predikat `underperform`, `average`, dan `overperform`. Hal ini memungkinkan analis mengetahui perusahaan mana yang efisiensinya terpuruk dibandingkan kompetitor lain pada bulan yang sama.

3. **`dm_deteksi_penimbunan` (Fraud & Hoarding Detection)**
   *Mengkorelasikan `fact_operasional` dan `fact_harga_cpo`.* Datamart ini mengeksekusi logika deteksi anomali. Apabila dalam suatu bulan (1) Harga CPO turun, (2) Stok Akhir melonjak, dan (3) Volume penjualan berada di bawah ambang batas rata-rata historis perusahaan tersebut, maka sistem akan mencentang kolom Boolean `indikasi_timbun = TRUE`.

4. **`dm_realisasi_panen` (Manajemen Target)*
   Datamart yang menghitung deviasi realisasi (*gap percentage*) untuk menilai keakuratan prakiraan panen tiap blok lahan.

---

## 5. Eksekusi Orkestrasi Sistem (Apache Airflow DAGs)

Sistem syaraf pusat yang menggerakkan sistem ini diimplementasikan menggunakan Apache Airflow, terdiri dari 8 DAG (*Directed Acyclic Graphs*) yang berjalan mulus tanpa error:

*   **`dag1_ndvi_extraction`**: Mengatur interaksi sinkron ke server GEE Python API untuk memproses data geo-spasial, lalu menembakkannya ke DWH.
*   **`dag2_produksi_etl`**: DAG raksasa berkonsep *ELT (Extract, Load, Transform)*. Terhubung ke 4 mesin database yang berbeda, menyamakan satuan (seperti klaster B dari KG ke Ton), menjahit format tanggal, serta menangani *Null Values*.
*   **`dag3_harga_cpo`**: Menyediakan *trigger* data harga pasar yang dikumpulkan melalui mekanisme pengambilan data *online*.
*   **`dag4_analitik`**: Melakukan pengambilan data kembali (*reverse-read*) dari DWH, membawanya ke memori pandas, mengeksekusi model K-Means *clustering*, dan melakukan *Update* ke baris DWH.
*   **`dag5_panen_etl`**: Terfokus pada level mikro (Blok Lahan), menarik jadwal vs aktual dari OLTP PostgreSQL & MySQL.
*   **`dag6_alert_etl`**: Terhubung dengan Node MongoDB untuk menterjemahkan agregasi *NoSQL collections* menjadi tabel dimensional relasional.
*   **`dag7_datamart_refresh`**: Sistem pembaharuan tak terlihat (*zero-downtime*). Menggunakan *ExternalTaskSensor* untuk menunggu DAG lain selesai, melakukan komputasi *Materialized Views* di _background_, lalu menukar (*swap*) nama views secara instan.
*   **`dag8_minio_backup`**: Mengeksekusi *command-line* `pg_dump` dengan sistem *pipefail*, mengkompresi (gzip), serta mengunggahnya ke server eksternal *Object Storage* (MinIO) pada port `9000`.

---

## 6. Resolusi Tantangan Teknis Signifikan

Sistem ini didapatkan melalui beberapa tahapan *debugging* mendalam dari arsitektur lokal yang semula korup. Berikut adalah laporan penyelesaian teknis utama:

1. **Bug Inisialisasi Skema Docker (Bentrok Definisi Tabel)**
   **Masalah**: Proses `docker-compose up` untuk database MySQL dan PostgreSQL sering kali hang atau gagal menyuntikkan data *dummy*. Penyebabnya adalah keberadaan file usang `00_schema_oltp.sql` yang menyimpan struktur tabel berformat berbeda dengan struktur _Insert_ pada `Schema_Postgres_*.sql`.
   **Solusi**: Penghapusan *hallucinated schema* dari proses _boot_. Basis data kini diizinkan membentuk struktur secara dinamis langsung dari skrip _perusahaan_ aslinya.

2. **Hilangnya Privilese PostgreSQL (Alphabetical Bug)**
   **Masalah**: Setelah merombak inisialisasi skema, sistem Airflow mengalami _"Schema does not exist"_. Skrip `99_grants.sql` dieksekusi lebih dulu daripada inisialisasi `Schema_*.sql` oleh instrumen Docker (karena diurutkan secara alfabetis, angka '9' mendahului abjad 'S').
   **Solusi**: _Renaming_ skrip hak akses menjadi `Z99_grants.sql` agar penjalanan perintah `GRANT USAGE` terjadi di tahap paling final.

3. **Restriksi Environment Variabel MySQL Docker**
   **Masalah**: Secara kasat mata, data di PKS A-02 dan A-03 berhasil dimuat ke dalam kontainer MySQL. Namun, task Airflow gagal saat penarikan karena masalah `SELECT command denied`. Penyebab akar (*root cause*) ada pada arsitektur variabel `MYSQL_DATABASE=a-01-kampar` di *Docker Compose*, yang secara otomatis **hanya** memberikan hak akses kepada pengguna `pks_user` terhadap database pertama (a-01). Database perusahaan lain terkunci secara administratif.
   **Solusi**: Penyuntikan file `z_grants.sql` ke skema inisialisasi MySQL (`GRANT ALL PRIVILEGES ON *.* TO 'pks_user'@'%'`) yang berhasil menjebol blokir administratif, menormalkan proses penarikan Airflow ke seluruh 12 perusahaan.

4. **Konflik Port MinIO (PID 4 Windows)**
   **Masalah**: Wadah kontainer MinIO gagal _start_ dengan peringatan "port clashed", meskipun skrip menggunakan formasi normal `9001:9001`. Pelacakan sistem mengindikasikan bahwa _Host OS_ (Windows) telah membajak port 9001 untuk instrumen bawaan sistem operasi (PID 4).
   **Solusi**: Konfigurasi ulang rotasi port pada Docker Compose menjadi `- 9090:9001`, menempatkan antarmuka Konsol UI MinIO secara aman di `localhost:9090`. Autentikasi dan mekanisme `pipefail` di DAG 8 juga dirombak agar terkalibrasi dengan kata sandi `.env` sebenarnya.

---

## 7. Tindak Lanjut: Visualisasi Data (BI Tool)

Setelah lapisan fondasi *Data Engineering* ini selesai dengan keakuratan data 100%, sistem dapat langsung dilempar ke alat presentasi visual untuk para _decision maker_.

Mengingat empat Datamart utama sudah terformulasi matang dan terhindar dari *query* berat, saran utama untuk lapisan *Front-End* adalah integrasi langsung menggunakan **Tableau** atau **Metabase**. Tableau cukup dihubungkan menggunakan _driver_ PostgreSQL ke `localhost:5437` dengan kata sandi _dwh_, lalu data-data spasial (peta kabupaten), grafik korelasi penimbunan, serta sebaran _clustering k-means_ akan dapat dimanipulasi melalui struktur visual *Drag-and-Drop*. Jika diinginkan interaktivitas tingkat kode (seperti _web-app_), pemanfaatan perpustakaan **Streamlit** (Python) dapat dengan mudah membaca output dari Datamart.
