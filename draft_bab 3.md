# BAB 3 PENGOLAHAN DAN ANALISIS DATA

Bab ini menguraikan tahapan rekayasa data (*data engineering*) dan pemodelan analitik (*data science*) yang dilakukan dalam membangun Sistem Monitoring Sawit Riau. Proses ini diorkestrasi sepenuhnya menggunakan Apache Airflow melalui pendekatan *Extract, Transform, Load* (ETL).

## 3.1 Ingestion Layer: Ekstraksi dan Preprocessing Data Mentah
Fase *ingestion* bertanggung jawab untuk menarik data dari berbagai sumber heterogen (seperti sistem OLTP relasional, NoSQL, hingga *web scraping*), melakukan pembersihan data (*data cleansing*), dan memuatnya ke dalam tabel fakta di Data Warehouse (PostgreSQL/PostGIS).

### 3.1.1 Eksekusi DAG NDVI: Pemrosesan Citra Satelit
Data kesehatan lahan berbasis satelit merupakan fondasi utama dari sistem monitoring ini. Namun, pengolahan citra satelit mentah memiliki tantangan geografis tersendiri.

*   **Sumber Data:** API Google Earth Engine (GEE) untuk Sentinel-2 Surface Reflectance.
    > *[PLACEHOLDER SCREENSHOT: Tampilan antarmuka Google Earth Engine Code Editor atau dokumentasi dataset Sentinel-2 GEE]*
*   **Proses ETL (DAG `dag1_ndvi_extraction`):** 
    Kondisi iklim tropis di Riau menyebabkan citra satelit mentah yang ditangkap seringkali tertutup oleh gumpalan awan tebal. Jika dibiarkan, piksel awan ini akan menghasilkan nilai NDVI semu yang merusak akurasi data. Oleh karena itu, sebelum ekstraksi, DAG ini terlebih dahulu menjalankan fungsi *Cloud Masking* menggunakan *Quality Assessment (QA) band* bawaan Sentinel-2 untuk membuang piksel yang terindikasi sebagai awan atau bayangan awan.
    
    Setelah citra dibersihkan, sistem baru mengalkulasi *Normalized Difference Vegetation Index* (NDVI) menggunakan pita spektrum *Near-Infrared* (B8) dan *Red* (B4). Hasil kalkulasi piksel tersebut kemudian diagregasi (dirata-ratakan) ke dalam tingkat wilayah administrasi (kabupaten) menggunakan batas poligon geospasial Riau.
    > *[PLACEHOLDER SCREENSHOT: Graph View DAG `dag1_ndvi_extraction` di Apache Airflow berwarna hijau/Success]*
    
    **Potongan Kode Penting (Cloud Masking & Ekstraksi GEE):**
    ```python
    def mask_s2_clouds(image):
        qa = image.select('QA60')
        # Bit 10 dan 11 merepresentasikan awan tebal dan awan cirrus
        cloudBitMask = 1 << 10
        cirrusBitMask = 1 << 11
        mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(cirrusBitMask).eq(0))
        return image.updateMask(mask).divide(10000)

    def calculate_ndvi(image):
        ndvi = image.normalizedDifference(['B8', 'B4']).rename('NDVI')
        return image.addBands(ndvi)
    ```
*   **Output DWH:** Data spasial dan nilai rata-rata NDVI bersih yang terbebas dari bias awan.
    > *[PLACEHOLDER SCREENSHOT: Tabel DWH untuk data NDVI di DBeaver/PgAdmin. Mohon beri kotak merah/highlight pada kolom `ndvi_mean` dan `kode_wilayah`]*

### 3.1.2 Eksekusi DAG Produksi dan DAG Data Panen
Data operasional produksi pabrik dan kebun ditarik dari database OLTP transaksional (MySQL) serta file laporan manual (Excel) dari berbagai cabang perusahaan.

*   **Sumber Data:** Database OLTP MySQL internal perusahaan dan file laporan panen berformat Excel/CSV.
    > *[PLACEHOLDER SCREENSHOT: Cuplikan file Excel mentah yang berantakan (ada cell kosong/format salah) ATAU tampilan raw data MySQL OLTP]*
*   **Proses ETL (DAG `dag2_produksi_etl` & `dag3_panen_etl`):** 
    Karena data berasal dari inputan *user* yang berbeda-beda (*human-entry*), tim menemukan banyak sekali anomali data (kotor). *Problem* utama yang ditemui adalah **inkonsistensi satuan ukuran**—di mana sebagian PKS menginput berat produksi dalam satuan Kilogram (Kg), namun ada pula yang langsung menuliskannya dalam Tonase. Selain itu, ditemukan ratusan baris data dengan *missing value* (kosong) pada kolom target harian, serta format penulisan tanggal (`YYYY/MM/DD` vs `DD-MM-YYYY`) yang berantakan.

    Melalui librari *Pandas* di dalam Airflow, anomali ini dibersihkan. Baris data yang mengalami *missing value* pada target harian ditangani menggunakan metode interpolasi *forward-fill* dari hari sebelumnya, atau diisi nilai *default* 0 jika benar-benar absen. Selanjutnya, sebuah fungsi pemetaan dijalankan untuk mendeteksi satuan Kg dan membaginya dengan 1000 agar seluruh data secara seragam menjadi satuan TON. Terakhir, *timestamp* tanggal dipotong (*truncate*) menjadi format seragam `YYYY-MM`.
    > *[PLACEHOLDER SCREENSHOT: Graph View DAG `dag2_produksi_etl` di Apache Airflow]*
    
    **Potongan Kode Penting (Data Cleansing & Normalisasi Unit):**
    ```python
    # Penanganan missing value dengan forward-fill pada grup perusahaan yang sama
    df['produksi_tbs_kg'] = df.groupby('perusahaan_id')['produksi_tbs_kg'].ffill().fillna(0)
    
    # Normalisasi inkonsistensi satuan ke TON
    df['produksi_tbs_ton'] = df.apply(
        lambda row: row['produksi_tbs_kg'] / 1000.0 if row['produksi_tbs_kg'] > 1000 
        else row['produksi_tbs_kg'], axis=1
    )
    df['periode'] = pd.to_datetime(df['tgl_laporan']).dt.strftime('%Y-%m')
    ```
*   **Output DWH:** Tabel fakta yang sudah bersih, konsisten, dan terstandarisasi.
    > *[PLACEHOLDER SCREENSHOT: Tabel `fact_produksi` atau `fact_panen` di DBeaver. Beri kotak merah pada kolom hasil konversi yaitu `produksi_tbs_ton` dan kolom `periode`]*

### 3.1.3 Eksekusi DAG Harga CPO: Scraping dan Transformasi
Data pergerakan harga referensi *Crude Palm Oil* (CPO) sangat krusial, namun pengumpulannya memiliki hambatan teknis dari sisi sumber data pemerintah.

*   **Sumber Data:** Situs web resmi Dinas Perkebunan Provinsi Riau.
    > *[PLACEHOLDER SCREENSHOT: Halaman website Disbun Riau yang menampilkan daftar link unduhan PDF Harga CPO]*
*   **Proses ETL (DAG `dag4_harga_cpo`):** 
    Pemerintah Provinsi Riau mempublikasikan harga acuan CPO mingguan yang terkunci secara kaku dalam format dokumen PDF (tanpa menyediakan API). Menginput data tersebut secara manual setiap minggu tentu menyita waktu dan rentan *typo*. 
    
    Untuk mengatasi hal ini, sistem menjalankan *Headless Browser* (Selenium) untuk meniru navigasi manusia—membuka portal web, mencari tombol "Download" pada tabel rilis terbaru, dan menyimpan file PDF tersebut secara temporer. Setelah PDF terunduh, pustaka OCR/PDF-Parser mengekstrak teks di dalamnya, mencari pola *RegEx* (Regular Expression) untuk nominal "Harga CPO (Rp/Kg)", membuang karakter titik ribuan (`.`), lalu mengonversinya menjadi tipe data *Integer*.
    > *[PLACEHOLDER SCREENSHOT: Graph View DAG `dag4_harga_cpo` di Airflow]*
    
    **Potongan Kode Penting (Web Scraping Selenium & Regex):**
    ```python
    import re
    # Selenium meniru perilaku manusia untuk mengunduh PDF
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(options=options)
    driver.get("URL_DISBUN_RIAU_CPO")
    
    # ... (setelah PDF diunduh dan dibaca) ...
    
    # Regex untuk menangkap angka harga CPO dan membuang titik ribuan
    harga_match = re.search(r'Harga CPO.*?(?:Rp\s*)?([\d\.]+)', pdf_text)
    if harga_match:
        harga_clean = int(harga_match.group(1).replace('.', ''))
    ```
*   **Output DWH:** Harga CPO terekam terstruktur ke dalam tabel kalender/dimensi.
    > *[PLACEHOLDER SCREENSHOT: Tabel DWH yang berisi riwayat harga CPO. Beri kotak merah pada kolom `harga_cpo`]*

### 3.1.4 Eksekusi DAG Peringatan Operasional
Pemantauan operasional pabrik dan IoT membutuhkan ekstraksi dari basis data *schema-less*.

*   **Sumber Data:** Log sistem berformat dokumen JSON dari MongoDB.
    > *[PLACEHOLDER SCREENSHOT: Tampilan dokumen JSON bersarang (nested) di MongoDB Compass yang terlihat rumit]*
*   **Proses ETL (DAG `dag5_alert_etl`):** 
    Mesin sensor IoT dan log aplikasi menghasilkan ribuan baris data per hari berupa dokumen JSON bersarang (*nested array/object*) di MongoDB. Jika data ini dipindahkan mentah-mentah ke Relational DWH, *query* dasbor akan menjadi sangat lambat karena harus mem-*parsing* teks JSON setiap kali dijalankan.
    
    DAG ini bertugas menarik koleksi dokumen tersebut, memipihkan (*flattening*) struktur bersarangnya menggunakan *Pandas `json_normalize`*, lalu mengagregasi kejadian peringatan tersebut. Alert ganda (*duplicate alerts*) yang terpancar di detik yang berdekatan untuk insiden yang sama dilakukan *deduplikasi*, kemudian dihitung total per kemunculan (*severity*) harian.
    > *[PLACEHOLDER SCREENSHOT: Graph View DAG `dag5_alert_etl`]*
*   **Output DWH:** Tabel baris dan kolom yang terstruktur (relasional).
    > *[PLACEHOLDER SCREENSHOT: Tabel DWH `fact_alert` / operasional. Beri kotak merah pada kolom tipe peringatan atau total kejadian]*

---

## 3.2 Analytics Layer: Pemodelan dan Sintesis Data
Pada layer ini, data mentah yang telah masuk ke dalam Data Warehouse tidak sekadar direkapitulasi, melainkan diproses menggunakan algoritma komputasi lanjutan untuk mengekstraksi nilai tambah (*insight*).

### 3.2.1 Analisis dan Klasifikasi Kondisi Kebun (NDVI)
Nilai mutlak NDVI yang berkisar dari 0 hingga 1 sulit diinterpretasikan secara langsung oleh pemangku kebijakan tanpa adanya parameter standar yang jelas.

*   **Proses (DAG Analitik):** 
    Menyajikan angka rata-rata NDVI 0.65 tidak akan memberikan makna apakah lahan tersebut sehat atau tidak. Oleh karena itu, sistem melakukan rekayasa parameter dengan menghitung kurva distribusi historis NDVI Provinsi Riau secara keseluruhan menggunakan metode Persentil. 
    Lahan kemudian dilabeli menjadi tiga status: **Normal** (jika nilai NDVI berada di atas Persentil ke-66 historis provinsi), **Menurun** (berada di antara Persentil 33 hingga 66), dan berstatus **Kritis** (anjlok di bawah Persentil 33).
    > *[PLACEHOLDER SCREENSHOT: Graph View bagian dari DAG Analitik yang mengeksekusi pelabelan NDVI]*
*   **Output:** Pelabelan status kesehatan yang human-readable.
    > *[PLACEHOLDER SCREENSHOT: Tabel hasil klasifikasi NDVI. Beri kotak merah pada kolom `status_kebun` (Kritis/Menurun/Normal)]*

### 3.2.2 Analisis Kesenjangan Produktivitas (K-Means Clustering)
Dalam mengawasi ratusan PKS, sangat sulit menentukan pabrik mana yang berkinerja buruk jika hanya menggunakan batas (*threshold*) buatan manusia yang cenderung bias dan kaku.

*   **Proses (DAG Analitik):** 
    Sistem mendelegasikan tugas profiling pabrik menggunakan algoritma *Machine Learning* tanpa supervisi (*Unsupervised Learning*) yaitu **K-Means Clustering** dari *Scikit-Learn*. Algoritma ini "belajar" mengelompokkan pabrik secara adil berdasarkan empat metrik kinerja simultan: tingkat rendemen, total volume olah, total kapasitas pabrik, dan persentase utilisasi kapasitas.
    Data distandardisasi terlebih dahulu menggunakan `StandardScaler` agar metrik volume (ribuan ton) tidak mengalahkan bobot metrik persentase (rendemen). K-Means kemudian mengelompokkan pabrik ke dalam tepat 3 *cluster* (k=3). Berdasarkan kalkulasi nilai tengah (*centroid*) tiap kelompok, sistem otomatis memetakannya menjadi profil: **Underperform**, **Average**, dan **Overperform**.
    > *[PLACEHOLDER SCREENSHOT: Log terminal Airflow yang menampilkan proses iterasi K-Means atau pembentukan array centroid]*
    
    **Potongan Kode Penting (K-Means Clustering):**
    ```python
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    # Standarisasi fitur yang memiliki variasi skala sangat jauh (Ton vs Persentase)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(group[["rendemen_pct", "volume_olah_ton", "utilisasi_kapasitas_pct"]])
    
    # Eksekusi algoritma K-Means untuk 3 kelompok (Underperform, Average, Overperform)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)
    
    # Mengurutkan centroid terendah ke tertinggi secara otomatis
    sorted_idx = np.argsort(kmeans.cluster_centers_.flatten())
    ```
*   **Output:** Segmentasi pabrik yang berbasis probabilitas matematis (K-Means), bukan subyektivitas.
    > *[PLACEHOLDER SCREENSHOT: Tabel DWH. Beri kotak merah pada kolom `cluster_produksi` yang berisi nilai teks klasifikasi]*

### 3.2.3 Deteksi Indikasi Penimbunan Sawit (Rule-Based System)
Permasalahan penahanan stok (*hoarding*) oleh PKS saat harga CPO sedang fluktuatif (anjlok) merupakan celah kecurangan yang merugikan petani plasma. Sistem dirancang untuk menjadi "pengawas digital" terhadap celah ini.

*   **Proses (DAG Analitik):** 
    Pendekatan yang digunakan adalah Sistem Berbasis Aturan (*Rule-Based System*) bertingkat menggunakan gerbang logika *IF-THEN* pada pergerakan data historis tergelincir (*rolling window*). 
    Sistem mendefinisikan sebuah indikasi penimbunan sebagai keadaan "BENAR (TRUE)" *hanya jika* **ketiga syarat** yang sangat ketat ini terpenuhi secara serentak pada bulan berjalan: 
    1. Harga CPO Provinsi Riau terpantau **lebih rendah** dari bulan sebelumnya (Terjadi penurunan harga).
    2. Volume penjualan CPO oleh PKS tersebut **anjlok** secara drastis di bawah rata-rata tren penjualannya selama 3 bulan terakhir.
    3. Bukannya kosong, stok akhir di tangki timbun pabrik tersebut justru bertambah **naik** melampaui bulan lalu (menahan barang, tidak dijual).
    > *[PLACEHOLDER SCREENSHOT: Potongan Task Group penimbunan di Graph View Airflow ATAU diagram flowchart sederhana]*
    
    **Potongan Kode Penting (Logika Multi-Condition Penimbunan):**
    ```python
    # 1. Cek Penurunan Harga CPO
    kondisi_harga = df['harga_cpo'] < df['prev_harga']
    
    # 2. Cek Penjualan yang Anjlok di bawah historis rata-rata 3 bulan
    kondisi_jual  = df['volume_penjualan_ton'] < df['rata_rata_historis_ton']
    
    # 3. Cek Penumpukan Stok
    kondisi_stok  = df['stok_akhir_ton'] > df['prev_stok']
    
    # Operator bitwise AND (&) memastikan 3 syarat harus mutlak terjadi bersamaan
    df['indikasi_timbun'] = kondisi_harga & kondisi_jual & kondisi_stok
    ```
*   **Output:** Penanda *flag* boolean (*TRUE/FALSE*) yang seketika menjadi alert peringatan merah di dasbor eksekutif.
    > *[PLACEHOLDER SCREENSHOT: Tabel hasil deteksi penimbunan di database. Beri kotak merah pada baris di kolom `indikasi_timbun` yang bernilai `TRUE`]*

---

## 3.3 Serving Layer: Pembentukan Data Mart dan Penyajian
Layer presentasi yang menyederhanakan kompleksitas relasi Data Warehouse dengan ratusan juta baris menjadi bentuk tabel pipih (*flat table*) yang siap dikonsumsi instan oleh visualisasi dasbor.

### 3.3.1 Inisiasi dan Eksekusi DAG Mart
*   **Proses (DAG `dag7_datamart_refresh`):** 
    Jika dasbor visualisasi (BI) dibiarkan mengeksekusi operasi `SQL JOIN` ke 10 tabel DWH yang saling terkait secara *real-time* tiap kali pengguna membuka halaman, sistem akan mengalami *bottleneck* performa (*loading* sangat lama). 
    Untuk mencegah hal ini, DAG Datamart bertugas men-denormalisasi (menggabungkan ulang) tabel fakta dan dimensi ke dalam 4 objek *Materialized View* spesifik pada skema `datamart` di PostgreSQL. Pendekatan *Materialized View* dipilih karena mampu menyimpan hasil komputasi kueri berat secara fisik di dalam *disk*, yang hanya perlu di-*refresh* ulang satu kali sesaat setelah proses ETL hulu rampung.
    > *[PLACEHOLDER SCREENSHOT: Tampilan DBeaver yang menampilkan list Materialized View di dalam schema `datamart`]*
*   **Output:** Tabel spesifik domain (*Subject-Oriented*) yang memangkas latensi pembacaan data.
    > *[PLACEHOLDER SCREENSHOT: Tampilan waktu eksekusi query yang cepat (hitungan millisecond) saat me-load tabel datamart dibandingkan tabel awal]*

### 3.3.2 Visualisasi dan Dashboard Eksekutif
*   **Proses:** Aplikasi berbasis web interaktif dikembangkan menggunakan ekosistem *Plotly Dash* (Python). Dasbor ini tidak hanya menampilkan pelaporan tabel pasif, tetapi berhasil memetakan geometri Peta *Choropleth* (berformat GeoJSON) tingkat kabupaten Provinsi Riau yang terhubung langsung dengan *Data Mart* NDVI. Dasbor mengimplementasikan navigasi berlapis dan komponen UI/UX analitikal sehingga dinas dapat langsung memfilter wilayah untuk mendapatkan profil *insight*.
    > *[PLACEHOLDER SCREENSHOT: Cuplikan antarmuka dasbor interaktif, khususnya halaman depan atau Peta Choropleth yang merender GeoJSON]*

---

## 3.4 Backup Layer: Pencadangan Sistem DWH
Sebagai infrastruktur berskala provinsi, sistem wajib dilengkapi mekanisme mitigasi bencana (*disaster recovery*) untuk menghindari kehilangan data sejarah (*Data Loss*) akibat insiden kerusakan *server* atau korupsi data.

### 3.4.1 Pencadangan Otomatis ke Object Storage (MinIO/S3)
*   **Proses (DAG `dag8_minio_backup`):** 
    Alur ini diotomatisasi secara *scheduled* pada akhir pekan (*@weekly*). DAG akan memanggil kontainer independen yang mengeksekusi utilitas bawaan PostgreSQL yaitu `pg_dump`. Seluruh isi struktur skema dan data diringkas ke dalam bentuk *SQL Dump*. Untuk meminimalisir beban jaringan, file *dump* dikompresi menjadi arsip `backup_YYYYMMDD.sql.gz`. 
    Arsip terkompresi ini kemudian dikirim (diunggah) keluar dari *server database* lokal menggunakan antarmuka protokol S3 (via *library Boto3*) menuju wadah *Object Storage* (MinIO) di lingkungan jaringan terpisah.
    > *[PLACEHOLDER SCREENSHOT: Graph View DAG Backup atau tampilan bucket penyimpanan MinIO yang memuat list file backup berakhiran .gz]*
    
    **Potongan Kode Penting (Boto3 MinIO Upload):**
    ```python
    import boto3
    # Menggunakan kompatibilitas S3 untuk mengunggah ke MinIO
    s3_client = boto3.client('s3', 
        endpoint_url='http://minio:9000', 
        aws_access_key_id='...', 
        aws_secret_access_key='...'
    )
    # Upload snapshot .gz ke dalam bucket 'dwh-backups'
    s3_client.upload_file('/tmp/backup.sql.gz', 'dwh-backups', 'backup_2026.sql.gz')
    ```
*   **Output:** Arsip cadangan historis harian/mingguan yang diisolasi dengan aman di dalam *cloud storage*.
    > *[PLACEHOLDER SCREENSHOT: Tampilan antarmuka Console S3/MinIO pada bucket `dwh-backups` dengan kotak merah pada file yang berhasil masuk]*
