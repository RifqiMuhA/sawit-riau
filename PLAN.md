# PLAN.md — Sistem Monitoring Perkebunan Sawit Riau
**Dokumen ini dibuat agar AI lain / session baru dapat melanjutkan pengembangan proyek dari titik yang tepat.**  
*Last updated: 2026-05-01*

---

## 0. Status Pengerjaan Saat Ini

### 0.1 Infrastruktur Docker & Database

| # | Komponen | Status | File Utama |
|---|---|---|---|
| I-1 | Docker Compose — 9 service (`postgres`, `postgis`, `mysql`, `mongodb`, `minio`, `airflow`×3, `pgadmin`) | ✅ Selesai | `docker-compose.yml` |
| I-2 | GEE Authentication (`gee-credentials.json`) | ✅ Selesai | `gee-credentials.json` |
| I-3 | Volume `data-perusahaan` (gantikan `data-sintesis` yang lama) | ✅ Selesai | `docker-compose.yml` |
| I-4 | Volume `data-pdf` (read-write, download PDF DAG 3 otomatis) | ✅ Selesai | `docker-compose.yml` |
| I-5 | **DDL MySQL** — 6 DB (a-01 s/d a-03, b-04 s/d b-06) × 6 tabel: `perusahaan`, `karyawan/pegawai`, `varietas`, `kebun/blok_kebun`, `laporan_bulanan/rekap_bulanan`, `jadwal_panen/rencana_panen` | ✅ Selesai | `init-sql/mysql/00_schema_oltp.sql` |
| I-6 | **DDL PostgreSQL OLTP** — 6 schema (`db_pks_c_07` s/d `db_pks_d_12`) × 6 tabel: `perusahaan_c/data_perusahaan`, `karyawan_c/data_karyawan`, `varietas`, `lahan/kebun_produksi`, `laporan/produksi_bulanan`, `target_panen/realisasi_panen` | ✅ Selesai | `init-sql/postgres/00_schema_oltp.sql` |
| I-7 | **DDL DWH PostGIS** — 15 tabel total | ✅ Selesai | `init-sql/postgis/00_schema_dwh.sql` |
| I-8 | **Data Load MySQL** — 6 file DML PKS-A & PKS-B di-load ke container | ✅ Selesai | `data-perusahaan/SQL_PKS-[AB]-*.sql` |
| I-9 | **Data Load PostgreSQL OLTP** — 6 file DML PKS-C & PKS-D di-load ke container | ✅ Selesai | `data-perusahaan/SQL_PKS-[CD]-*.sql` |
| I-10 | **Data Load MongoDB** — 1290 dokumen di `sawit_alerts.log_alert_harian` | ✅ Selesai | `data-perusahaan/MongoDB_Alerts_PKS-*.json` |
| I-11 | GeoJSON 12 kab/kota Riau → `dim_kabupaten.geometry` | ✅ Selesai | `load_geojson_to_postgis.py` |

### 0.2 Pipeline DAG

| DAG | Nama & Fungsi | Sumber → Target | Jadwal | Status |
|---|---|---|---|---|
| **DAG 1** | **NDVI Extraction** — Ambil NDVI bulanan Sentinel-2 per kabupaten via GEE | GEE Sentinel-2 → `fact_ndvi` | `@monthly`, catchup=True (sejak 2023-01) | ✅ **Selesai** |
| **DAG 2** | **Produksi ETL** — ETL laporan produksi + operasional dari 12 OLTP DB + 12 Excel PKS + load `dim_kebun` | MySQL(A,B) + PgSQL(C,D) + Excel → `fact_produksi`, `fact_operasional`, `dim_karyawan`, `dim_kebun` | `@monthly` | ✅ **Selesai** |
| **DAG 3** | **Harga CPO** — Scraping PDF harga TBS dari disbun.riau.go.id per tahun (2023–2025), parsing inkonsistensi format antar tahun | PDF Disbun Riau → `fact_harga_cpo` | Manual trigger via UI (`{"tahun": YYYY}`) | ✅ **Selesai** |
| **DAG 4** | **Analitik** — K-Means k=3 produktivitas, flagging penimbunan (3 kondisi), gap analysis panen, isi `status_kebun` di `fact_ndvi` | `fact_produksi`, `fact_operasional`, `fact_harga_cpo`, `fact_ndvi`, `fact_panen` → update kolom | `@monthly` (setelah DAG 2,5,6) | ⬜ **Belum** |
| **DAG 5** | **Panen ETL** — ETL realisasi vs target panen dari 4 tipe OLTP + hitung `gap_persen` | MySQL `jadwal_panen`/`rencana_panen` + PgSQL `target_panen`/`realisasi_panen` → `fact_panen` | `@monthly` | ✅ **Selesai** |
| **DAG 6** | **Alert ETL** — Agregasi log alert harian MongoDB per (perusahaan, bulan) | MongoDB `log_alert_harian` → `fact_rendemen` | `@monthly` | ✅ **Selesai** |
| **DAG 7** | **Datamart Refresh** *(opsional)* — Materialized view / summary untuk dashboard | DWH fact tables → view/summary | `@monthly` (setelah DAG 4) | ⬜ **Belum** |
| **DAG 8** | **Backup DWH** — Snapshot PostGIS ke MinIO S3 | `sawit_dwh` → MinIO `sawit-backup` | `@weekly` | ⬜ **Belum** |

### 0.3 Alur Dependency DAG

```
DAG 1 (NDVI) ──────────────────────────────────────────┐
DAG 2 (Produksi + dim_kebun) ──────────────────────────┤
DAG 3 (Harga CPO) ─────────────────────────────────────┼──▶ DAG 4 (Analitik) ──▶ DAG 7 (Refresh)
DAG 5 (Panen ETL) ─────────────────────────────────────┤
DAG 6 (Alert MongoDB) ─────────────────────────────────┘

DAG 8 (Backup MinIO) — independen @weekly
```

> ⚠️ **DAG 5 depend pada DAG 2** — `fact_panen` FK ke `dim_kebun`. Jalankan DAG 2 dulu hingga task `load_dim_kebun` sukses sebelum trigger DAG 5.

### 0.4 Next Priority

1. **DAG 4** — Analitik: K-Means + flagging penimbunan + gap analysis + isi `status_kebun`
2. **DAG 8** — Backup MinIO (opsional)
3. **Dashboard** — Streamlit / Superset

---

## 1. Ringkasan Proyek

**Decision Support System (DSS)** untuk monitoring perkebunan kelapa sawit di Provinsi Riau.  
Menjawab 3 pertanyaan bisnis utama:

| Tujuan | Pertanyaan | Metode |
|---|---|---|
| **T1 — Status Kebun** | Kabupaten mana yang kondisi kebunnya memburuk? | NDVI Sentinel-2 via GEE, threshold historis |
| **T2 — Gap Produksi** | Perusahaan mana yang underperform? | K-Means k=3 pada produktivitas (ton/ha) |
| **T3 — Deteksi Timbun** | Apakah perusahaan menahan penjualan saat harga CPO turun? | 3 kondisi serentak: harga turun + volume jual turun + stok naik |

---

## 2. Status Infrastruktur (SUDAH SELESAI ✅)

### 2.1 Docker Services

Semua container berjalan via `docker compose up -d` di direktori proyek.

| Container | Image | Port Host | Fungsi |
|---|---|---|---|
| `airflow_postgres` | postgres:14 | 5432 | Metadata Airflow |
| `postgis_db` | postgis/postgis:14-3.3 | **5433** | Data Warehouse (sawit_dwh) |
| `postgres_oltp` | postgres:14 | **5434** | OLTP Perusahaan C & D |
| `mysql_oltp` | mysql:8.0 | **3306** | OLTP Perusahaan A & B |
| `minio` | minio/minio | 9000, 9001 | Object storage backup |
| `airflow_web` | apache/airflow:2.9.0 | **8080** | Airflow UI |
| `airflow_scheduler` | apache/airflow:2.9.0 | — | Scheduler |
| `pgadmin` | dpage/pgadmin4 | **5050** | GUI PostGIS DWH |

### 2.2 Akses GUI

| Tool | URL | Kredensial |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@admin.com / admin |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| MySQL Workbench | localhost:3306 | root / root |

### 2.3 File Konfigurasi

- **`docker-compose.yml`** — definisi semua service
- **`.env`** — semua kredensial dan variabel lingkungan
- **`.gitignore`** — `.env` sudah di-ignore

### 2.4 Pip Packages di Airflow

Sudah dikonfigurasi via `_PIP_ADDITIONAL_REQUIREMENTS` di `docker-compose.yml`:
```
apache-airflow-providers-postgres
apache-airflow-providers-mysql
mysql-connector-python
psycopg2-binary
boto3
earthengine-api
pdfplumber
bautifulsoup4
scikit-learn
openpyxl
requests
pymongo
```

---

## 3. Status Database (SUDAH SELESAI ✅)

### 3.1 MySQL OLTP — Perusahaan A & B

**Container:** `mysql_oltp` | **Port:** 3306 | **Credentials:** root/root atau pks_user/pks_pass

| Database | Perusahaan | Tabel Utama | Anomali |
|---|---|---|---|
| `` `a-01-kampar` `` | PT Sawit Makmur Kampar | perusahaan, karyawan, laporan_bulanan | Duplikat baris 2024-03 (v1/v2) |
| `` `a-02-pelalawan` `` | PT Pelalawan Agro | (sama) | — |
| `` `a-03-siak` `` | PT Siak Palma Sejahtera | (sama) | — |
| `` `b-04-indragiri-hulu` `` | PT Inhu Lestari | perusahaan_b, pegawai, rekap_bulanan | NULL luas_panen 2024-06/07 |
| `` `b-05-kuansing` `` | PT Kuansing Mas | (sama) | NULL luas_panen 2024-06/07 |
| `` `b-06-indragiri-hilir` `` | PT Inhil Gemilang | (sama) | NULL luas_panen 2024-06/07 |

**Perbedaan kritis Tipe A vs B untuk ETL:**
- A: satuan **TON**, kolom `periode CHAR(7)` (YYYY-MM), nama `perusahaan`
- B: satuan **KILOGRAM** (÷1000 saat ETL), kolom `bulan VARCHAR(7)`, nama `perusahaan_b` + `rekap_bulanan`

### 3.2 PostgreSQL OLTP — Perusahaan C & D

**Container:** `postgres_oltp` | **Port:** 5434 | **Database:** `oltp_db`  
**Credentials:** oltp_user / oltp_pass

| Schema | Perusahaan | Tabel Utama | Anomali |
|---|---|---|---|
| `"c-07-bengkalis"` | CV Bengkalis Sawit | perusahaan_c, karyawan_c, laporan | produksi = 0.00 (Nov 2024, Mar 2025) |
| `"c-08-rokan-hilir"` | PT Rohil Abadi | (sama) | produksi = 0.00 |
| `"c-09-meranti"` | PT Meranti Jaya | (sama) | produksi = 0.00 |
| `"d-10-rokan-hulu"` | PT Rohul Palma | data_perusahaan, data_karyawan, produksi_bulanan | NULL stok 2024-12, duplikat 2023-08 |
| `"d-11-pekanbaru"` | PT Pekanbaru Mill | (sama) | NULL stok 2024-12 |
| `"d-12-dumai"` | PT Dumai Indah | (sama) | NULL stok 2024-12 |

**Perbedaan kritis Tipe C vs D untuk ETL:**
- C: `tgl_laporan DATE` → ETL: `TO_CHAR(tgl_laporan, 'YYYY-MM')`, kolom `produksi` (singkat), `stok_akhir_tahun` (misleading, isinya stok akhir bulan), `aktif BOOLEAN`
- D: `tahun SMALLINT` + `bulan SMALLINT` terpisah → ETL: `LPAD(bulan::TEXT, 2, '0') || '-' || tahun`, deduplikasi ambil nilai terbesar

### 3.3 PostGIS DWH — sawit_dwh

**Container:** `postgis_db` | **Port:** 5433 | **Database:** `sawit_dwh`  
**Credentials:** dwh / dwh

**Tabel yang sudah ada dan terisi seed:**

| Tabel | Status | Keterangan |
|---|---|---|
| `dim_kabupaten` | ✅ Seed (12 rows) | **geometry = NULL** — perlu diisi GeoJSON |
| `map_kabupaten_alias` | ✅ Seed | Mapping variasi nama → kode_wilayah |
| `dim_perusahaan` | ✅ Seed (12 rows) | Semua 12 PKS |
| `dim_waktu` | ✅ Seed (36 rows) | 2023-01 s/d 2025-12 |
| `dim_karyawan` | ✅ Empty | Diisi opsional dari OLTP |
| `fact_ndvi` | ⬜ Empty | Diisi DAG 1 |
| `fact_produksi` | ⬜ Empty | Diisi DAG 2 |
| `fact_operasional` | ⬜ Empty | Diisi DAG 2 |
| `fact_harga_cpo` | ⬜ Empty | Diisi DAG 3 |

### 3.4 Koneksi Airflow (SUDAH DIBUAT ✅)

| Conn ID | Type | Host | Port | Schema/DB |
|---|---|---|---|---|
| `mysql_oltp` | MySQL | mysql-oltp | 3306 | (kosong, dinamis) |
| `postgres_oltp` | Postgres | postgres-oltp | 5432 | oltp_db |
| `postgis_dwh` | Postgres | postgis_db | 5432 | sawit_dwh |
| `minio_s3` | Amazon Web Service | — | — | extra: endpoint_url, keys |

### 3.5 MinIO

- **Bucket sudah dibuat** (nama bucket yang dipakai: cek di MinIO Console http://localhost:9001)
- Endpoint internal: `http://minio:9000`
- Credentials: minio / minio123

---

## 4. Struktur Direktori Proyek

```
Project/
├── .env                          # Semua kredensial
├── .gitignore
├── docker-compose.yml            # Definisi semua service
├── brief_sistem_monitoring_sawit_riau_new.md  # Requirement lengkap
├── PLAN.md                       # File ini
│
├── dags/                         # ← DAG Airflow (BELUM DIBUAT)
│   ├── dag1_ndvi_extraction.py
│   ├── dag2_produksi_etl.py
│   ├── dag3_harga_cpo.py
│   ├── dag4_analitik.py
│   └── dag5_backup_dwh.py
│
├── data-sintesis/                # DML sintetis 12 PKS + 12 Excel
│   ├── SQL_PKS-A-01_Kab._Kampar.sql
│   ├── ... (12 SQL files, sudah di-load ke container)
│   ├── Excel_PKS-A-01_Kab._Kampar.xlsx
│   └── ... (12 Excel files, belum di-load ke DWH)
│
├── init-sql/                     # DDL yang auto-run saat container start
│   ├── mysql/
│   │   └── 00_schema_oltp.sql   # DDL 6 database MySQL (A & B)
│   ├── postgres/
│   │   └── 00_schema_oltp.sql   # DDL 6 schema PostgreSQL (C & D)
│   └── postgis/
│       └── 00_schema_dwh.sql    # DDL DWH + seed dim_kabupaten, dim_perusahaan, dim_waktu
```

---

## 5. TODO List — Yang Belum Dibuat

### 5.1 🔴 BLOCKER — GEE Authentication

Sebelum DAG 1 bisa jalan, Google Earth Engine harus diautentikasi:

**Opsi A (Development):** Interactive auth di dalam container
```bash
docker exec -it airflow_scheduler bash
earthengine authenticate --quiet
# Ikuti URL browser, paste token
```

**Opsi B (Production/Stabil):** Service Account JSON
1. Buat service account di Google Cloud Console dengan akses GEE
2. Download key JSON → simpan sebagai `./gee-credentials.json`
3. Tambah volume di `docker-compose.yml` untuk airflow-common:
```yaml
volumes:
  - ./gee-credentials.json:/home/airflow/.config/earthengine/credentials:ro
```

---

### 5.2 🔴 BLOCKER — Load Geometry Kabupaten ke PostGIS

`dim_kabupaten.geometry` saat ini NULL. DAG 1 perlu polygon untuk spatial join.

```bash
# Jika punya GeoJSON riau_kabupaten.geojson:
ogr2ogr -f "PostgreSQL" PG:"host=localhost port=5433 dbname=sawit_dwh user=dwh password=dwh" \
  riau_kabupaten.geojson -nln dim_kabupaten_temp
# Lalu UPDATE dim_kabupaten SET geometry = (SELECT geom FROM dim_kabupaten_temp WHERE kode = dim_kabupaten.kode_wilayah)
```

Atau via psql dengan `ST_GeomFromGeoJSON()`.

---

### 5.3 DAG 2 — ETL Produksi (Prioritas Tertinggi)

**File:** `dags/dag2_produksi_etl.py`

**Logika ETL per sumber:**

#### MySQL Tipe A (laporan_bulanan)
```python
# Koneksi per database (dinamis)
for db in ['a-01-kampar', 'a-02-pelalawan', 'a-03-siak']:
    hook = MySqlHook(mysql_conn_id='mysql_oltp', schema=db)
    df = hook.get_pandas_df("""
        SELECT perusahaan_id, periode, produksi_tbs_ton,
               luas_panen_ha, stok_akhir_ton, volume_penjualan_ton
        FROM laporan_bulanan
        WHERE laporan_id NOT LIKE '%-v2'  -- deduplikasi: buang v2, ambil v1
    """)
    # periode sudah CHAR(7) YYYY-MM → langsung pakai
```

#### MySQL Tipe B (rekap_bulanan)
```python
for db in ['b-04-indragiri-hulu', 'b-05-kuansing', 'b-06-indragiri-hilir']:
    hook = MySqlHook(mysql_conn_id='mysql_oltp', schema=db)
    df = hook.get_pandas_df("""
        SELECT id_pks as perusahaan_id, bulan as periode,
               produksi_tbs / 1000 as produksi_tbs_ton,   -- KG → TON
               luas_panen as luas_panen_ha,                -- forward-fill NULL di pandas
               stok_akhir / 1000 as stok_akhir_ton,
               volume_jual / 1000 as volume_penjualan_ton
        FROM rekap_bulanan
    """)
    df['luas_panen_ha'] = df['luas_panen_ha'].fillna(method='ffill')  # forward-fill NULL
```

#### PostgreSQL Tipe C (laporan)
```python
for schema in ['c-07-bengkalis', 'c-08-rokan-hilir', 'c-09-meranti']:
    hook = PostgresHook(postgres_conn_id='postgres_oltp')
    df = hook.get_pandas_df(f"""
        SET search_path TO "{schema}";
        SELECT kode_perusahaan as perusahaan_id,
               TO_CHAR(tgl_laporan, 'YYYY-MM') as periode,  -- DATE → YYYY-MM
               NULLIF(produksi, 0) as produksi_tbs_ton,      -- 0.00 → NULL
               luas_ha as luas_panen_ha,
               stok_akhir_tahun as stok_akhir_ton,
               penjualan_ton as volume_penjualan_ton
        FROM laporan
        WHERE produksi > 0   -- filter nilai tidak valid
    """)
```

#### PostgreSQL Tipe D (produksi_bulanan)
```python
for schema in ['d-10-rokan-hulu', 'd-11-pekanbaru', 'd-12-dumai']:
    hook = PostgresHook(postgres_conn_id='postgres_oltp')
    df = hook.get_pandas_df(f"""
        SET search_path TO "{schema}";
        SELECT DISTINCT ON (perusahaan_id, tahun, bulan)
               perusahaan_id,
               LPAD(tahun::TEXT, 4, '0') || '-' || LPAD(bulan::TEXT, 2, '0') as periode,
               produksi_tbs_ton,
               luas_panen_ha,
               stok_akhir_ton,   -- bisa NULL → flag 'missing'
               volume_penjualan as volume_penjualan_ton
        FROM produksi_bulanan
        ORDER BY perusahaan_id, tahun, bulan, produksi_tbs_ton DESC  -- deduplikasi: ambil nilai terbesar
    """)
    df['stok_flag'] = df['stok_akhir_ton'].apply(lambda x: 'missing' if pd.isna(x) else None)
```

**Output ke DWH:**
- `fact_produksi`: (perusahaan_id, periode, kode_wilayah, produksi_tbs_ton, luas_panen_ha, produktivitas=produksi/luas)
- `fact_operasional`: (perusahaan_id, periode, stok_akhir_ton, volume_penjualan_ton, stok_flag)

---

### 5.4 DAG 2B — ETL Excel PKS

**File Excel:** `data-sintesis/Excel_PKS-*.xlsx` (12 file)  
**Note:** Perlu diketahui struktur kolom Excel dulu — buka satu file untuk verifikasi.

```python
import openpyxl
from airflow.models import Variable

excel_dir = '/opt/airflow/data-sintesis'  # perlu mount ke container!
for fname in glob.glob(f'{excel_dir}/Excel_PKS-*.xlsx'):
    df = pd.read_excel(fname, engine='openpyxl')
    # Map nama kabupaten → kode_wilayah via map_kabupaten_alias di DWH
    # Lalu upsert ke fact_produksi
```

> ⚠️ **PENTING:** Direktori `data-sintesis/` belum di-mount ke container Airflow.  
> Perlu tambahkan di `docker-compose.yml` bagian `x-airflow-common.volumes`:
> ```yaml
> - ./data-sintesis:/opt/airflow/data-sintesis:ro
> ```

---

### 5.5 DAG 3 — Scraping Harga CPO Disbun

**File:** `dags/dag3_harga_cpo.py`

```python
import pdfplumber, requests

# URL Disbun Riau — verifikasi URL aktual dulu
BASE_URL = "https://disbun.riau.go.id/..."
for tahun in [2023, 2024, 2025]:
    url = f"{BASE_URL}/{tahun}.pdf"
    resp = requests.get(url, timeout=30)
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            # Parse tabel harga CPO → DataFrame
            # Kolom: periode (YYYY-MM), harga_cpo (Rp/kg)

# Upsert ke fact_harga_cpo
hook = PostgresHook(postgres_conn_id='postgis_dwh')
hook.run("INSERT INTO fact_harga_cpo ... ON CONFLICT DO UPDATE ...")
```

---

### 5.6 DAG 1 — Ekstraksi NDVI GEE

**File:** `dags/dag1_ndvi_extraction.py`  
**Dependency:** GEE auth + geometry polygon di dim_kabupaten

```python
import ee

ee.Initialize()  # atau ee.Initialize(credentials=...) dengan service account

# Untuk setiap kabupaten dan periode:
for kode_wilayah, geom in kabupaten_geometries.items():
    for periode in ['2023-01', ..., '2025-12']:
        year, month = periode.split('-')
        start = f"{year}-{month}-01"
        end   = (datetime(int(year), int(month), 1) + relativedelta(months=1)).strftime('%Y-%m-%d')

        s2 = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterBounds(geom)
                .filterDate(start, end)
                .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20)))

        ndvi = s2.median().normalizedDifference(['B8', 'B4'])
        stats = ndvi.reduceRegion(ee.Reducer.mean().combine(ee.Reducer.stdDev(), sharedInputs=True), geom, 10)
        # Insert ke fact_ndvi
```

---

### 5.7 DAG 4 — Analitik & Flagging

**File:** `dags/dag4_analitik.py`

```python
from sklearn.cluster import KMeans
import numpy as np

# --- K-Means produktivitas ---
hook = PostgresHook(postgres_conn_id='postgis_dwh')
df = hook.get_pandas_df("SELECT perusahaan_id, periode, produktivitas FROM fact_produksi WHERE produktivitas IS NOT NULL")

for periode in df['periode'].unique():
    subset = df[df['periode'] == periode][['perusahaan_id', 'produktivitas']].dropna()
    if len(subset) < 3:
        continue
    km = KMeans(n_clusters=3, random_state=42, n_init=10)
    labels = km.fit_predict(subset[['produktivitas']])
    centers = km.cluster_centers_.flatten()
    sorted_idx = centers.argsort()
    label_map = {sorted_idx[0]: 'underperform', sorted_idx[1]: 'average', sorted_idx[2]: 'overperform'}
    subset['cluster_produksi'] = [label_map[l] for l in labels]
    # UPDATE fact_produksi SET cluster_produksi = ... WHERE perusahaan_id = ... AND periode = ...

# --- NDVI status ---
# UPDATE fact_ndvi SET status_kebun = CASE WHEN ndvi_mean > 0.6 THEN 'normal'
#                                          WHEN ndvi_mean > 0.4 THEN 'menurun'
#                                          ELSE 'kritis' END

# --- Indikasi timbun ---
# JOIN fact_operasional + fact_harga_cpo per periode
# Flag indikasi_timbun = TRUE jika:
#   harga_cpo[t] < harga_cpo[t-1] AND
#   volume_penjualan_ton[t] < avg(volume_penjualan_ton[t-3:t-1]) AND
#   stok_akhir_ton[t] > stok_akhir_ton[t-1]
```

---

### 5.8 DAG 5 — Backup DWH ke MinIO

**File:** `dags/dag5_backup_dwh.py`

```python
import boto3, io
from airflow.hooks.base import BaseHook

conn = BaseHook.get_connection('minio_s3')
extra = conn.extra_dejson
s3 = boto3.client('s3',
    endpoint_url=extra['endpoint_url'],
    aws_access_key_id=extra['aws_access_key_id'],
    aws_secret_access_key=extra['aws_secret_access_key'])

hook = PostgresHook(postgres_conn_id='postgis_dwh')
for tabel in ['fact_ndvi', 'fact_produksi', 'fact_operasional', 'fact_harga_cpo']:
    df = hook.get_pandas_df(f"SELECT * FROM {tabel}")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    tanggal = datetime.now().strftime('%Y%m%d')
    s3.put_object(Bucket='NAMA_BUCKET_MINIO', Key=f"backup/{tanggal}/{tabel}.parquet", Body=buf)
```

> **GANTI `NAMA_BUCKET_MINIO`** dengan nama bucket yang sudah dibuat di MinIO Console.

---

### 5.9 Dashboard

**Pilihan teknologi:** Streamlit (lebih cepat development)

**File:** `dashboard/app.py`

Koneksi ke PostGIS:
```python
import psycopg2
conn = psycopg2.connect(host='localhost', port=5433, dbname='sawit_dwh', user='dwh', password='dwh')
```

**Halaman yang direncanakan:**
1. **Peta Kondisi Kebun** — choropleth NDVI per kabupaten (butuh geometry)
2. **Ranking Produktivitas** — bar chart cluster K-Means per periode
3. **Deteksi Timbun** — tabel perusahaan dengan flag + grafik stok vs harga CPO
4. **Tren Produksi** — line chart produksi bulanan per perusahaan

---

## 6. Urutan Pengerjaan yang Disarankan

```
✅ 1. Mount data-sintesis ke Airflow (docker-compose.yml)
✅ 2. DAG 2 — ETL OLTP (MySQL A/B, PostgreSQL C/D) + Excel PKS → DWH
       Fix: GRANT SELECT pks_user ke semua 6 MySQL DB (init-sql/mysql/00_schema_oltp.sql)
✅ 3. Load geometry GeoJSON ke dim_kabupaten (load_geojson_to_postgis.py)
✅ 4. GEE Authentication (gee-credentials.json persistent)
✅ 5. DAG 3 — Harga CPO PDF Disbun → fact_harga_cpo
       Cara pakai: letakkan data-pdf/{tahun}.pdf, trigger manual conf {"tahun": YYYY}
✅ 6. DAG 1 — NDVI extraction (fact_ndvi) — dependency: GEE auth + dim_kabupaten geometry
⏳ 7. DAG 4 — Analitik (K-Means, flag timbun, status NDVI)
⬜ 8. DAG 5 — Backup MinIO
⬜ 9. Dashboard Streamlit
```

---

## 7. Catatan Penting untuk AI yang Melanjutkan

1. **Database MySQL dengan nama mengandung hyphen** (misal `a-01-kampar`) harus selalu pakai backtick di query MySQL: `` `a-01-kampar` ``
2. **Schema PostgreSQL dengan hyphen** harus pakai double-quote: `"c-07-bengkalis"`  
3. **Koneksi MySQL Airflow** pakai `schema=db_name` di `MySqlHook` untuk ganti database
4. **Koneksi PostgreSQL Tipe C/D** pakai `SET search_path TO "schema"` sebelum query, bukan lewat parameter `schema` di hook
5. **Produksi B = KILOGRAM** — selalu bagi 1000 sebelum masuk DWH
6. **Produksi C = 0.00** bukan NULL — filter dengan `WHERE produksi > 0` atau `NULLIF(produksi, 0)`
7. **Deduplikasi A**: ambil laporan_id tanpa `-v2` suffix
8. **Deduplikasi D**: `DISTINCT ON (perusahaan_id, tahun, bulan) ORDER BY produksi_tbs_ton DESC`
9. **stok_flag**: set `'missing'` jika `stok_akhir_ton IS NULL` (khusus Perusahaan D)
10. **dim_waktu** sudah berisi 2023-01 s/d 2025-12 — jangan insert ulang, pakai ON CONFLICT DO NOTHING

---

## 8. Mapping Lengkap Perusahaan

| PKS ID | Nama | Kabupaten | Kode Wilayah | Tipe DB | Database/Schema |
|---|---|---|---|---|---|
| PKS-A-01 | PT Sawit Makmur Kampar | Kab. Kampar | 1406 | MySQL A | `a-01-kampar` |
| PKS-A-02 | PT Pelalawan Agro | Kab. Pelalawan | 1404 | MySQL A | `a-02-pelalawan` |
| PKS-A-03 | PT Siak Palma Sejahtera | Kab. Siak | 1405 | MySQL A | `a-03-siak` |
| PKS-B-04 | PT Inhu Lestari | Kab. Indragiri Hulu | 1402 | MySQL B | `b-04-indragiri-hulu` |
| PKS-B-05 | PT Kuansing Mas | Kab. Kuantan Singingi | 1401 | MySQL B | `b-05-kuansing` |
| PKS-B-06 | PT Inhil Gemilang | Kab. Indragiri Hilir | 1403 | MySQL B | `b-06-indragiri-hilir` |
| PKS-C-07 | CV Bengkalis Sawit | Kab. Bengkalis | 1408 | PostgreSQL C | `"c-07-bengkalis"` |
| PKS-C-08 | PT Rohil Abadi | Kab. Rokan Hilir | 1409 | PostgreSQL C | `"c-08-rokan-hilir"` |
| PKS-C-09 | PT Meranti Jaya | Kab. Kepulauan Meranti | 1410 | PostgreSQL C | `"c-09-meranti"` |
| PKS-D-10 | PT Rohul Palma | Kab. Rokan Hulu | 1407 | PostgreSQL D | `"d-10-rokan-hulu"` |
| PKS-D-11 | PT Pekanbaru Mill | Kota Pekanbaru | 1471 | PostgreSQL D | `"d-11-pekanbaru"` |
| PKS-D-12 | PT Dumai Indah | Kota Dumai | 1472 | PostgreSQL D | `"d-12-dumai"` |
