"""
DAG 2 — ETL Produksi & Operasional: OLTP + Excel → DWH
========================================================
Sumber  : 6 MySQL DB (A & B) + 6 PostgreSQL schema (C & D) + 12 Excel PKS
Target  : fact_produksi + fact_operasional di sawit_dwh (PostGIS)
Jadwal  : @monthly (atau manual trigger)

Transformasi kritis per tipe:
  A (MySQL) : periode Jan-2023 → 2023-01 | satuan TON | deduplikasi v1/v2
  B (MySQL) : periode 01/01/2023 → 2023-01 | KG ÷ 1000 → TON | forward-fill NULL luas
  C (PG)    : DATE → YYYY-MM | filter produksi > 0 (nilai 0 = missing)
  D (PG)    : tahun+bulan terpisah → YYYY-MM | DISTINCT ON deduplikasi | NULL stok → flag
  Excel A   : Nama_Perusahaan, Kabupaten, Periode (Jan-2023), Produksi_TBS_Ton, Luas_Panen_Ha
  Excel B   : NAMA_PKS, KAB, TANGGAL (01/01/2023), PRODUKSI_KG÷1000, LUAS_HA
  Excel C/D : header baris ke-3 | perusahaan, kabupaten, periode_lap(YYYY-MM), tbs_ton, ha_aktif
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# KONSTANTA
# ─────────────────────────────────────────────────────────────

MYSQL_A_DATABASES  = ["a-01-kampar",        "a-02-pelalawan",      "a-03-siak"]
MYSQL_B_DATABASES  = ["b-04-indragiri-hulu", "b-05-kuansing",       "b-06-indragiri-hilir"]
PG_C_SCHEMAS       = ["c-07-bengkalis",      "c-08-rokan-hilir",    "c-09-meranti"]
PG_D_SCHEMAS       = ["d-10-rokan-hulu",     "d-11-pekanbaru",      "d-12-dumai"]
EXCEL_DIR          = "/opt/airflow/data-perusahaan"

EXCEL_PERUSAHAAN_MAP = {
    # nama di Excel → perusahaan_id di DWH
    "PT Sawit Makmur Kampar"   : "PKS-A-01",
    "PT Pelalawan Agro"        : "PKS-A-02",
    "PT Siak Palma Sejahtera"  : "PKS-A-03",
    "PT Inhu Lestari"          : "PKS-B-04",
    "PT Kuansing Mas"          : "PKS-B-05",
    "PT Inhil Gemilang"        : "PKS-B-06",
    "CV Bengkalis Sawit"       : "PKS-C-07",
    "PT Rohil Abadi"           : "PKS-C-08",
    "PT Meranti Jaya"          : "PKS-C-09",
    "PT Rohul Palma"           : "PKS-D-10",
    "PT Pekanbaru Mill"        : "PKS-D-11",
    "PT Dumai Indah"           : "PKS-D-12",
}

PERUSAHAAN_WILAYAH_MAP = {
    "PKS-A-01": "1406", "PKS-A-02": "1404", "PKS-A-03": "1405",
    "PKS-B-04": "1402", "PKS-B-05": "1401", "PKS-B-06": "1403",
    "PKS-C-07": "1408", "PKS-C-08": "1409", "PKS-C-09": "1410",
    "PKS-D-10": "1407", "PKS-D-11": "1471", "PKS-D-12": "1472",
}

MONTH_MAP_ID = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}

# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────

def parse_periode_a(raw: str) -> str:
    """'Jan-2023' → '2023-01'"""
    parts = str(raw).strip().split("-")
    return f"{parts[1]}-{MONTH_MAP_ID.get(parts[0], '01')}"


def parse_periode_b(raw) -> str:
    """'01/01/2023' atau datetime → '2023-01'"""
    if isinstance(raw, (datetime,)):
        return raw.strftime("%Y-%m")
    dt = datetime.strptime(str(raw).strip(), "%d/%m/%Y")
    return dt.strftime("%Y-%m")


def upsert_fact_produksi(dwh_hook: PostgresHook, df: pd.DataFrame) -> int:
    """
    Upsert ke fact_produksi.
    df harus punya kolom: perusahaan_id, periode, kode_wilayah,
                          produksi_tbs_ton, luas_panen_ha, produktivitas
    """
    if df.empty:
        return 0
    rows = df.to_dict("records")
    sql = """
        INSERT INTO fact_produksi
            (perusahaan_id, periode, kode_wilayah, produksi_tbs_ton, luas_panen_ha, produktivitas)
        VALUES (%(perusahaan_id)s, %(periode)s, %(kode_wilayah)s,
                %(produksi_tbs_ton)s, %(luas_panen_ha)s, %(produktivitas)s)
        ON CONFLICT (perusahaan_id, periode) DO UPDATE SET
            produksi_tbs_ton = EXCLUDED.produksi_tbs_ton,
            luas_panen_ha    = COALESCE(NULLIF(EXCLUDED.luas_panen_ha, 0), fact_produksi.luas_panen_ha),
            produktivitas    = COALESCE(NULLIF(EXCLUDED.produktivitas, 0), fact_produksi.produktivitas)
    """
    conn = dwh_hook.get_conn()
    cur  = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def upsert_fact_operasional(dwh_hook: PostgresHook, df: pd.DataFrame) -> int:
    """
    Upsert ke fact_operasional.
    df harus punya kolom: perusahaan_id, periode, stok_akhir_ton,
                          volume_penjualan_ton, stok_flag
    """
    if df.empty:
        return 0
    rows = df.to_dict("records")
    sql = """
        INSERT INTO fact_operasional
            (perusahaan_id, periode, stok_akhir_ton, volume_penjualan_ton, stok_flag)
        VALUES (%(perusahaan_id)s, %(periode)s, %(stok_akhir_ton)s,
                %(volume_penjualan_ton)s, %(stok_flag)s)
        ON CONFLICT (perusahaan_id, periode) DO UPDATE SET
            stok_akhir_ton       = EXCLUDED.stok_akhir_ton,
            volume_penjualan_ton = EXCLUDED.volume_penjualan_ton,
            stok_flag            = EXCLUDED.stok_flag
    """
    conn = dwh_hook.get_conn()
    cur  = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    return len(rows)


def build_produksi_row(pid: str, periode: str, prod_ton: float, luas_ha: float) -> dict:
    luas = float(luas_ha) if luas_ha is not None else 0.0
    produktivitas = round(prod_ton / luas, 4) if luas > 0 else 0.0
    return {
        "perusahaan_id"   : pid,
        "periode"         : periode,
        "kode_wilayah"    : PERUSAHAAN_WILAYAH_MAP.get(pid),
        "produksi_tbs_ton": round(float(prod_ton), 4) if prod_ton else 0.0,
        "luas_panen_ha"   : round(luas, 4),
        "produktivitas"   : produktivitas,
    }


def build_operasional_row(pid: str, periode: str,
                          stok: float | None, vol_jual: float | None,
                          stok_flag: str | None = None) -> dict:
    return {
        "perusahaan_id"      : pid,
        "periode"            : periode,
        "stok_akhir_ton"     : round(float(stok), 4) if stok is not None else None,
        "volume_penjualan_ton": round(float(vol_jual), 4) if vol_jual is not None else 0.0,
        "stok_flag"          : stok_flag,
    }


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def etl_mysql_a(**_):
    """MySQL Tipe A: laporan_bulanan — satuan TON, deduplikasi v2."""
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    mysql = MySqlHook(mysql_conn_id="mysql_oltp")
    prod_rows, ops_rows = [], []

    for db in MYSQL_A_DATABASES:
        log.info("MySQL A — processing %s", db)
        df = mysql.get_pandas_df(
            f"""
            SELECT perusahaan_id, periode,
                   produksi_tbs_ton, luas_panen_ha,
                   stok_akhir_ton, volume_penjualan_ton
            FROM `{db}`.laporan_bulanan
            WHERE laporan_id NOT LIKE '%-v2'
            """,
            parameters=None
        )
        for _, r in df.iterrows():
            pid     = r["perusahaan_id"]
            periode = str(r["periode"]).strip()
            prod_rows.append(build_produksi_row(pid, periode, r["produksi_tbs_ton"], r["luas_panen_ha"]))
            ops_rows.append(build_operasional_row(pid, periode, r["stok_akhir_ton"], r["volume_penjualan_ton"]))

    n_p = upsert_fact_produksi(dwh, pd.DataFrame(prod_rows))
    n_o = upsert_fact_operasional(dwh, pd.DataFrame(ops_rows))
    log.info("MySQL A — loaded %d produksi, %d operasional", n_p, n_o)


def etl_mysql_b(**_):
    """MySQL Tipe B: rekap_bulanan — KG→TON, forward-fill NULL luas, parse periode."""
    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    mysql = MySqlHook(mysql_conn_id="mysql_oltp")
    prod_rows, ops_rows = [], []

    for db in MYSQL_B_DATABASES:
        log.info("MySQL B — processing %s", db)
        df = mysql.get_pandas_df(
            f"""
            SELECT id_pks as perusahaan_id, bulan as periode,
                   produksi_tbs / 1000  AS produksi_tbs_ton,
                   luas_panen           AS luas_panen_ha,
                   stok_akhir  / 1000   AS stok_akhir_ton,
                   volume_jual / 1000   AS volume_penjualan_ton
            FROM `{db}`.rekap_bulanan
            ORDER BY bulan
            """,
            parameters=None
        )
        # Forward-fill NULL luas_panen (pandas 2.x compatible)
        df["luas_panen_ha"] = df["luas_panen_ha"].ffill()

        for _, r in df.iterrows():
            pid     = r["perusahaan_id"]
            periode = str(r["periode"]).strip()  # sudah YYYY-MM dari DB
            prod_rows.append(build_produksi_row(pid, periode, r["produksi_tbs_ton"], r["luas_panen_ha"]))
            ops_rows.append(build_operasional_row(pid, periode, r["stok_akhir_ton"], r["volume_penjualan_ton"]))

    n_p = upsert_fact_produksi(dwh, pd.DataFrame(prod_rows))
    n_o = upsert_fact_operasional(dwh, pd.DataFrame(ops_rows))
    log.info("MySQL B — loaded %d produksi, %d operasional", n_p, n_o)


def etl_pg_c(**_):
    """PostgreSQL Tipe C: laporan — DATE→YYYY-MM, filter produksi=0."""
    dwh     = PostgresHook(postgres_conn_id="postgis_dwh")
    oltp_pg = PostgresHook(postgres_conn_id="postgres_oltp")
    prod_rows, ops_rows = [], []

    for schema in PG_C_SCHEMAS:
        log.info("PG C — processing %s", schema)
        df = oltp_pg.get_pandas_df(
            f"""
            SET search_path TO "{schema}";
            SELECT kode_perusahaan         AS perusahaan_id,
                   TO_CHAR(tgl_laporan, 'YYYY-MM') AS periode,
                   NULLIF(produksi, 0)     AS produksi_tbs_ton,
                   luas_ha                AS luas_panen_ha,
                   stok_akhir_tahun       AS stok_akhir_ton,
                   penjualan_ton          AS volume_penjualan_ton
            FROM "{schema}".laporan
            WHERE produksi > 0
            """
        )
        for _, r in df.iterrows():
            pid = r["perusahaan_id"]
            prod_rows.append(build_produksi_row(pid, r["periode"], r["produksi_tbs_ton"], r["luas_panen_ha"]))
            ops_rows.append(build_operasional_row(pid, r["periode"], r["stok_akhir_ton"], r["volume_penjualan_ton"]))

    n_p = upsert_fact_produksi(dwh, pd.DataFrame(prod_rows))
    n_o = upsert_fact_operasional(dwh, pd.DataFrame(ops_rows))
    log.info("PG C — loaded %d produksi, %d operasional", n_p, n_o)


def etl_pg_d(**_):
    """PostgreSQL Tipe D: produksi_bulanan — tahun+bulan→YYYY-MM, deduplikasi, NULL stok→flag."""
    dwh     = PostgresHook(postgres_conn_id="postgis_dwh")
    oltp_pg = PostgresHook(postgres_conn_id="postgres_oltp")
    prod_rows, ops_rows = [], []

    for schema in PG_D_SCHEMAS:
        log.info("PG D — processing %s", schema)
        df = oltp_pg.get_pandas_df(
            f"""
            SELECT DISTINCT ON (perusahaan_id, tahun, bulan)
                   perusahaan_id,
                   CONCAT(tahun, '-', LPAD(bulan::text, 2, '0')) AS periode,
                   produksi_tbs_ton,
                   luas_panen_ha,
                   stok_akhir_ton,
                   volume_penjualan AS volume_penjualan_ton
            FROM "{schema}".produksi_bulanan
            ORDER BY perusahaan_id, tahun, bulan, produksi_tbs_ton DESC NULLS LAST
            """
        )
        for _, r in df.iterrows():
            pid       = r["perusahaan_id"]
            periode   = r["periode"]
            stok      = r["stok_akhir_ton"] if pd.notna(r["stok_akhir_ton"]) else None
            stok_flag = "missing" if stok is None else None
            prod_rows.append(build_produksi_row(pid, periode, r["produksi_tbs_ton"], r["luas_panen_ha"]))
            ops_rows.append(build_operasional_row(pid, periode, stok, r["volume_penjualan_ton"], stok_flag))

    n_p = upsert_fact_produksi(dwh, pd.DataFrame(prod_rows))
    n_o = upsert_fact_operasional(dwh, pd.DataFrame(ops_rows))
    log.info("PG D — loaded %d produksi, %d operasional", n_p, n_o)


def load_dim_kebun(**_):
    """
    Load dim_kebun dari semua 12 OLTP database.

    Mapping nama tabel per tipe:
      A (MySQL)  : kebun         — kolom: kebun_id, perusahaan_id, nama_kebun, kode_wilayah, luas_ha, tahun_tanam, varietas_id, status_lahan
      B (MySQL)  : blok_kebun    — kolom: blok_id→kebun_id, id_pks→perusahaan_id, nama_blok→nama_kebun, ..., status→status_lahan
      C (PgSQL)  : lahan         — kolom: lahan_id→kebun_id, kode_perusahaan→perusahaan_id, nama_lahan→nama_kebun, wilayah_kode→kode_wilayah, ...
      D (PgSQL)  : kebun_produksi — kolom: kebun_id, perusahaan_id, nama_kebun, lokasi_kabupaten→kode_wilayah, ..., status→status_lahan
    """
    dwh     = PostgresHook(postgres_conn_id="postgis_dwh")
    mysql   = MySqlHook(mysql_conn_id="mysql_oltp")
    oltp_pg = PostgresHook(postgres_conn_id="postgres_oltp")
    rows    = []

    # ── Tipe A: kebun ────────────────────────────────────────────
    for db in MYSQL_A_DATABASES:
        df = mysql.get_pandas_df(
            f"SELECT kebun_id, perusahaan_id, nama_kebun, kode_wilayah, "
            f"luas_ha, tahun_tanam, varietas_id, status_lahan "
            f"FROM `{db}`.kebun"
        )
        for _, r in df.iterrows():
            rows.append({
                "kebun_id"    : r["kebun_id"],
                "perusahaan_id": r["perusahaan_id"],
                "nama_kebun"  : r["nama_kebun"],
                "kode_wilayah": str(r["kode_wilayah"]),
                "luas_ha"     : float(r["luas_ha"]) if pd.notna(r["luas_ha"]) else None,
                "tahun_tanam" : int(r["tahun_tanam"]) if pd.notna(r["tahun_tanam"]) else None,
                "varietas_id" : r["varietas_id"],
                "status_lahan": r["status_lahan"],
            })
    log.info("dim_kebun — MySQL A: %d kebun", len(rows))

    # ── Tipe B: blok_kebun ────────────────────────────────────────
    count_b = 0
    for db in MYSQL_B_DATABASES:
        df = mysql.get_pandas_df(
            f"SELECT blok_id, id_pks, nama_blok, kode_wilayah, "
            f"luas_ha, tahun_tanam, varietas_id, status "
            f"FROM `{db}`.blok_kebun"
        )
        for _, r in df.iterrows():
            rows.append({
                "kebun_id"    : r["blok_id"],
                "perusahaan_id": r["id_pks"],
                "nama_kebun"  : r["nama_blok"],
                "kode_wilayah": str(r["kode_wilayah"]),
                "luas_ha"     : float(r["luas_ha"]) if pd.notna(r["luas_ha"]) else None,
                "tahun_tanam" : int(r["tahun_tanam"]) if pd.notna(r["tahun_tanam"]) else None,
                "varietas_id" : r["varietas_id"],
                "status_lahan": r["status"],
            })
            count_b += 1
    log.info("dim_kebun — MySQL B: %d kebun", count_b)

    # ── Tipe C: lahan ─────────────────────────────────────────────
    count_c = 0
    for schema in PG_C_SCHEMAS:
        df = oltp_pg.get_pandas_df(
            f"SELECT lahan_id, kode_perusahaan, nama_lahan, wilayah_kode, "
            f"luas_ha, tahun_tanam, varietas_id, status_lahan "
            f"FROM \"{schema}\".lahan"
        )
        for _, r in df.iterrows():
            rows.append({
                "kebun_id"    : r["lahan_id"],
                "perusahaan_id": r["kode_perusahaan"],
                "nama_kebun"  : r["nama_lahan"],
                "kode_wilayah": str(r["wilayah_kode"]),
                "luas_ha"     : float(r["luas_ha"]) if pd.notna(r["luas_ha"]) else None,
                "tahun_tanam" : int(r["tahun_tanam"]) if pd.notna(r["tahun_tanam"]) else None,
                "varietas_id" : r["varietas_id"],
                "status_lahan": r["status_lahan"],
            })
            count_c += 1
    log.info("dim_kebun — PG C: %d kebun", count_c)

    # ── Tipe D: kebun_produksi ────────────────────────────────────
    count_d = 0
    for schema in PG_D_SCHEMAS:
        df = oltp_pg.get_pandas_df(
            f"SELECT id_kebun AS kebun_id, id_perusahaan AS perusahaan_id, nama_kebun, lokasi_kabupaten, "
            f"area_ha AS luas_ha, planted_year AS tahun_tanam, kode_bibit AS varietas_id, status "
            f"FROM \"{schema}\".kebun_produksi"
        )
        for _, r in df.iterrows():
            rows.append({
                "kebun_id"    : r["kebun_id"],
                "perusahaan_id": r["perusahaan_id"],
                "nama_kebun"  : r["nama_kebun"],
                "kode_wilayah": str(r["lokasi_kabupaten"]),
                "luas_ha"     : float(r["luas_ha"]) if pd.notna(r["luas_ha"]) else None,
                "tahun_tanam" : int(r["tahun_tanam"]) if pd.notna(r["tahun_tanam"]) else None,
                "varietas_id" : r["varietas_id"],
                "status_lahan": r["status"],
            })
            count_d += 1
    log.info("dim_kebun — PG D: %d kebun", count_d)

    # ── Upsert ke dim_kebun ───────────────────────────────────────
    if not rows:
        log.warning("Tidak ada data kebun yang ditemukan")
        return

    sql = """
        INSERT INTO dim_kebun
            (kebun_id, perusahaan_id, nama_kebun, kode_wilayah,
             luas_ha, tahun_tanam, varietas_id, status_lahan)
        VALUES (%(kebun_id)s, %(perusahaan_id)s, %(nama_kebun)s, %(kode_wilayah)s,
                %(luas_ha)s, %(tahun_tanam)s, %(varietas_id)s, %(status_lahan)s)
        ON CONFLICT (kebun_id) DO UPDATE SET
            nama_kebun   = EXCLUDED.nama_kebun,
            kode_wilayah = EXCLUDED.kode_wilayah,
            luas_ha      = EXCLUDED.luas_ha,
            tahun_tanam  = EXCLUDED.tahun_tanam,
            varietas_id  = EXCLUDED.varietas_id,
            status_lahan = EXCLUDED.status_lahan
    """
    conn = dwh.get_conn()
    cur  = conn.cursor()
    cur.executemany(sql, rows)
    conn.commit()
    log.info("dim_kebun — total %d kebun di-upsert ke DWH", len(rows))


def etl_excel(**_):
    """
    Baca 12 file Excel PKS dan load ke fact_produksi.
    Excel TIDAK memiliki data operasional (hanya produksi & luas).
    Format berbeda per tipe:
      A: header baris 1, periode Jan-2023, satuan TON
      B: header baris 1, periode 01/01/2023, satuan KG
      C & D: header baris 3, periode YYYY-MM, satuan TON
    """
    import glob
    import os

    dwh = PostgresHook(postgres_conn_id="postgis_dwh")
    prod_rows = []

    excel_files = sorted(glob.glob(os.path.join(EXCEL_DIR, "Laporan_Excel_PKS-*.xlsx")))
    log.info("Found %d Excel files", len(excel_files))

    for fpath in excel_files:
        fname = os.path.basename(fpath)
        log.info("Processing %s", fname)

        # Tentukan tipe dari nama file
        if "-A-" in fname:
            tipe = "A"
        elif "-B-" in fname:
            tipe = "B"
        else:
            tipe = "CD"  # C dan D sama formatnya

        try:
            df = pd.read_excel(fpath, engine="openpyxl")

            for _, r in df.iterrows():
                if tipe == "A":
                    # Excel A: Periode, Produksi_TBS, Penjualan_CPO, Stok_Gudang
                    pid     = fname.replace("Laporan_Excel_", "").replace(".xlsx", "")
                    periode = parse_periode_a(r.get("Periode", ""))
                    prod    = float(r.get("Produksi_TBS", 0) or 0)
                    luas    = 0.0  # Tidak ada di Excel baru, biarkan DB yang mengisi
                elif tipe == "B":
                    # Excel B: TANGGAL, HASIL_PANEN_KG, VOL_JUAL_KG, SISA_STOK
                    pid     = fname.replace("Laporan_Excel_", "").replace(".xlsx", "")
                    periode = parse_periode_b(r.get("TANGGAL", ""))
                    prod    = float(r.get("HASIL_PANEN_KG", 0) or 0) / 1000  # KG → TON
                    luas    = 0.0
                else:  # C & D
                    # Excel C/D: perusahaan, periode_y_m, tbs_ton, cpo_jual
                    pid     = EXCEL_PERUSAHAAN_MAP.get(str(r.get("perusahaan", "")).strip())
                    periode = str(r.get("periode_y_m", "")).strip()
                    prod    = float(r.get("tbs_ton", 0) or 0)
                    luas    = 0.0

                if not pid:
                    log.warning("Perusahaan tidak dikenali dari file %s", fname)
                    continue
                if not periode or prod <= 0:
                    continue

                prod_rows.append(build_produksi_row(pid, periode, prod, luas))

        except Exception as e:
            log.error("Error membaca %s: %s", fname, e)
            raise

    n_p = upsert_fact_produksi(dwh, pd.DataFrame(prod_rows))
    log.info("Excel — loaded %d produksi rows", n_p)


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner"           : "airflow",
    "retries"         : 1,
    "retry_delay"     : timedelta(seconds=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id      = "dag2_produksi_etl",
    description = "ETL produksi & operasional dari 12 OLTP database + 12 Excel PKS ke DWH",
    start_date  = datetime(2023, 1, 1),
    schedule    = "@monthly",
    catchup     = False,
    default_args= default_args,
    tags        = ["etl", "produksi", "oltp"],
) as dag:

    with TaskGroup("extract_and_transform_db", tooltip="Extract and transform data from 12 OLTP Databases") as ekstraksi_db_group:
        t_mysql_a = PythonOperator(
            task_id         = "extract_and_transform_mysql_cluster_A",
            python_callable = etl_mysql_a,
        )

        t_mysql_b = PythonOperator(
            task_id         = "extract_and_transform_mysql_cluster_B",
            python_callable = etl_mysql_b,
        )

        t_pg_c = PythonOperator(
            task_id         = "extract_and_transform_postgres_cluster_C",
            python_callable = etl_pg_c,
        )

        t_pg_d = PythonOperator(
            task_id         = "extract_and_transform_postgres_cluster_D",
            python_callable = etl_pg_d,
        )

    t_excel = PythonOperator(
        task_id         = "extract_and_transform_excel",
        python_callable = etl_excel,
    )

    t_load_dwh = PythonOperator(
        task_id         = "load_dwh",
        python_callable = load_dim_kebun,
    )

    # 1. Parallel DB extraction
    # 2. Merge Excel data (overwrites/completes DB data)
    # 3. Load kebun dimensions
    ekstraksi_db_group >> t_excel >> t_load_dwh
