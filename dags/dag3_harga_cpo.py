"""
DAG 3 — Harga CPO Disbun: Download PDF + Ekstrak + Update → dim_periode.harga_cpo (DWH)
======================================================================================
Adaptasi dari etl_cpo_data.py (referensi teman, sudah teruji).

Sumber  : PDF laporan tahunan Dinas Perkebunan Provinsi Riau
          https://disbun.riau.go.id/rekap_harga_tbs
Target  : dim_periode.harga_cpo di sawit_dwh (PostGIS)
          harga_cpo adalah atribut periodik — disimpan langsung di dim_periode,
          bukan fact table terpisah (fact_harga_cpo sudah dihapus dari schema).
Jadwal  : 1 Januari tiap tahun (proses PDF tahun sebelumnya)
          Trigger manual: conf {"tahun": 2023}

Requirement: Container Airflow HARUS pakai custom image (Dockerfile)
             yang sudah install chromium + chromedriver via apt-get.
             Jalankan: docker-compose build && docker-compose up -d
"""

from __future__ import annotations

import glob
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pdfplumber
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────────────────────
BASE_URL   = "https://disbun.riau.go.id/rekap_harga_tbs"
PDF_DIR    = Path("/opt/airflow/data-pdf")
YEAR_START = 2023
HEADLESS   = True
TIMEOUT    = 15
DL_TIMEOUT = 60
DELAY      = 1.5

HARGA_MIN = 5_000
HARGA_MAX = 25_000

NAMA_BULAN = {
    "JANUARI": 1, "FEBRUARI": 2, "MARET": 3, "APRIL": 4,
    "MEI": 5, "JUNI": 6, "JULI": 7, "AGUSTUS": 8,
    "SEPTEMBER": 9, "OKTOBER": 10, "NOVEMBER": 11, "DESEMBER": 12,
}
BULAN_PARTIAL = {
    "EPTEMBE": 9, "EPTEMBER": 9, "SEPTMBER": 9,
    "KTOBER": 10, "OVEMBER": 11, "ESEMBER": 12,
}


# ─────────────────────────────────────────────────────────────
# BAGIAN 1 — DOWNLOADER (Selenium + Chromium)
# ─────────────────────────────────────────────────────────────

SELENIUM_HUB = "http://selenium_chrome:4444/wd/hub"  # container name dari docker-compose


def _build_driver(download_dir: Path) -> webdriver.Remote:
    """
    Buat Selenium Remote WebDriver yang terhubung ke selenium/standalone-chrome container.
    File download akan di-handle via base64 setelah navigasi ke halaman PDF.
    """
    download_dir.mkdir(parents=True, exist_ok=True)
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    # Aktifkan download otomatis di remote (via prefs)
    prefs = {
        "download.default_directory" : "/home/seluser/downloads",
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True,
    }
    opts.add_experimental_option("prefs", prefs)
    driver = webdriver.Remote(
        command_executor=SELENIUM_HUB,
        options=opts,
    )
    driver.implicitly_wait(TIMEOUT)
    return driver





def _wait_download(download_dir: Path, timeout: int = DL_TIMEOUT) -> Path | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        tmp = glob.glob(str(download_dir / "*.crdownload")) + \
              glob.glob(str(download_dir / "*.tmp"))
        pdf = glob.glob(str(download_dir / "*.pdf"))
        if not tmp and pdf:
            return Path(max(pdf, key=os.path.getmtime))
    return None


def _rename_pdf(download_dir: Path, target_name: str) -> bool:
    pdf_files = glob.glob(str(download_dir / "*.pdf"))
    if not pdf_files:
        return False
    latest = Path(max(pdf_files, key=os.path.getmtime))
    target = download_dir / target_name
    shutil.move(str(latest), str(target))
    log.info("Renamed: %s → %s", latest.name, target_name)
    return True


def download_pdfs(years: list[int], pdf_dir: Path) -> list[int]:
    downloaded: list[int] = []
    driver = _build_driver(pdf_dir)
    wait   = WebDriverWait(driver, TIMEOUT)

    try:
        log.info("Membuka %s", BASE_URL)
        driver.get(BASE_URL)
        time.sleep(DELAY * 2)

        for year in years:
            target_pdf = pdf_dir / f"{year}.pdf"
            if target_pdf.exists():
                log.info("%d.pdf sudah ada, skip download.", year)
                downloaded.append(year)
                continue

            log.info("Download tahun %d ...", year)
            try:
                Select(driver.find_element(By.ID, "jenis_filter")).select_by_value("1")
                time.sleep(0.5)
                Select(driver.find_element(By.ID, "tahun")).select_by_value(str(year))
                time.sleep(DELAY)

                driver.find_element(By.CSS_SELECTOR, "button.btn.btn-primary").click()
                time.sleep(DELAY * 2)

                dl_link = wait.until(EC.element_to_be_clickable((
                    By.XPATH,
                    "//a[contains(.,'Download PDF') or contains(.,'PDF') "
                    "or contains(.,'download')]"
                )))
                dl_link.click()
                time.sleep(DELAY)

                result = _wait_download(pdf_dir)
                if not result:
                    log.error("Timeout download tahun %d", year)
                    continue
                if _rename_pdf(pdf_dir, f"{year}.pdf"):
                    log.info("✓ Sukses: %d.pdf", year)
                    downloaded.append(year)

            except Exception as exc:
                log.warning("Gagal download tahun %d: %s", year, exc)

    except Exception as exc:
        log.exception("Error downloader: %s", exc)
    finally:
        driver.quit()

    return downloaded


# ─────────────────────────────────────────────────────────────
# BAGIAN 2 — EXTRACTOR (pdfplumber)
# ─────────────────────────────────────────────────────────────

def _clean_numeric(text) -> float | None:
    if not text:
        return None
    s = re.sub(r"\s+", "", str(text).strip())
    if re.search(r"\d+\.\d{3},\d{2}", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _detect_schema(full_text: str) -> str:
    return "multi_mitra" if "MITRA PLASMA" in full_text.upper() else "single_layer"


def _extract_year_from_text(text: str, pdf_path: Path) -> int:
    m = re.search(r"TAHUN\s+(\d{4})", text, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d{4})", pdf_path.stem)
    return int(m.group(1)) if m else 0


def _update_bulan(col2: str, current: int | None) -> int | None:
    upper = col2.upper()
    for nama, nomor in NAMA_BULAN.items():
        if nama in upper:
            return nomor
    for partial, nomor in BULAN_PARTIAL.items():
        if partial in upper:
            return nomor
    return current


def _extract_records(tables: list, tahun: int) -> list[dict]:
    records: list[dict] = []
    current_bulan: int | None = None
    state = 0

    def simpan(cpo: float | None) -> None:
        if current_bulan and cpo and HARGA_MIN < cpo < HARGA_MAX:
            if not any(r["bulan"] == current_bulan for r in records):
                records.append({"tahun": tahun, "bulan": current_bulan, "harga_cpo": cpo})
                log.info("Bulan %02d: Rp %,.2f", current_bulan, cpo)

    for table in tables:
        for row in table:
            if len(row) < 6:
                continue
            col2    = str(row[2]).strip() if row[2] else ""
            col3    = str(row[3]).strip() if row[3] else ""
            col5    = str(row[5]).strip() if row[5] else ""
            col2_up = col2.upper()

            current_bulan = _update_bulan(col2_up, current_bulan)

            if "RATA-RATA PLASMA/THN" in col2_up or "RATA-RATA SWADAYA/THN" in col2_up:
                state = 0
            elif col5 == "":
                pass
            elif "JUMLAH PLASMA" in col2_up and "RATA-RATA PLASMA" in col2_up:
                state = "B1"
            elif "JUMLAH" in col2_up and "RATA-RATA" in col2_up and "PLASMA" not in col2_up:
                state = "A"
            elif "RATA-RATA PLASMA" in col2_up and col3 == "":
                simpan(_clean_numeric(col5))
                state = 0
            elif state == "A":
                simpan(_clean_numeric(col5))
                state = 0
            elif state == "B1":
                state = "B2"
            elif state == "B2":
                simpan(_clean_numeric(col5))
                state = 0

    return records


def extract_cpo(pdf_path: Path) -> pd.DataFrame:
    log.info("Membaca: %s", pdf_path.name)
    with pdfplumber.open(pdf_path) as pdf:
        full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        tahun     = _extract_year_from_text(full_text, pdf_path)
        schema    = _detect_schema(full_text)
        log.info("Tahun: %s | Schema: %s", tahun, schema)

        all_tables: list = []
        for page in pdf.pages:
            tbls = page.extract_tables()
            if tbls:
                all_tables.extend(tbls)

    if schema != "multi_mitra":
        log.warning("Schema single_layer belum di-support: %s", pdf_path.name)
        return pd.DataFrame(columns=["tahun", "bulan", "harga_cpo"])

    records = _extract_records(all_tables, tahun)
    df = pd.DataFrame(records).sort_values("bulan").reset_index(drop=True)
    log.info("Total: %d bulan berhasil diekstrak", len(df))
    return df


# ─────────────────────────────────────────────────────────────
# BAGIAN 3 — LOAD KE DWH
# ─────────────────────────────────────────────────────────────

def load_ke_dwh(df: pd.DataFrame, tahun: int) -> dict:
    if df.empty:
        log.warning("DataFrame kosong, skip load ke DWH.")
        return {"tahun": tahun, "loaded": 0}

    dwh  = PostgresHook(postgres_conn_id="postgis_dwh")
    conn = dwh.get_conn()
    cur  = conn.cursor()
    loaded = 0

    for _, rec in df.iterrows():
        periode = f"{int(rec['tahun'])}-{int(rec['bulan']):02d}"
        harga   = rec["harga_cpo"]

        # Pastikan periode sudah ada di dim_periode (seed sudah di-generate sampai 2030)
        cur.execute("""
            INSERT INTO dim_periode (periode, tahun, bulan, kuartal)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
        """, (periode, int(rec["tahun"]), int(rec["bulan"]),
              (int(rec["bulan"]) - 1) // 3 + 1))

        # Update kolom harga_cpo langsung di dim_periode
        cur.execute("""
            UPDATE dim_periode SET harga_cpo = %s WHERE periode = %s
        """, (harga, periode))

        log.info("Update dim_periode %s → Rp %,.2f", periode, harga)
        loaded += 1

    conn.commit()
    cur.close()
    conn.close()
    log.info("✅ %d periode di-update harga_cpo di dim_periode (tahun %d)", loaded, tahun)
    return {"tahun": tahun, "loaded": loaded}


# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS (Airflow)
# ─────────────────────────────────────────────────────────────

def _get_tahun(context: dict) -> int:
    conf = context.get("dag_run").conf or {}
    if "tahun" in conf:
        return int(conf["tahun"])
    return context["logical_date"].year - 1


def task_download_pdf(**context) -> None:
    tahun    = _get_tahun(context)
    pdf_path = PDF_DIR / f"{tahun}.pdf"

    if pdf_path.exists():
        log.info("PDF sudah ada: %s — skip download.", pdf_path)
        context["ti"].xcom_push(key="tahun", value=tahun)
        return

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = download_pdfs([tahun], PDF_DIR)
    if tahun not in downloaded:
        raise RuntimeError(
            f"Gagal download PDF tahun {tahun}. "
            f"Letakkan {tahun}.pdf secara manual di folder data-pdf/ sebagai fallback."
        )
    context["ti"].xcom_push(key="tahun", value=tahun)


def task_ekstrak_cpo(**context) -> None:
    ti    = context["ti"]
    tahun = (ti.xcom_pull(task_ids="extract_group.download_pdf", key="tahun")
             or ti.xcom_pull(task_ids="download_pdf", key="tahun")
             or _get_tahun(context))

    pdf_path = PDF_DIR / f"{tahun}.pdf"
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF tidak ada: {pdf_path}")

    df = extract_cpo(pdf_path)
    if df.empty:
        raise ValueError(f"Tidak ada data CPO dari {pdf_path.name}.")

    log.info("Ekstrak %d bulan untuk tahun %s", len(df), tahun)
    ti.xcom_push(key="cpo_records", value=df.to_dict(orient="records"))
    ti.xcom_push(key="tahun", value=tahun)


def task_load_ke_dwh(**context) -> None:
    ti      = context["ti"]
    records = (ti.xcom_pull(task_ids="transform_group.ekstrak_cpo_pdf", key="cpo_records")
               or ti.xcom_pull(task_ids="ekstrak_cpo_pdf", key="cpo_records"))
    tahun   = (ti.xcom_pull(task_ids="transform_group.ekstrak_cpo_pdf", key="tahun")
               or ti.xcom_pull(task_ids="ekstrak_cpo_pdf", key="tahun"))

    if not records:
        raise ValueError("Tidak ada data CPO dari XCom.")

    result = load_ke_dwh(pd.DataFrame(records), tahun)
    context["ti"].xcom_push(key="ringkasan", value=result)


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner"           : "airflow",
    "depends_on_past" : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(seconds=5),
    "email_on_failure": False,
}

with DAG(
    dag_id      = "dag3_harga_cpo",
    description = "ETL harga CPO Plasma dari PDF Disbun Riau → dim_periode.harga_cpo (DWH).",
    schedule    = "0 0 1 1 *",   # 1 Januari tiap tahun, proses PDF tahun lalu
    start_date  = datetime(2023, 1, 1),
    catchup     = True,
    default_args= default_args,
    tags        = ["etl", "harga-cpo", "disbun", "selenium"],
) as dag:

    with TaskGroup("extract_group", tooltip="Download PDF dari Disbun Riau") as eg:
        t_download = PythonOperator(
            task_id         = "download_pdf",
            python_callable = task_download_pdf,
        )

    with TaskGroup("transform_group", tooltip="Ekstrak tabel harga CPO dari PDF") as tg:
        t_ekstrak = PythonOperator(
            task_id         = "ekstrak_cpo_pdf",
            python_callable = task_ekstrak_cpo,
        )

    with TaskGroup("load_group", tooltip="Update harga_cpo di dim_periode") as lg:
        t_load = PythonOperator(
            task_id         = "load_ke_dwh",
            python_callable = task_load_ke_dwh,
        )

    eg >> Label("PDF siap") >> tg >> Label("Data siap") >> lg