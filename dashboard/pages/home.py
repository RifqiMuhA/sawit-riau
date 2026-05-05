"""
pages/home.py — Landing Page (Minimalis)
1 section: Hero banner + 4 KPI Cards + Tagline & Mascot
"""

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from db import run_query

dash.register_page(__name__, path="/", name="Beranda", order=0)

# ── Helper KPI ────────────────────────────────────────────────
def _load_kpis() -> dict:
    try:
        kpi = {}

        # Total produksi TBS bulan terakhir
        df = run_query("""
            SELECT COALESCE(SUM(produksi_tbs_ton), 0) AS total
            FROM fact_produksi
            WHERE periode = (SELECT MAX(periode) FROM fact_produksi)
        """)
        kpi["produksi"] = f"{df['total'].iloc[0]:,.0f} ton"

        # Jumlah perusahaan
        df = run_query("SELECT COUNT(*) AS n FROM dim_perusahaan")
        kpi["perusahaan"] = str(df["n"].iloc[0])

        # % kebun produktif
        df = run_query("""
            SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE status_lahan='produktif')
                         / NULLIF(COUNT(*), 0), 1) AS pct
            FROM dim_kebun
        """)
        kpi["produktif"] = f"{df['pct'].iloc[0]}%"

        # Deteksi penimbunan (Total Kasus)
        df = run_query("""
            SELECT COUNT(*) AS n
            FROM fact_operasional
            WHERE indikasi_timbun = TRUE
        """)
        kpi["timbun"] = str(df["n"].iloc[0])

    except Exception:
        kpi = {
            "produksi": "–", "perusahaan": "–",
            "produktif": "–", "timbun": "–"
        }
    return kpi


# ── Layout ────────────────────────────────────────────────────
def layout():
    kpi = _load_kpis()

    return html.Div([
        # Top Row: Hero Card (Left) + KPI Cards (Right)
        dbc.Row([
            # Hero Card
            dbc.Col([
                html.Div([
                    html.Div(className="hero-bg-overlay"),
                    html.Div([
                        html.Div("Pemantauan Kebun Sawit Cerdas, Terintegrasi.", className="hero-title"),
                        html.Div("Pantau produktivitas, SDM, hingga kondisi lahan sawit di seluruh Provinsi Riau melalui dashboard ini.", className="hero-desc"),
                        dcc.Link(html.Button("Lihat Reporting →", className="hero-btn"), href="/reporting/sawit")
                    ], className="hero-content"),
                ], className="hero-card")
            ], lg=4, md=12, className="mb-4"),

            # 4 KPI Cards
            dbc.Col([
                dbc.Row([
                    dbc.Col(_mini_kpi("/assets/mascot_2.webp", "Total Produksi (Bulan Ini)", kpi["produksi"]), lg=3, md=6, sm=6, xs=12, className="mb-3"),
                    dbc.Col(_mini_kpi("/assets/mascot_3.webp", "Perusahaan Terdaftar", kpi["perusahaan"]), lg=3, md=6, sm=6, xs=12, className="mb-3"),
                    dbc.Col(_mini_kpi("/assets/mascot_4.webp", "Kebun Produktif", kpi["produktif"]), lg=3, md=6, sm=6, xs=12, className="mb-3"),
                    dbc.Col(_mini_kpi("/assets/mascot_5.webp", "Indikasi Penimbunan", kpi["timbun"]), lg=3, md=6, sm=6, xs=12, className="mb-3"),
                ], className="g-3 h-100")
            ], lg=8, md=12)
        ], className="g-3 mb-5 align-items-stretch"),

        # Bottom Row: Tagline + Mascot
        html.Div([
            html.Div(className="tagline-bg-overlay"),
            html.Div([
                html.H1("Halo, Saya Savi (Sawit Vision)!"),
                html.P("Platform analitik terintegrasi untuk mendukung pengambilan keputusan strategis Dinas Perkebunan Provinsi Riau. Mendorong industri kelapa sawit yang lebih transparan dan efisien.")
            ], className="tagline-text"),
            html.Img(src="/assets/mascot_1.webp", className="tagline-mascot")
        ], className="tagline-section")
    ])

def _mini_kpi(img_src: str, label: str, value: str) -> html.Div:
    return html.Div([
        html.Img(src=img_src, className="mini-kpi-mascot"),
        html.Div(label, className="mini-kpi-label"),
        html.Div(value, className="mini-kpi-value"),
    ], className="mini-kpi-card")
