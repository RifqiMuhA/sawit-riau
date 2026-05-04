"""
pages/home.py — Landing Page (Minimalis)
1 section: Hero banner + 4 KPI Cards + tombol navigasi
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

        # Deteksi penimbunan bulan terakhir
        df = run_query("""
            SELECT COUNT(*) AS n
            FROM fact_operasional
            WHERE indikasi_timbun = TRUE
              AND periode = (SELECT MAX(periode) FROM fact_operasional)
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

    kpi_cards = dbc.Row([
        dbc.Col(_kpi_card(kpi["produksi"],   "Total Produksi (Bulan Terakhir)", "🌾", ""), lg=3, md=6, sm=12, className="mb-3"),
        dbc.Col(_kpi_card(kpi["perusahaan"], "Perusahaan PKS Terdaftar",        "🏭", "info"), lg=3, md=6, sm=12, className="mb-3"),
        dbc.Col(_kpi_card(kpi["produktif"],  "Kebun Berstatus Produktif",       "✅", ""), lg=3, md=6, sm=12, className="mb-3"),
        dbc.Col(_kpi_card(kpi["timbun"],     "Indikasi Penimbunan (Bln. Ini)",  "⚠️", "danger"), lg=3, md=6, sm=12, className="mb-3"),
    ], className="g-3")

    return html.Div([
        # Hero Banner
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.H1("Sistem Monitoring\nPerkebunan Sawit Riau", className="landing-hero-title"),
                    html.P(
                        "Platform analitik terintegrasi untuk memantau produktivitas, "
                        "kondisi lahan, dan potensi anomali distribusi CPO "
                        "di 12 Kabupaten/Kota Provinsi Riau.",
                    ),
                    html.Div([
                        dcc.Link("📊 Lihat Reporting →", href="/reporting/sawit",
                                 className="btn-primary-green"),
                        dcc.Link("🔬 Lihat Analytics →", href="/analytics/kondisi-kebun",
                                 className="btn-primary-green",
                                 style={"background": "rgba(255,255,255,0.18)",
                                        "color": "#fff",
                                        "border": "1px solid rgba(255,255,255,0.4)"}),
                    ]),
                ], lg=8),
                dbc.Col([
                    html.Div("🌴", style={"fontSize": "120px", "textAlign": "center",
                                          "opacity": "0.35", "marginTop": "10px"}),
                ], lg=4, className="d-none d-lg-block"),
            ]),
        ], className="landing-hero"),

        # KPI Cards
        html.Div([
            html.P("Ringkasan Data Terkini", style={
                "fontSize": "12px", "fontWeight": "600",
                "color": "var(--text-muted)", "textTransform": "uppercase",
                "letterSpacing": "1px", "marginBottom": "14px"
            }),
            kpi_cards,
        ]),
    ])


def _kpi_card(value: str, label: str, icon: str, variant: str) -> html.Div:
    return html.Div([
        html.Div(icon, style={"fontSize": "24px", "marginBottom": "8px"}),
        html.Div(value, className="kpi-value"),
        html.Div(label, className="kpi-label"),
    ], className=f"kpi-card {variant}")
