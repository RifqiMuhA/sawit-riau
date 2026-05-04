"""
app.py — Entry Point Utama Sawit Riau Dashboard
Routing multi-page + sidebar navigasi permanen.
"""

import dash
from dash import Dash, html, dcc, Input, Output
import dash_bootstrap_components as dbc

# ── Inisialisasi App ──────────────────────────────────────────
app = Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
    ],
    suppress_callback_exceptions=True,
)
server = app.server  # untuk Gunicorn

# ── Sidebar Navigasi ──────────────────────────────────────────
def make_nav_link(label: str, href: str, icon: str) -> html.A:
    return html.A(
        [html.Span(icon, style={"fontSize": "15px"}), label],
        href=href,
        id=f"nav-{href.strip('/').replace('/', '-') or 'home'}",
    )

sidebar = html.Div([
    # Logo
    html.Div([
        html.H4("🌿 Sawit Riau"),
        html.Small("Sistem Monitoring Perkebunan"),
    ], className="sidebar-logo"),

    # Beranda
    html.Div("Beranda", className="sidebar-section-label"),
    make_nav_link("Dashboard", "/", "🏠"),

    # Reporting
    html.Div("Reporting", className="sidebar-section-label"),
    make_nav_link("Gambaran Sawit",   "/reporting/sawit",   "🌴"),
    make_nav_link("SDM & Karyawan",   "/reporting/sdm",     "👥"),
    make_nav_link("Realisasi Panen",  "/reporting/panen",   "🌾"),
    make_nav_link("Alert Operasional","/reporting/alert",   "🔔"),

    # Analytics
    html.Div("Analytics", className="sidebar-section-label"),
    make_nav_link("Kondisi Kebun (NDVI)", "/analytics/kondisi-kebun",  "🗺️"),
    make_nav_link("Produktivitas & Cluster", "/analytics/produktivitas", "📈"),
    make_nav_link("Deteksi Penimbunan",  "/analytics/penimbunan",      "⚠️"),

], className="sidebar")

# ── Layout Utama ──────────────────────────────────────────────
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    sidebar,
    html.Div(
        dash.page_container,
        className="main-content",
        id="page-content"
    ),
], style={"display": "flex"})


# ── Callback: Tandai Link Aktif ───────────────────────────────
NAV_IDS = [
    ("nav-home",                          "/"),
    ("nav-reporting-sawit",               "/reporting/sawit"),
    ("nav-reporting-sdm",                 "/reporting/sdm"),
    ("nav-reporting-panen",               "/reporting/panen"),
    ("nav-reporting-alert",               "/reporting/alert"),
    ("nav-analytics-kondisi-kebun",       "/analytics/kondisi-kebun"),
    ("nav-analytics-produktivitas",       "/analytics/produktivitas"),
    ("nav-analytics-penimbunan",          "/analytics/penimbunan"),
]

@app.callback(
    [Output(nav_id, "className") for nav_id, _ in NAV_IDS],
    Input("url", "pathname"),
)
def set_active_nav(pathname):
    classes = []
    for _, href in NAV_IDS:
        if pathname == href:
            classes.append("active")
        else:
            classes.append("")
    return classes


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
