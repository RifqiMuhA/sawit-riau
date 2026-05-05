"""
app.py — Entry Point Utama Sawit Riau Dashboard
Minimalist Redesign with FontAwesome icons.
"""

import dash
from dash import Dash, html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

# ── Inisialisasi App ──────────────────────────────────────────
app = Dash(
    __name__,
    use_pages=True,
    pages_folder="pages",
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        dbc.icons.FONT_AWESOME,
    ],
    suppress_callback_exceptions=True,
    title="Sawit Riau Monitor",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server  # untuk Gunicorn

# ── Top Navbar ────────────────────────────────────────────────
top_navbar = html.Div(className="top-navbar")

# ── Sidebar ───────────────────────────────────────────────────
def _nav(label, href, icon_class):
    return html.A(
        [html.I(className=icon_class), label],
        href=href,
        id=f"nav-{href.strip('/').replace('/', '-') or 'home'}",
    )

sidebar = html.Div([
    # Branding
    html.Div([
        html.Div(html.I(className="fa-solid fa-leaf"), className="sidebar-logo-icon"),
        html.H4("Sawit Riau"),
    ], className="sidebar-logo"),

    # Beranda
    _nav("Home", "/", "fa-solid fa-house"),

    # Reporting
    html.Div("REPORTING", className="sidebar-section-label"),
    _nav("Gambaran Sawit",    "/reporting/sawit",  "fa-solid fa-tree"),
    _nav("SDM & Karyawan",    "/reporting/sdm",    "fa-solid fa-users"),
    _nav("Alert Operasional", "/reporting/alert",  "fa-solid fa-bell"),

    # Analytics
    html.Div("ANALYTICS", className="sidebar-section-label"),
    _nav("Kondisi Kebun",     "/analytics/kondisi-kebun",  "fa-solid fa-map-location-dot"),
    _nav("Produktivitas",     "/analytics/produktivitas",  "fa-solid fa-chart-pie"),
    _nav("Deteksi Penimbunan",       "/analytics/penimbunan",     "fa-solid fa-triangle-exclamation"),
    _nav("Realisasi Panen",   "/analytics/panen",  "fa-solid fa-seedling"),

], className="sidebar", id="sidebar")

# ── Layout ────────────────────────────────────────────────────
app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    sidebar,
    html.Div([
        top_navbar,
        html.Div(
            dash.page_container,
            className="page-container"
        )
    ], className="main-content", id="page-content")
], style={"display": "flex", "minHeight": "100vh"})

# ── Active Nav Highlight ──────────────────────────────────────
NAV_ROUTES = [
    ("nav-home",                   "/"),
    ("nav-reporting-sawit",        "/reporting/sawit"),
    ("nav-reporting-sdm",          "/reporting/sdm"),
    ("nav-reporting-alert",        "/reporting/alert"),
    ("nav-analytics-kondisi-kebun","/analytics/kondisi-kebun"),
    ("nav-analytics-produktivitas","/analytics/produktivitas"),
    ("nav-analytics-penimbunan",   "/analytics/penimbunan"),
    ("nav-analytics-panen",        "/analytics/panen"),
]

@app.callback(
    [Output(nid, "className") for nid, _ in NAV_ROUTES],
    Input("url", "pathname"),
)
def highlight_nav(pathname):
    return ["active" if pathname == href else "" for _, href in NAV_ROUTES]


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
