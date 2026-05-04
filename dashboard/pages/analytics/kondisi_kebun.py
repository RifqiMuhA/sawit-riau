"""
pages/analytics/kondisi_kebun.py — Kondisi Kebun (NDVI)
Multi-chart: Choropleth Map | Bar NDVI per Kab | Line Tren NDVI
Sumber data: datamart.dm_kondisi_kebun + ST_AsGeoJSON dari dim_kabupaten
"""

import dash
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db import run_query, get_riau_geojson, get_kabupaten_options

dash.register_page(__name__, path="/analytics/kondisi-kebun", name="Kondisi Kebun (NDVI)", order=5)

STATUS_COLOR_MAP = {"kritis": "#D62839", "menurun": "#F4A261", "normal": "#52B788"}
STATUS_SCORE     = {"kritis": 1, "menurun": 2, "normal": 3}   # untuk agregasi dominan


# ── Data Loaders ──────────────────────────────────────────────
def _ndvi_data(periode=None):
    cond = f"WHERE periode = '{periode}'" if periode else ""
    return run_query(f"""
        SELECT kode_wilayah, periode, ndvi_mean, status_kebun
        FROM datamart.dm_kondisi_kebun
        {cond}
        ORDER BY kode_wilayah, periode
    """)

def _ndvi_agregat(periode=None):
    """Rata-rata NDVI dan status dominan per kabupaten untuk 1 periode (peta)."""
    cond = f"WHERE periode = '{periode}'" if periode else \
           "WHERE periode = (SELECT MAX(periode) FROM datamart.dm_kondisi_kebun)"
    return run_query(f"""
        SELECT kode_wilayah,
               ROUND(AVG(ndvi_mean)::numeric, 4) AS ndvi_mean,
               MODE() WITHIN GROUP (ORDER BY status_kebun) AS status_dominan
        FROM datamart.dm_kondisi_kebun
        {cond}
        GROUP BY kode_wilayah
    """)

def _ndvi_bar():
    return run_query("""
        SELECT k.nama_kabupaten, ROUND(AVG(d.ndvi_mean)::numeric, 4) AS ndvi_mean,
               MODE() WITHIN GROUP (ORDER BY d.status_kebun) AS status
        FROM datamart.dm_kondisi_kebun d
        JOIN dim_kabupaten k ON d.kode_wilayah = k.kode_wilayah
        GROUP BY k.nama_kabupaten ORDER BY ndvi_mean ASC
    """)

def _ndvi_tren(kode_wilayah=None):
    cond = f"AND d.kode_wilayah = '{kode_wilayah}'" if kode_wilayah else ""
    return run_query(f"""
        SELECT k.nama_kabupaten, d.periode,
               ROUND(AVG(d.ndvi_mean)::numeric, 4) AS ndvi_mean
        FROM datamart.dm_kondisi_kebun d
        JOIN dim_kabupaten k ON d.kode_wilayah = k.kode_wilayah
        WHERE 1=1 {cond}
        GROUP BY k.nama_kabupaten, d.periode ORDER BY d.periode
    """)

def _periode_list():
    df = run_query("SELECT DISTINCT periode FROM datamart.dm_kondisi_kebun ORDER BY periode")
    return df["periode"].tolist()


# ── Layout ────────────────────────────────────────────────────
def layout():
    periodes = _periode_list()
    periode_marks = {i: p for i, p in enumerate(periodes)} if periodes else {}

    return html.Div([
        html.Div([
            html.H2("Kondisi Kebun — Analitik NDVI"),
            html.P("Peta spasial kesehatan lahan sawit berdasarkan citra satelit Sentinel-2 (GEE). "
                   "Status: Normal (NDVI ≥ P66) | Menurun (P33–P66) | Kritis (< P33)"),
        ], className="page-header"),

        # Filter
        html.Div([
            html.Label("Filter Kabupaten:"),
            dcc.Dropdown(
                id="ndvi-filter-kab",
                options=get_kabupaten_options(),
                value="ALL", clearable=False,
                style={"minWidth": "240px"},
            ),
            html.Label("Periode:"),
            dcc.Dropdown(
                id="ndvi-filter-periode",
                options=[{"label": p, "value": p} for p in periodes],
                value=periodes[-1] if periodes else None,
                clearable=False,
                style={"minWidth": "140px"},
            ),
        ], className="filter-bar"),

        # KPI Status
        html.Div(id="ndvi-kpi-row", className="mb-3"),

        # Section 1: Peta Choropleth (lebar penuh)
        html.P("Peta Spasial Kondisi Kebun Riau", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Choropleth NDVI per Kabupaten — Klik wilayah untuk drill-down", className="chart-card-title"),
            dcc.Graph(id="ndvi-map", config={"displayModeBar": True, "scrollZoom": True}, style={"height": "420px"}),
        ], className="chart-card mb-2"),

        # Section 2: Bar + Line
        html.P("Detail NDVI per Wilayah", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Rata-rata NDVI per Kabupaten (Semua Periode)", className="chart-card-title"),
                dcc.Graph(id="ndvi-chart-bar", config={"displayModeBar": False}),
            ], className="chart-card"), lg=5),
            dbc.Col(html.Div([
                html.Div("Tren NDVI Bulanan per Wilayah", className="chart-card-title"),
                dcc.Graph(id="ndvi-chart-tren", config={"displayModeBar": False}),
            ], className="chart-card"), lg=7),
        ], className="g-3"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("ndvi-kpi-row", "children"),
          Input("ndvi-filter-periode", "value"))
def update_kpi(periode):
    if not periode:
        return html.Div()
    df = run_query(f"""
        SELECT status_kebun, COUNT(*) AS n
        FROM datamart.dm_kondisi_kebun
        WHERE periode = '{periode}'
        GROUP BY status_kebun
    """)
    counts = df.set_index("status_kebun")["n"].to_dict() if not df.empty else {}
    return dbc.Row([
        dbc.Col(_kpi(str(counts.get("normal",  0)), "Wilayah Normal",  "🟢", ""),       lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(str(counts.get("menurun", 0)), "Wilayah Menurun", "🟡", "warning"), lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(str(counts.get("kritis",  0)), "Wilayah Kritis",  "🔴", "danger"),  lg=4, md=4, sm=12, className="mb-2"),
    ], className="g-3")


@callback(Output("ndvi-map", "figure"),
          Input("ndvi-filter-periode", "value"))
def update_map(periode):
    geojson = get_riau_geojson()
    df = _ndvi_agregat(periode)
    if df.empty:
        return go.Figure()

    # Encode status dominan ke angka untuk skala warna
    df["status_score"] = df["status_dominan"].map(STATUS_SCORE).fillna(2)

    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        locations="kode_wilayah",
        featureidkey="id",
        color="status_score",
        color_continuous_scale=[[0, "#D62839"], [0.5, "#F4A261"], [1, "#52B788"]],
        range_color=[1, 3],
        hover_data={"ndvi_mean": True, "status_dominan": True, "kode_wilayah": False, "status_score": False},
        mapbox_style="carto-positron",
        center={"lat": 0.5, "lon": 102.0},
        zoom=6.5,
        opacity=0.75,
    )
    fig.update_coloraxes(showscale=False)
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="white",
        font_family="Inter",
    )
    return fig


@callback(Output("ndvi-chart-bar", "figure"), Input("ndvi-filter-periode", "value"))
def update_bar(_):
    df = _ndvi_bar()
    if df.empty:
        return go.Figure()
    df["warna"] = df["status"].map(STATUS_COLOR_MAP)
    fig = go.Figure(go.Bar(
        x=df["ndvi_mean"], y=df["nama_kabupaten"],
        orientation="h",
        marker_color=df["warna"],
        text=df["ndvi_mean"].round(3),
        textposition="outside",
    ))
    fig.add_vline(x=0.5, line_dash="dash", line_color="#6B8F7E", line_width=1.5,
                  annotation_text="Threshold Normal", annotation_position="top right")
    fig.update_layout(
        template="plotly_white", height=380,
        margin=dict(l=0, r=40, t=10, b=0),
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(range=[0, 0.9]),
    )
    return fig


@callback(Output("ndvi-chart-tren", "figure"),
          Input("ndvi-filter-kab", "value"),
          Input("ndvi-filter-periode", "value"))
def update_tren(kab, _):
    kode = None if kab == "ALL" else kab
    df = _ndvi_tren(kode)
    if df.empty:
        return go.Figure()
    fig = px.line(
        df, x="periode", y="ndvi_mean", color="nama_kabupaten",
        labels={"ndvi_mean": "NDVI Mean", "periode": "Periode", "nama_kabupaten": ""},
        template="plotly_white", markers=True,
    )
    fig.add_hline(y=0.5, line_dash="dot", line_color="#2D6A4F",
                  annotation_text="Threshold", annotation_position="bottom right")
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=60), height=380,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30, tickfont=dict(size=9)),
        legend=dict(font=dict(size=9)),
    )
    return fig


def _kpi(value, label, icon, variant):
    return html.Div([
        html.Div(icon, style={"fontSize": "22px", "marginBottom": "6px"}),
        html.Div(value, className="kpi-value"),
        html.Div(label, className="kpi-label"),
    ], className=f"kpi-card {variant}")
