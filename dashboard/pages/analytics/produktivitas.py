"""
pages/analytics/produktivitas.py — Produktivitas & K-Means Clustering
Multi-chart: Scatter Cluster | Bar Rata-rata | Pie Distribusi | Multi-line Tren
Sumber data: datamart.dm_gap_produksi
"""

import dash
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_perusahaan_options, get_kabupaten_options

dash.register_page(__name__, path="/analytics/produktivitas", name="Produktivitas & Cluster", order=6)

CLUSTER_COLOR = {
    "overperform":  "#2D6A4F",
    "average":      "#52B788",
    "underperform": "#D62839",
}


# ── Data Loaders ──────────────────────────────────────────────
def _scatter_data(tahun=None, kab=None):
    cond = []
    if tahun and tahun != "ALL":
        cond.append(f"d.tahun = {tahun}")
    if kab and kab != "ALL":
        cond.append(f"d.nama_kabupaten = '{kab}'")
    where = ("WHERE " + " AND ".join(cond)) if cond else ""
    return run_query(f"""
        SELECT d.periode, d.nama_perusahaan, d.produktivitas,
               d.cluster_produksi, d.nama_kabupaten, d.tahun, d.bulan
        FROM datamart.dm_gap_produksi d
        {where}
        ORDER BY d.periode
    """)

def _bar_rata(tahun=None):
    cond = f"WHERE tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT nama_perusahaan,
               ROUND(AVG(produktivitas)::numeric, 2) AS avg_prod,
               MODE() WITHIN GROUP (ORDER BY cluster_produksi) AS cluster
        FROM datamart.dm_gap_produksi {cond}
        GROUP BY nama_perusahaan ORDER BY avg_prod DESC
    """)

def _pie_cluster(tahun=None):
    cond = f"WHERE tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT cluster_produksi, COUNT(*) AS n
        FROM datamart.dm_gap_produksi {cond}
        WHERE cluster_produksi IS NOT NULL
        GROUP BY cluster_produksi
    """)

def _tren_top_bottom():
    return run_query("""
        WITH avg_prod AS (
            SELECT nama_perusahaan, AVG(produktivitas) AS avg_p
            FROM datamart.dm_gap_produksi GROUP BY nama_perusahaan
        ),
        ranked AS (
            SELECT nama_perusahaan,
                   ROW_NUMBER() OVER (ORDER BY avg_p DESC) AS rn_top,
                   ROW_NUMBER() OVER (ORDER BY avg_p ASC)  AS rn_bot
            FROM avg_prod
        ),
        selected AS (
            SELECT nama_perusahaan FROM ranked
            WHERE rn_top <= 3 OR rn_bot <= 3
        )
        SELECT d.periode, d.nama_perusahaan,
               ROUND(AVG(d.produktivitas)::numeric, 2) AS produktivitas
        FROM datamart.dm_gap_produksi d
        JOIN selected s ON d.nama_perusahaan = s.nama_perusahaan
        GROUP BY d.periode, d.nama_perusahaan ORDER BY d.periode
    """)

def _kpi_prod(tahun=None):
    cond = f"WHERE tahun = {tahun}" if tahun and tahun != "ALL" else ""
    df = run_query(f"""
        SELECT ROUND(AVG(produktivitas)::numeric, 2) AS avg_prod,
               ROUND(MAX(produktivitas)::numeric, 2) AS max_prod,
               ROUND(MIN(produktivitas)::numeric, 2) AS min_prod
        FROM datamart.dm_gap_produksi {cond}
    """)
    return df.iloc[0]

def _tahun_options():
    df = run_query("SELECT DISTINCT tahun FROM datamart.dm_gap_produksi ORDER BY tahun")
    opts = [{"label": "Semua Tahun", "value": "ALL"}]
    opts += [{"label": str(t), "value": t} for t in df["tahun"].tolist()]
    return opts


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("Produktivitas & K-Means Clustering"),
            html.P("Klasifikasi produktivitas perusahaan PKS menggunakan K-Means (k=3): "
                   "Overperform | Average | Underperform"),
        ], className="page-header"),

        # Filter
        html.Div([
            html.Label("Tahun:"),
            dcc.Dropdown(id="prod-filter-tahun", options=_tahun_options(),
                         value="ALL", clearable=False, style={"minWidth": "150px"}),
            html.Label("Kabupaten:"),
            dcc.Dropdown(id="prod-filter-kab", options=get_kabupaten_options(),
                         value="ALL", clearable=False, style={"minWidth": "220px"}),
        ], className="filter-bar"),

        # KPI
        html.Div(id="prod-kpi", className="mb-3"),

        # Section 1: Scatter (lebar penuh)
        html.P("Sebaran Cluster Produktivitas", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Scatter Plot: Produktivitas per Perusahaan, Warna = Cluster K-Means", className="chart-card-title"),
            dcc.Graph(id="prod-chart-scatter", config={"displayModeBar": False}, style={"height": "340px"}),
        ], className="chart-card mb-2"),

        # Section 2: Bar + Pie
        html.P("Distribusi & Rata-rata", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Rata-rata Produktivitas per Perusahaan (ton/ha)", className="chart-card-title"),
                dcc.Graph(id="prod-chart-bar", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Distribusi Cluster", className="chart-card-title"),
                dcc.Graph(id="prod-chart-pie", config={"displayModeBar": False}),
            ], className="chart-card"), lg=4),
        ], className="g-3 mb-2"),

        # Section 3: Tren Top vs Bottom
        html.P("Tren Produktivitas: 3 Terbaik vs 3 Terburuk", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Multi-Line Chart: Tren Produktivitas Perusahaan Terpilih", className="chart-card-title"),
            dcc.Graph(id="prod-chart-tren", config={"displayModeBar": False}),
        ], className="chart-card"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("prod-kpi", "children"),
          Input("prod-filter-tahun", "value"),
          Input("prod-filter-kab", "value"))
def update_kpi(tahun, _):
    k = _kpi_prod(tahun)
    return dbc.Row([
        dbc.Col(_kpi(f"{k['avg_prod'] or 0:.2f} ton/ha", "Rata-rata Produktivitas", "📊", ""),       lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"{k['max_prod'] or 0:.2f} ton/ha", "Produktivitas Tertinggi",  "🏆", ""),       lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"{k['min_prod'] or 0:.2f} ton/ha", "Produktivitas Terendah",   "⚠️", "danger"), lg=4, md=4, sm=12, className="mb-2"),
    ], className="g-3")


@callback(Output("prod-chart-scatter", "figure"),
          Input("prod-filter-tahun", "value"),
          Input("prod-filter-kab", "value"))
def update_scatter(tahun, kab):
    df = _scatter_data(tahun, kab)
    if df.empty:
        return go.Figure()
    fig = px.scatter(
        df, x="periode", y="produktivitas",
        color="cluster_produksi", color_discrete_map=CLUSTER_COLOR,
        symbol="nama_perusahaan",
        hover_data=["nama_perusahaan", "nama_kabupaten"],
        labels={"produktivitas": "Produktivitas (ton/ha)", "periode": "Periode",
                "cluster_produksi": "Cluster"},
        template="plotly_white",
    )
    fig.update_traces(marker_size=9, marker_opacity=0.85)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=60), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
    )
    return fig


@callback(Output("prod-chart-bar", "figure"),
          Input("prod-filter-tahun", "value"),
          Input("prod-filter-kab", "value"))
def update_bar(tahun, _):
    df = _bar_rata(tahun)
    if df.empty:
        return go.Figure()
    df["warna"] = df["cluster"].map(CLUSTER_COLOR)
    fig = go.Figure(go.Bar(
        y=df["nama_perusahaan"], x=df["avg_prod"],
        orientation="h",
        marker_color=df["warna"],
        text=df["avg_prod"],
        texttemplate="%{text:.2f}",
        textposition="outside",
    ))
    fig.update_layout(
        template="plotly_white", height=340,
        margin=dict(l=0, r=50, t=10, b=0), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


@callback(Output("prod-chart-pie", "figure"), Input("prod-filter-tahun", "value"))
def update_pie(tahun):
    df = _pie_cluster(tahun)
    if df.empty:
        return go.Figure()
    fig = px.pie(
        df, values="n", names="cluster_produksi",
        color="cluster_produksi", color_discrete_map=CLUSTER_COLOR,
        hole=0.55, template="plotly_white",
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(
        showlegend=False, height=340,
        margin=dict(l=0, r=0, t=10, b=0),
        font_family="Inter", paper_bgcolor="white",
    )
    return fig


@callback(Output("prod-chart-tren", "figure"),
          Input("prod-filter-tahun", "value"),
          Input("prod-filter-kab", "value"))
def update_tren(_, __):
    df = _tren_top_bottom()
    if df.empty:
        return go.Figure()
    fig = px.line(
        df, x="periode", y="produktivitas", color="nama_perusahaan",
        labels={"produktivitas": "Produktivitas (ton/ha)", "periode": "Periode", "nama_perusahaan": ""},
        template="plotly_white", markers=True,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=60), height=300,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
        legend=dict(font=dict(size=9)),
    )
    return fig


def _kpi(value, label, icon, variant):
    return html.Div([
        html.Div(icon, style={"fontSize": "22px", "marginBottom": "6px"}),
        html.Div(value, className="kpi-value"),
        html.Div(label, className="kpi-label"),
    ], className=f"kpi-card {variant}")
