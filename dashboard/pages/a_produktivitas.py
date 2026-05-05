"""
pages/analytics/produktivitas.py — Produktivitas & K-Means Clustering
Minimalist Redesign: 3 Visualizations + Mascot Advice (Saran Decision)
"""

import dash
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_riau_geojson

dash.register_page(__name__, path="/analytics/produktivitas", name="Produktivitas & Cluster", order=6)

CLUSTER_COLOR = {
    "overperform":  "#2D6A4F",
    "average":      "#52B788",
    "underperform": "#D62839",
}


# ── Data Loaders ──────────────────────────────────────────────
def _scatter_data(tahun=None):
    cond = f"WHERE d.tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT d.periode, d.nama_perusahaan, d.produktivitas,
               d.cluster_produksi, d.nama_kabupaten, d.tahun, d.bulan
        FROM datamart.dm_gap_produksi d
        {cond}
        ORDER BY d.periode
    """)

def _line_cluster_data(tahun=None):
    cond = f"WHERE tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT periode, cluster_produksi,
               AVG(produktivitas) AS avg_prod
        FROM datamart.dm_gap_produksi
        {cond}
        GROUP BY periode, cluster_produksi
        ORDER BY periode
    """)

def _map_rata(tahun=None):
    cond = f"WHERE d.tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT p.kode_wilayah, k.nama_kabupaten, d.nama_perusahaan,
               ROUND(AVG(d.produktivitas)::numeric, 2) AS avg_prod,
               MODE() WITHIN GROUP (ORDER BY d.cluster_produksi) AS cluster_dominan
        FROM datamart.dm_gap_produksi d
        JOIN dim_perusahaan p ON d.nama_perusahaan = p.nama_perusahaan
        JOIN dim_kabupaten k ON p.kode_wilayah = k.kode_wilayah
        {cond}
        GROUP BY p.kode_wilayah, k.nama_kabupaten, d.nama_perusahaan
    """)

def _pie_cluster(tahun=None):
    cond = f"WHERE tahun = {tahun}" if tahun and tahun != "ALL" else ""
    return run_query(f"""
        SELECT cluster_produksi, COUNT(*) AS n
        FROM datamart.dm_gap_produksi {cond}
        WHERE cluster_produksi IS NOT NULL
        GROUP BY cluster_produksi
    """)

def _tahun_options():
    df = run_query("SELECT DISTINCT tahun FROM datamart.dm_gap_produksi ORDER BY tahun")
    opts = [{"label": "Semua Tahun", "value": "ALL"}]
    opts += [{"label": str(t), "value": t} for t in df["tahun"].tolist()]
    return opts

def _perusahaan_options():
    df = run_query("SELECT DISTINCT nama_perusahaan FROM datamart.dm_gap_produksi ORDER BY nama_perusahaan")
    opts = [{"label": "Semua Perusahaan", "value": "ALL"}]
    opts += [{"label": p, "value": p} for p in df["nama_perusahaan"].tolist()]
    return opts

def _cluster_options():
    opts = [
        {"label": "Semua Cluster", "value": "ALL"},
        {"label": "Overperform", "value": "overperform"},
        {"label": "Average", "value": "average"},
        {"label": "Underperform", "value": "underperform"},
    ]
    return opts


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("Produktivitas & K-Means Clustering"),
            html.P("Klasifikasi produktivitas perusahaan kelapa sawit (PKS) menggunakan algoritma K-Means (k=3): "
                   "Overperform | Average | Underperform."),
            html.Div(
                "Produktivitas = Realisasi Panen (Ton) ÷ Luas Kebun (Hektar)",
                style={
                    "backgroundColor": "#f4f7f5",
                    "border": "1px solid #cce3d8",
                    "padding": "4px 10px",
                    "borderRadius": "4px",
                    "color": "#2d6a4f",
                    "fontWeight": "600",
                    "fontSize": "13px",
                    "display": "inline-block",
                    "marginTop": "8px"
                }
            )
        ], className="page-header"),

        # Section 1: Filter Tahun Utama
        html.Div([
            html.Label("Filter Tahun:", style={"marginRight": "12px", "fontWeight": "600"}),
            dcc.Dropdown(
                id="prod-filter-tahun",
                options=_tahun_options(),
                value="ALL", clearable=False,
                style={"width": "180px"}
            )
        ], className="filter-bar mb-4"),

        # Section 2: Tren Rata-rata per Cluster (Primary) + Mascot Advice
        dbc.Row([
            # Kiri: Line Chart (Avg per Cluster)
            dbc.Col(html.Div([
                html.Div("Tren Rata-rata Produktivitas per Cluster", className="chart-card-title"),
                dcc.Graph(id="prod-chart-line", config={"displayModeBar": False}, style={"height": "380px"}),
            ], className="chart-card"), lg=8),
            
            # Kanan: Mascot Advice
            dbc.Col(html.Div([
                html.Div(className="advice-bg-overlay"),
                html.Div([
                    html.Div("Saran Decision", className="advice-title"),
                    html.Div(id="prod-mascot-advice-content", className="advice-text"),
                    html.Div(html.Img(src="/assets/mascot_1.webp", className="advice-mascot"), className="advice-mascot-container")
                ], className="advice-content")
            ], className="mascot-advice-card"), lg=4),
        ], className="g-4 align-items-stretch mb-4"),

        # Section 3: Map + Pie
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Sebaran Rata-rata Produktivitas (ton/ha)", className="chart-card-title"),
                dcc.Graph(id="prod-chart-map", config={"displayModeBar": False}, style={"height": "400px"}),
            ], className="chart-card"), lg=8),
            
            dbc.Col(html.Div([
                html.Div("Distribusi Cluster Pabrik", className="chart-card-title"),
                dcc.Graph(id="prod-chart-pie", config={"displayModeBar": False}, style={"height": "400px"}),
            ], className="chart-card"), lg=4),
        ], className="g-4 mb-4"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("prod-chart-line", "figure"),
          Input("prod-filter-tahun", "value"))
def update_line(tahun):
    df = _line_cluster_data(tahun)
    if df.empty:
        return go.Figure()
    
    fig = px.line(
        df, x="periode", y="avg_prod", color="cluster_produksi",
        color_discrete_map=CLUSTER_COLOR,
        labels={"avg_prod": "Avg Produktivitas (ton/ha)", "periode": "Periode", "cluster_produksi": "Cluster"},
        template="plotly_white", markers=True
    )
    fig.update_traces(line_width=3, marker_size=8)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=20), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig


@callback(Output("prod-chart-map", "figure"),
          Input("prod-filter-tahun", "value"))
def update_map(tahun):
    df = _map_rata(tahun)
    geojson = get_riau_geojson()
    if df.empty:
        return go.Figure()
        
    df["kode_wilayah"] = df["kode_wilayah"].astype(str)
    
    fig = px.choropleth_mapbox(
        df, geojson=geojson, color="avg_prod",
        locations="kode_wilayah", featureidkey="id",
        hover_name="nama_perusahaan",
        hover_data={"nama_kabupaten": True, "cluster_dominan": True, "kode_wilayah": False},
        color_continuous_scale="RdYlGn",
        mapbox_style="carto-positron", zoom=5.5, center={"lat": 0.5, "lon": 101.5},
        opacity=0.8,
        labels={"avg_prod": "Produktivitas", "cluster_dominan": "Dominan"}
    )
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        coloraxis_colorbar=dict(title="Ton/Ha")
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


@callback(Output("prod-mascot-advice-content", "children"),
          Input("prod-filter-tahun", "value"))
def update_advice(tahun):
    df = _map_rata(tahun)
    if df.empty:
        return '"Belum ada data evaluasi produktivitas untuk tahun ini."'
    
    # Hitung jumlah perusahaan per kategori cluster
    counts = df["cluster_dominan"].value_counts().to_dict()
    under = counts.get("underperform", 0)
    total = len(df)
    
    pct_under = under / total
    
    if pct_under > 0.3:
        return f'"{under} dari {total} Perusahaan Kelapa Sawit (PKS) berada dalam kategori Underperform secara rata-rata! Segera instruksikan dinas terkait untuk melakukan audit mesin dan evaluasi manajemen pabrik-pabrik tersebut."'
    elif under > 0:
        return f'"Sebagian besar pabrik ({total - under} PKS) sudah beroperasi dengan baik, namun masih ada {under} PKS yang Underperform secara rata-rata. Lakukan pembinaan terarah pada pabrik tersebut agar produktivitasnya meningkat."'
    else:
        return '"Luar biasa! Seluruh PKS beroperasi secara optimal pada standar Average hingga Overperform. Pertahankan terus kolaborasi ini!"'
