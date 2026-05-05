"""
pages/reporting/sawit_overview.py — Gambaran Umum Sawit Riau
Multi-chart: Profil Kebun | Harga CPO | Produksi Agregat
Sumber data: DWH langsung. Semua chart dirender di layout() tanpa callback filter.
"""

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_riau_geojson

dash.register_page(__name__, path="/reporting/sawit", name="Gambaran Sawit", order=1)

GREEN_PALETTE = ["#2D6A4F", "#40916C", "#52B788", "#74C69D", "#95D5B2", "#B7E4C7"]
STATUS_COLOR  = {"produktif": "#52B788", "TBM": "#F4A261", "replanting": "#D62839"}


def _fig_status():
    df = run_query("SELECT status_lahan, COUNT(*) AS jumlah FROM dim_kebun GROUP BY status_lahan")
    fig = px.pie(df, values="jumlah", names="status_lahan",
                 color="status_lahan", color_discrete_map=STATUS_COLOR,
                 hole=0.55, template="plotly_white")
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(showlegend=False, height=185, margin=dict(l=0,r=0,t=10,b=0),
                      font_family="Inter", paper_bgcolor="white")
    return fig

def _fig_varietas():
    df = run_query("""
        SELECT COALESCE(v.nama_varietas, 'Tidak Diketahui') AS varietas, COUNT(*) AS jumlah
        FROM dim_kebun k LEFT JOIN dim_varietas v ON k.varietas_id = v.varietas_id
        GROUP BY varietas ORDER BY jumlah DESC
    """)
    fig = px.bar(df, x="varietas", y="jumlah", color_discrete_sequence=GREEN_PALETTE,
                 labels={"jumlah": "Jumlah Kebun", "varietas": ""}, template="plotly_white")
    fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=185,
                      font_family="Inter", paper_bgcolor="white", plot_bgcolor="white")
    return fig

def _fig_produksi_bulanan():
    df = run_query("SELECT periode, SUM(produksi_tbs_ton) AS total FROM fact_produksi GROUP BY periode ORDER BY periode")
    if df.empty:
        return go.Figure()
    fig = px.area(df, x="periode", y="total",
                  labels={"total":"Total TBS (ton)","periode":"Periode"},
                  color_discrete_sequence=["#52B788"], template="plotly_white")
    fig.update_traces(line_color="#2D6A4F", fillcolor="rgba(82,183,136,0.18)")
    fig.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=260,
                      font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
                      xaxis=dict(tickangle=-30))
    return fig

def _fig_produksi_kab():
    df = run_query("""
        SELECT k.kode_wilayah, k.nama_kabupaten, SUM(f.produksi_tbs_ton) AS total
        FROM fact_produksi f JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
        GROUP BY k.kode_wilayah, k.nama_kabupaten ORDER BY total DESC
    """)
    if df.empty:
        return go.Figure()
        
    geojson = get_riau_geojson()
    fig = px.choropleth_mapbox(
        df,
        geojson=geojson,
        locations="kode_wilayah",
        featureidkey="id",
        color="total",
        color_continuous_scale="Greens",
        hover_data={"nama_kabupaten": True, "total": True, "kode_wilayah": False},
        mapbox_style="carto-positron",
        center={"lat": 0.5, "lon": 102.0},
        zoom=6.5,
        opacity=0.8,
        labels={"total": "Total Produksi (Ton)"}
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="white",
        font_family="Inter",
    )
    return fig


def layout():
    return html.Div([
        html.Div([
            html.H2("Gambaran Umum Sawit Riau"),
            html.P("Profil kebun dan agregasi produksi TBS seluruh PKS Riau"),
        ], className="page-header"),

        html.P("Profil Kebun & Varietas", style={"fontWeight":"600","color":"var(--primary-dark)","marginBottom":"12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Status Lahan", className="chart-card-title"),
                dcc.Graph(figure=_fig_status(), config={"displayModeBar": False}),
            ], className="chart-card"), lg=6),
            dbc.Col(html.Div([
                html.Div("Distribusi Varietas", className="chart-card-title"),
                dcc.Graph(figure=_fig_varietas(), config={"displayModeBar": False}),
            ], className="chart-card"), lg=6),
        ], className="g-3 mb-4"),

        html.P("Produksi TBS Agregat", style={"fontWeight":"600","color":"var(--primary-dark)","marginBottom":"12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Peta Produksi per Kabupaten (Ton)", className="chart-card-title"),
                dcc.Graph(figure=_fig_produksi_kab(), config={"displayModeBar": False, "scrollZoom": True}, style={"height": "480px"}),
            ], className="chart-card"), xs=12),
        ], className="g-3 mb-4"),
        
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Total Produksi TBS Bulanan (Semua PKS)", className="chart-card-title"),
                dcc.Graph(figure=_fig_produksi_bulanan(), config={"displayModeBar": False}),
            ], className="chart-card"), xs=12),
        ], className="g-3"),
    ])
