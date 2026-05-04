"""
pages/reporting/sawit_overview.py — Gambaran Umum Sawit Riau
Multi-chart: Profil Kebun | Harga CPO | Produksi Agregat
Sumber data: DWH langsung (fact + dim tables)
"""

import dash
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db import run_query

dash.register_page(__name__, path="/reporting/sawit", name="Gambaran Sawit", order=1)


# ── Data Loaders ──────────────────────────────────────────────
def _luas_per_perusahaan():
    return run_query("""
        SELECT p.nama_perusahaan, SUM(k.luas_ha) AS total_luas, k.status_lahan
        FROM dim_kebun k
        JOIN dim_perusahaan p ON k.perusahaan_id = p.perusahaan_id
        GROUP BY p.nama_perusahaan, k.status_lahan
        ORDER BY total_luas DESC
    """)

def _status_lahan():
    return run_query("""
        SELECT status_lahan, COUNT(*) AS jumlah
        FROM dim_kebun GROUP BY status_lahan
    """)

def _varietas_dist():
    return run_query("""
        SELECT COALESCE(v.nama_varietas, 'Tidak Diketahui') AS varietas,
               COUNT(*) AS jumlah
        FROM dim_kebun k
        LEFT JOIN dim_varietas v ON k.varietas_id = v.varietas_id
        GROUP BY varietas ORDER BY jumlah DESC
    """)

def _harga_cpo():
    return run_query("""
        SELECT periode, tahun, bulan, harga_cpo
        FROM dim_periode
        WHERE harga_cpo IS NOT NULL AND tahun >= 2023
        ORDER BY periode
    """)

def _produksi_bulanan():
    return run_query("""
        SELECT f.periode, SUM(f.produksi_tbs_ton) AS total_produksi
        FROM fact_produksi f
        GROUP BY f.periode ORDER BY f.periode
    """)

def _produksi_per_kabupaten():
    return run_query("""
        SELECT k.nama_kabupaten, SUM(f.produksi_tbs_ton) AS total
        FROM fact_produksi f
        JOIN dim_kabupaten k ON f.kode_wilayah = k.kode_wilayah
        GROUP BY k.nama_kabupaten ORDER BY total DESC
    """)


# ── Warna ─────────────────────────────────────────────────────
GREEN_PALETTE = ["#2D6A4F", "#40916C", "#52B788", "#74C69D", "#95D5B2", "#B7E4C7"]
STATUS_COLOR  = {"produktif": "#52B788", "TBM": "#F4A261", "replanting": "#D62839"}


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        # Header
        html.Div([
            html.H2("Gambaran Umum Sawit Riau"),
            html.P("Profil kebun, tren harga CPO, dan agregasi produksi TBS seluruh PKS Riau"),
        ], className="page-header"),

        # ── Section 1: Profil Kebun ──────────────────────────
        html.P("Profil Kebun & Varietas", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Div("Distribusi Luas (Ha) per Perusahaan", className="chart-card-title"),
                    dcc.Graph(id="chart-luas-perusahaan", config={"displayModeBar": False}),
                ], className="chart-card"),
            ], lg=6),
            dbc.Col([
                dbc.Row([
                    dbc.Col(html.Div([
                        html.Div("Status Lahan", className="chart-card-title"),
                        dcc.Graph(id="chart-status-lahan", config={"displayModeBar": False}, style={"height": "200px"}),
                    ], className="chart-card"), xs=12),
                    dbc.Col(html.Div([
                        html.Div("Distribusi Varietas", className="chart-card-title"),
                        dcc.Graph(id="chart-varietas", config={"displayModeBar": False}, style={"height": "200px"}),
                    ], className="chart-card"), xs=12),
                ]),
            ], lg=6),
        ], className="g-3 mb-2"),

        # ── Section 2: Harga CPO ─────────────────────────────
        html.P("Tren Harga CPO Riau", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Harga CPO Plasma Disbun Riau (Rp/kg)", className="chart-card-title"),
                dcc.Graph(id="chart-harga-cpo", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Ringkasan Harga per Tahun", className="chart-card-title"),
                html.Div(id="tabel-harga-ringkasan"),
            ], className="chart-card"), lg=4),
        ], className="g-3 mb-2"),

        # ── Section 3: Produksi Agregat ──────────────────────
        html.P("Produksi TBS Agregat", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Total Produksi TBS Bulanan (Semua PKS)", className="chart-card-title"),
                dcc.Graph(id="chart-produksi-bulanan", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Produksi per Kabupaten (Ton)", className="chart-card-title"),
                dcc.Graph(id="chart-produksi-kabupaten", config={"displayModeBar": False}),
            ], className="chart-card"), lg=4),
        ], className="g-3"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("chart-luas-perusahaan", "figure"))
def update_luas(_=None):
    df = _luas_per_perusahaan()
    fig = px.bar(
        df, x="total_luas", y="nama_perusahaan", color="status_lahan",
        orientation="h",
        color_discrete_map=STATUS_COLOR,
        labels={"total_luas": "Luas (Ha)", "nama_perusahaan": ""},
        template="plotly_white",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), legend_title="Status",
        plot_bgcolor="white", paper_bgcolor="white",
        font_family="Inter", height=320,
    )
    return fig


@callback(Output("chart-status-lahan", "figure"))
def update_status(_=None):
    df = _status_lahan()
    fig = px.pie(
        df, values="jumlah", names="status_lahan",
        color="status_lahan", color_discrete_map=STATUS_COLOR,
        hole=0.55, template="plotly_white",
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False, height=180,
        font_family="Inter", paper_bgcolor="white",
    )
    return fig


@callback(Output("chart-varietas", "figure"))
def update_varietas(_=None):
    df = _varietas_dist()
    fig = px.bar(
        df, x="varietas", y="jumlah",
        color_discrete_sequence=GREEN_PALETTE,
        template="plotly_white",
        labels={"jumlah": "Jumlah Kebun", "varietas": ""},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=180,
        font_family="Inter", paper_bgcolor="white",
        plot_bgcolor="white",
    )
    return fig


@callback(Output("chart-harga-cpo", "figure"))
def update_harga(_=None):
    df = _harga_cpo()
    if df.empty:
        return go.Figure()
    fig = px.line(
        df, x="periode", y="harga_cpo",
        markers=True,
        labels={"harga_cpo": "Harga (Rp/kg)", "periode": "Periode"},
        color_discrete_sequence=["#2D6A4F"],
        template="plotly_white",
    )
    fig.update_traces(line_width=2.5, marker_size=6)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=260,
        font_family="Inter", paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
    )
    return fig


@callback(Output("tabel-harga-ringkasan", "children"))
def update_tabel_harga(_=None):
    df = _harga_cpo()
    if df.empty:
        return html.P("Data belum tersedia", style={"color": "var(--text-muted)", "fontSize": "13px"})
    summary = df.groupby("tahun")["harga_cpo"].agg(
        Min="min", Max="max", Avg="mean"
    ).reset_index()
    rows = []
    for _, row in summary.iterrows():
        rows.append(html.Tr([
            html.Td(str(int(row["tahun"])), style={"fontWeight": "600"}),
            html.Td(f"Rp {row['Min']:,.0f}"),
            html.Td(f"Rp {row['Max']:,.0f}"),
            html.Td(f"Rp {row['Avg']:,.0f}"),
        ]))
    return dbc.Table(
        [html.Thead(html.Tr([html.Th("Tahun"), html.Th("Min"), html.Th("Max"), html.Th("Rata-rata")])),
         html.Tbody(rows)],
        bordered=False, hover=True, size="sm",
        style={"fontSize": "12px"},
    )


@callback(Output("chart-produksi-bulanan", "figure"))
def update_produksi_bulanan(_=None):
    df = _produksi_bulanan()
    if df.empty:
        return go.Figure()
    fig = px.area(
        df, x="periode", y="total_produksi",
        labels={"total_produksi": "Total TBS (ton)", "periode": "Periode"},
        color_discrete_sequence=["#52B788"],
        template="plotly_white",
    )
    fig.update_traces(line_color="#2D6A4F", fillcolor="rgba(82,183,136,0.18)")
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=260,
        font_family="Inter", paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
    )
    return fig


@callback(Output("chart-produksi-kabupaten", "figure"))
def update_produksi_kab(_=None):
    df = _produksi_per_kabupaten()
    if df.empty:
        return go.Figure()
    fig = px.bar(
        df, x="total", y="nama_kabupaten", orientation="h",
        color_discrete_sequence=["#40916C"],
        labels={"total": "Total (ton)", "nama_kabupaten": ""},
        template="plotly_white",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=260,
        font_family="Inter", paper_bgcolor="white",
        plot_bgcolor="white",
    )
    return fig
