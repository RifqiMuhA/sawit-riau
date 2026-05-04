"""
pages/analytics/penimbunan.py — Deteksi Penimbunan CPO
Multi-chart: Dual-Axis Line (Harga CPO vs Volume) | Bar Insiden | Scatter Stok | Tabel
Sumber data: datamart.dm_deteksi_penimbunan
"""

import dash
from dash import html, dcc, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_perusahaan_options

dash.register_page(__name__, path="/analytics/penimbunan", name="Deteksi Penimbunan", order=7)


# ── Data Loaders ──────────────────────────────────────────────
def _dual_axis_data(pid):
    cond = f"WHERE nama_perusahaan = '{pid}'" if pid != "ALL" else ""
    sql = f"""
        SELECT periode, bulan, tahun,
               AVG(harga_cpo)              AS harga_cpo,
               SUM(volume_penjualan_ton)   AS vol_jual,
               BOOL_OR(indikasi_timbun)    AS ada_timbun
        FROM datamart.dm_deteksi_penimbunan
        {cond}
        GROUP BY periode, bulan, tahun ORDER BY periode
    """
    return run_query(sql)

def _bar_insiden():
    return run_query("""
        SELECT nama_perusahaan,
               COUNT(*) FILTER (WHERE indikasia_timbun) AS insiden
        FROM (
            SELECT nama_perusahaan,
                   BOOL_OR(indikasi_timbun) AS indikasia_timbun
            FROM datamart.dm_deteksi_penimbunan
            GROUP BY nama_perusahaan, periode
        ) t
        GROUP BY nama_perusahaan ORDER BY insiden DESC
    """)

def _scatter_stok(pid):
    cond = f"WHERE nama_perusahaan = '{pid}'" if pid != "ALL" else ""
    return run_query(f"""
        SELECT periode, nama_perusahaan, stok_akhir_ton,
               volume_penjualan_ton, indikasi_timbun
        FROM datamart.dm_deteksi_penimbunan
        {cond}
        ORDER BY periode
    """)

def _tabel_timbun(pid):
    cond = f"WHERE nama_perusahaan = '{pid}' AND" if pid != "ALL" else "WHERE"
    return run_query(f"""
        SELECT periode AS "Periode", nama_perusahaan AS "Perusahaan",
               ROUND(harga_cpo::numeric, 0) AS "Harga CPO (Rp/kg)",
               ROUND(volume_penjualan_ton::numeric, 2) AS "Vol. Jual (ton)",
               ROUND(stok_akhir_ton::numeric, 2) AS "Stok Akhir (ton)",
               indikasi_timbun AS "Indikasi Timbun"
        FROM datamart.dm_deteksi_penimbunan
        {cond} indikasi_timbun = TRUE
        ORDER BY periode DESC
        LIMIT 100
    """)

def _kpi_timbun(pid):
    cond = f"WHERE nama_perusahaan = '{pid}'" if pid != "ALL" else ""
    df = run_query(f"""
        SELECT
            COUNT(*) FILTER (WHERE indikasi_timbun = TRUE) AS insiden,
            COUNT(*)                                        AS total,
            ROUND(AVG(harga_cpo)::numeric, 0)              AS avg_harga
        FROM datamart.dm_deteksi_penimbunan {cond}
    """)
    return df.iloc[0]


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("Deteksi Penimbunan CPO"),
            html.P("Identifikasi indikasi penahanan stok CPO saat harga turun — "
                   "berdasarkan 3 kondisi simultan: harga turun, volume jual turun, stok naik"),
        ], className="page-header"),

        # Filter
        html.Div([
            html.Label("Perusahaan:"),
            dcc.Dropdown(
                id="timbun-filter-pks",
                options=get_perusahaan_options(),
                value="ALL", clearable=False,
                style={"minWidth": "280px"},
            ),
        ], className="filter-bar"),

        # KPI
        html.Div(id="timbun-kpi", className="mb-3"),

        # Section 1: Dual Axis Line (lebar penuh)
        html.P("Korelasi Harga CPO vs Volume Penjualan", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Dual-Axis Line Chart — ★ = Bulan Terindikasi Penimbunan", className="chart-card-title"),
            dcc.Graph(id="timbun-chart-dual", config={"displayModeBar": False}, style={"height": "360px"}),
        ], className="chart-card mb-2"),

        # Section 2: Bar + Scatter
        html.P("Distribusi Insiden & Korelasi Stok", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Bar: Jumlah Bulan Terindikasi per Perusahaan", className="chart-card-title"),
                dcc.Graph(id="timbun-chart-bar", config={"displayModeBar": False}),
            ], className="chart-card"), lg=5),
            dbc.Col(html.Div([
                html.Div("Scatter: Stok Akhir vs Volume Jual (Merah = Terindikasi)", className="chart-card-title"),
                dcc.Graph(id="timbun-chart-scatter", config={"displayModeBar": False}),
            ], className="chart-card"), lg=7),
        ], className="g-3 mb-2"),

        # Section 3: Tabel
        html.P("Daftar Periode Terindikasi Penimbunan", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Tabel Detail — Hanya menampilkan baris indikasi_timbun = TRUE", className="chart-card-title"),
            html.Div(id="timbun-tabel"),
        ], className="chart-card"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("timbun-kpi", "children"), Input("timbun-filter-pks", "value"))
def update_kpi(pid):
    k = _kpi_timbun(pid)
    total = int(k["total"]) or 1
    pct = f"{100 * int(k['insiden']) / total:.1f}%"
    return dbc.Row([
        dbc.Col(_kpi(str(int(k["insiden"])),           "Total Insiden Penimbunan", "⚠️", "danger"), lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(pct,                               "Rasio Periode Terindikasi","📊", "warning"),lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"Rp {k['avg_harga']:,.0f}/kg",   "Rata-rata Harga CPO",     "💰", ""),        lg=4, md=4, sm=12, className="mb-2"),
    ], className="g-3")


@callback(Output("timbun-chart-dual", "figure"), Input("timbun-filter-pks", "value"))
def update_dual(pid):
    df = _dual_axis_data(pid)
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    # Garis Harga CPO (sumbu kanan)
    fig.add_trace(go.Scatter(
        x=df["periode"], y=df["harga_cpo"],
        name="Harga CPO (Rp/kg)", mode="lines+markers",
        line=dict(color="#F4A261", width=2.5),
        marker=dict(size=5),
        yaxis="y2",
    ))

    # Garis Volume Penjualan (sumbu kiri)
    fig.add_trace(go.Scatter(
        x=df["periode"], y=df["vol_jual"],
        name="Volume Penjualan (ton)", mode="lines+markers",
        line=dict(color="#4895EF", width=2.5),
        marker=dict(size=5),
    ))

    # Marker ★ merah saat ada indikasi timbun
    timbun_df = df[df["ada_timbun"] == True]
    if not timbun_df.empty:
        fig.add_trace(go.Scatter(
            x=timbun_df["periode"], y=timbun_df["vol_jual"],
            name="Indikasi Penimbunan ★",
            mode="markers",
            marker=dict(color="#D62839", size=14, symbol="star"),
        ))

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=0, r=60, t=10, b=60),
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        yaxis=dict(title="Volume Penjualan (ton)", titlefont_color="#4895EF"),
        yaxis2=dict(title="Harga CPO (Rp/kg)", titlefont_color="#F4A261",
                    overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis=dict(tickangle=-30),
    )
    return fig


@callback(Output("timbun-chart-bar", "figure"), Input("timbun-filter-pks", "value"))
def update_bar(_):
    df = _bar_insiden()
    if df.empty:
        return go.Figure()
    fig = px.bar(
        df, x="insiden", y="nama_perusahaan", orientation="h",
        color="insiden",
        color_continuous_scale=[[0, "#B7E4C7"], [1, "#D62839"]],
        labels={"insiden": "Jumlah Bulan", "nama_perusahaan": ""},
        template="plotly_white",
    )
    fig.update_coloraxes(showscale=False)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


@callback(Output("timbun-chart-scatter", "figure"), Input("timbun-filter-pks", "value"))
def update_scatter(pid):
    df = _scatter_stok(pid)
    if df.empty:
        return go.Figure()
    df["warna"] = df["indikasi_timbun"].map({True: "#D62839", False: "#52B788", None: "#B7E4C7"})
    fig = px.scatter(
        df, x="volume_penjualan_ton", y="stok_akhir_ton",
        color="indikasi_timbun",
        color_discrete_map={True: "#D62839", False: "#52B788"},
        hover_data=["periode", "nama_perusahaan"],
        labels={"volume_penjualan_ton": "Volume Jual (ton)",
                "stok_akhir_ton": "Stok Akhir (ton)",
                "indikasi_timbun": "Terindikasi"},
        template="plotly_white",
    )
    fig.update_traces(marker_size=8, marker_opacity=0.8)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


@callback(Output("timbun-tabel", "children"), Input("timbun-filter-pks", "value"))
def update_tabel(pid):
    df = _tabel_timbun(pid)
    if df.empty:
        return html.P("Tidak ada data penimbunan untuk filter yang dipilih.",
                      style={"color": "var(--text-muted)", "padding": "12px"})
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#D62839", "color": "white", "fontWeight": "600", "fontSize": "12px"},
        style_cell={"fontSize": "12px", "padding": "8px 12px", "border": "1px solid #D8EAE2"},
        style_data_conditional=[
            {"if": {"filter_query": "{Indikasi Timbun} = 'True'", "column_id": "Indikasi Timbun"},
             "backgroundColor": "#FFE5E8", "color": "#D62839", "fontWeight": "700"},
            {"if": {"row_index": "odd"}, "backgroundColor": "#FFF5F5"},
        ],
        sort_action="native",
    )


def _kpi(value, label, icon, variant):
    return html.Div([
        html.Div(icon, style={"fontSize": "22px", "marginBottom": "6px"}),
        html.Div(value, className="kpi-value"),
        html.Div(label, className="kpi-label"),
    ], className=f"kpi-card {variant}")
