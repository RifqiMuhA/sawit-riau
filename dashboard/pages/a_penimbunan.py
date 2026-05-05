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
from plotly.subplots import make_subplots
from db import run_query, get_perusahaan_options, get_riau_geojson

dash.register_page(__name__, path="/analytics/penimbunan", name="Deteksi Penimbunan", order=7)


# ── Data Loaders ──────────────────────────────────────────────
def _chart_data_agregat():
    sql = """
        SELECT periode,
               ROUND(AVG(harga_cpo)::numeric, 0) AS harga_cpo,
               COUNT(*) FILTER (WHERE indikasi_timbun = TRUE) AS jumlah_insiden
        FROM datamart.dm_deteksi_penimbunan
        GROUP BY periode, tahun, bulan
        ORDER BY tahun, bulan
    """
    return run_query(sql)

def _chart_data_pks(pid):
    sql = f"""
        SELECT periode,
               harga_cpo,
               volume_penjualan_ton AS vol_jual,
               stok_akhir_ton AS stok,
               indikasi_timbun AS ada_timbun
        FROM datamart.dm_deteksi_penimbunan
        WHERE nama_perusahaan = (SELECT nama_perusahaan FROM dim_perusahaan WHERE perusahaan_id = '{pid}')
        ORDER BY tahun, bulan
    """
    return run_query(sql)

def _bar_insiden(pid="ALL"):
    cond = f"WHERE nama_perusahaan = (SELECT nama_perusahaan FROM dim_perusahaan WHERE perusahaan_id = '{pid}')" if pid != "ALL" else ""
    return run_query(f"""
        SELECT nama_perusahaan,
               COUNT(*) FILTER (WHERE indikasia_timbun) AS insiden
        FROM (
            SELECT nama_perusahaan,
                   BOOL_OR(indikasi_timbun) AS indikasia_timbun
            FROM datamart.dm_deteksi_penimbunan
            {cond}
            GROUP BY nama_perusahaan, periode
        ) t
        GROUP BY nama_perusahaan ORDER BY insiden DESC LIMIT 5
    """)

def _map_timbun_data(pid="ALL"):
    cond = f"AND p.perusahaan_id = '{pid}'" if pid != "ALL" else ""
    sql = f"""
        SELECT p.kode_wilayah, k.nama_kabupaten,
               COUNT(*) AS total_insiden
        FROM datamart.dm_deteksi_penimbunan d
        JOIN dim_perusahaan p ON d.nama_perusahaan = p.nama_perusahaan
        JOIN dim_kabupaten k ON p.kode_wilayah = k.kode_wilayah
        WHERE d.indikasi_timbun = TRUE {cond}
        GROUP BY p.kode_wilayah, k.nama_kabupaten
    """
    return run_query(sql)

def _scatter_stok(pid):
    cond = f"WHERE nama_perusahaan = (SELECT nama_perusahaan FROM dim_perusahaan WHERE perusahaan_id = '{pid}')" if pid != "ALL" else ""
    return run_query(f"""
        SELECT periode, nama_perusahaan, stok_akhir_ton,
               volume_penjualan_ton, indikasi_timbun
        FROM datamart.dm_deteksi_penimbunan
        {cond}
        ORDER BY periode
    """)

def _tabel_timbun(pid):
    cond = f"WHERE nama_perusahaan = (SELECT nama_perusahaan FROM dim_perusahaan WHERE perusahaan_id = '{pid}') AND" if pid != "ALL" else "WHERE"
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
    cond = f"WHERE nama_perusahaan = (SELECT nama_perusahaan FROM dim_perusahaan WHERE perusahaan_id = '{pid}')" if pid != "ALL" else ""
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
            html.P("Identifikasi indikasi penahanan stok CPO saat harga turun "
                   "berdasarkan 3 kondisi simultan: harga turun, volume jual turun, stok naik"),
            html.Div(
                "Indikasi Penimbunan = Volume Jual Turun & Stok Naik & Harga Pasar Turun",
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

        # KPI & Mascot Advice Row
        html.Div(id="timbun-kpi", className="mb-3"),

        # Section 1: Line & Top 5 (Conditional)
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("★ = Bulan Terindikasi Penimbunan", id="timbun-chart-dual-title", className="chart-card-title"),
                dcc.Graph(id="timbun-chart-dual", config={"displayModeBar": False}, style={"height": "340px"}),
            ], className="chart-card"), id="col-line", lg=7),
            
            dbc.Col(html.Div([
                html.Div("Top 5 Perusahaan Terindikasi", className="chart-card-title"),
                dcc.Graph(id="timbun-chart-bar", config={"displayModeBar": False}, style={"height": "340px"}),
            ], className="chart-card"), id="col-top5", lg=5),
        ], className="g-3 mb-4 align-items-stretch"),

        # Section 2: Map (Full Row)
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Sebaran Insiden per Kabupaten", className="chart-card-title"),
                dcc.Graph(id="timbun-chart-map", config={"displayModeBar": False}, style={"height": "450px"}),
            ], className="chart-card"), lg=12),
        ], className="mb-4"),

        # Section 3: Tabel
        html.P("Daftar Periode Terindikasi Penimbunan", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Periode terindikasi penimbunan", className="chart-card-title"),
            html.Div(id="timbun-tabel"),
        ], className="chart-card"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("timbun-kpi", "children"), Input("timbun-filter-pks", "value"))
def update_kpi(pid):
    k = _kpi_timbun(pid)
    total = int(k["total"]) or 1
    insiden = int(k["insiden"])
    pct = f"{100 * insiden / total:.1f}%"
    
    penjelasan_dasar = "Sistem mendeteksi indikasi penimbunan ketika terjadi 3 kondisi simultan: Harga CPO sedang turun, namun Volume Penjualan pabrik menurun drastis sementara Stok Akhir malah menumpuk."
    if insiden > 0:
        if pid == "ALL":
            advice_text = f'"Secara total provinsi, terdapat {insiden} temuan indikasi penimbunan. Cek grafik \'Top 5\' di bawah untuk melihat perusahaan mana saja yang paling sering terindikasi."'
            rasio_label = "Rasio Insiden (Provinsi)"
        else:
            advice_text = f'"{penjelasan_dasar} Pabrik ini terdeteksi memiliki {insiden} indikasi bulan penimbunan. Lakukan sidak ke pabrik ini!"'
            rasio_label = "Rasio Insiden (Pabrik)"
    else:
        advice_text = f'"Aman! Tidak ada indikasi penimbunan. Pergerakan volume jual terlihat wajar mengikuti fluktuasi harga pasar."'
        rasio_label = "Rasio Insiden (Provinsi)" if pid == "ALL" else "Rasio Insiden (Pabrik)"

    return dbc.Row([
        dbc.Col(_kpi(str(insiden), "Total Insiden", "/assets/mascot_1.webp", "danger"), lg=2, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(pct, rasio_label, "/assets/mascot_2.webp", "warning"), lg=2, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"Rp {k['avg_harga']:,.0f}/kg", "Rata-rata Harga Pasar", "/assets/mascot_1.webp", ""), lg=3, md=4, sm=12, className="mb-2"),
        dbc.Col(html.Div([
            html.Div(className="advice-bg-overlay"),
            html.Div([
                html.Div("Penjelasan Indikasi", className="advice-title", style={"fontSize": "1.1rem"}),
                html.Div(advice_text, className="advice-text", style={"fontSize": "1rem", "lineHeight": "1.4"}),
                html.Div(html.Img(src="/assets/mascot_2.webp", className="advice-mascot", style={"width": "110px", "bottom": "-10px"}), className="advice-mascot-container")
            ], className="advice-content")
        ], className="mascot-advice-card h-100"), lg=5, md=12, sm=12, className="mb-2"),
    ], className="g-3 align-items-stretch")

@callback(
    [Output("timbun-chart-dual", "figure"), Output("timbun-chart-dual-title", "children")],
    Input("timbun-filter-pks", "value")
)
def update_dual(pid):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    if pid == "ALL":
        df = _chart_data_agregat()
        if df.empty:
            return go.Figure(), "Tren Penimbunan Agregat"
            
        fig.add_trace(
            go.Bar(x=df["periode"], y=df["jumlah_insiden"], name="Jumlah Pabrik Terindikasi", marker_color="#E07A5F"),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=df["periode"], y=df["harga_cpo"], name="Rata-rata Harga CPO", mode="lines+markers", line=dict(color="#3D405B", width=3)),
            secondary_y=True
        )
        
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            paper_bgcolor="white", plot_bgcolor="white", font_family="Inter",
            hovermode="x unified"
        )
        fig.update_yaxes(title_text="Jumlah Pabrik", secondary_y=False)
        fig.update_yaxes(title_text="Harga CPO (Rp)", secondary_y=True, showgrid=False)
        return fig, "Tren Insiden Penimbunan se-Provinsi Riau"
        
    else:
        df = _chart_data_pks(pid)
        if df.empty:
            return go.Figure(), "Tren Penjualan & Stok"
            
        fig.add_trace(
            go.Bar(x=df["periode"], y=df["vol_jual"], name="Volume Penjualan (Ton)", marker_color="#81B29A"),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=df["periode"], y=df["stok"], name="Stok Akhir (Ton)", fill='tozeroy', fillcolor="rgba(242, 204, 143, 0.3)", mode="lines+markers", line=dict(color="#F2CC8F", width=2)),
            secondary_y=False
        )
        fig.add_trace(
            go.Scatter(x=df["periode"], y=df["harga_cpo"], name="Harga CPO (Rp/kg)", mode="lines+markers", line=dict(color="#3D405B", width=3)),
            secondary_y=True
        )
        
        timbun_df = df[df["ada_timbun"] == True]
        if not timbun_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=timbun_df["periode"], y=timbun_df["vol_jual"], mode="markers",
                    name="★ Terindikasi Penimbunan", marker=dict(color="#E07A5F", size=12, symbol="star"),
                    hoverinfo="skip"
                ),
                secondary_y=False
            )
            
        fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            paper_bgcolor="white", plot_bgcolor="white", font_family="Inter",
            hovermode="x unified"
        )
        fig.update_yaxes(title_text="Volume / Stok (Ton)", secondary_y=False)
        fig.update_yaxes(title_text="Harga CPO (Rp)", secondary_y=True, showgrid=False)
        return fig, "★ Analisis Volume, Stok, dan Harga CPO"

@callback(Output("timbun-chart-bar", "figure"), Input("timbun-filter-pks", "value"))
def update_bar(pid):
    if pid != "ALL":
        return go.Figure() # Tidak dirender jika bukan ALL
    df = _bar_insiden(pid)
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
        margin=dict(l=0, r=0, t=10, b=0),
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        yaxis=dict(autorange="reversed")
    )
    return fig

@callback(Output("timbun-chart-map", "figure"), Input("timbun-filter-pks", "value"))
def update_map(pid):
    df = _map_timbun_data(pid)
    geojson = get_riau_geojson()
    
    if df.empty:
        fig = go.Figure()
        fig.update_layout(template="plotly_white", title="Tidak ada insiden tercatat", margin=dict(l=0, r=0, t=30, b=0))
        return fig
    df["kode_wilayah"] = df["kode_wilayah"].astype(str)
    
    fig = px.choropleth_mapbox(
        df, geojson=geojson, color="total_insiden",
        locations="kode_wilayah", featureidkey="id",
        hover_name="nama_kabupaten",
        color_continuous_scale="Reds",
        mapbox_style="carto-positron", zoom=5.5, center={"lat": 0.5, "lon": 101.5},
        opacity=0.7,
        labels={"total_insiden": "Total Insiden"}
    )
    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0},
        coloraxis_colorbar=dict(title="Insiden")
    )
    return fig

@callback(
    [Output("col-line", "lg"), Output("col-top5", "className")],
    Input("timbun-filter-pks", "value")
)
def adjust_layout(pid):
    if pid == "ALL":
        return 7, "mb-2"
    else:
        return 12, "d-none"


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


def _kpi(value, label, img_src, variant):
    return html.Div([
        html.Img(src=img_src, style={"height": "56px", "objectFit": "contain", "marginBottom": "12px", "opacity": "0.9"}),
        html.Div(label, className="mini-kpi-label", style={"fontSize": "0.95rem", "fontWeight": "600"}),
        html.Div(value, className="mini-kpi-value", style={"fontSize": "1.7rem", "fontWeight": "bold"}),
    ], className=f"mini-kpi-card {variant}", style={"padding": "16px", "textAlign": "center"})

