"""
pages/analytics/kondisi_kebun.py — Kondisi Kebun (NDVI)
Minimalist Redesign: 
Top: Full Map with Legend
Bottom: Tren NDVI (with filter) + Mascot Advice Card
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
def _ndvi_agregat():
    """Rata-rata NDVI dan status dominan per kabupaten untuk periode terakhir (peta)."""
    return run_query("""
        SELECT k.kode_wilayah, k.nama_kabupaten,
               ROUND(AVG(d.ndvi_mean)::numeric, 4) AS ndvi_mean,
               MODE() WITHIN GROUP (ORDER BY d.status_kebun) AS status_dominan
        FROM datamart.dm_kondisi_kebun d
        JOIN dim_kabupaten k ON d.nama_kabupaten = k.nama_kabupaten
        WHERE d.periode = (SELECT MAX(periode) FROM datamart.dm_kondisi_kebun)
        GROUP BY k.kode_wilayah, k.nama_kabupaten
    """)

def _ndvi_tren(kode_wilayah=None):
    cond = f"AND k.kode_wilayah = '{kode_wilayah}'" if kode_wilayah else ""
    return run_query(f"""
        SELECT d.nama_kabupaten, d.periode,
               ROUND(AVG(d.ndvi_mean)::numeric, 4) AS ndvi_mean
        FROM datamart.dm_kondisi_kebun d
        JOIN dim_kabupaten k ON d.nama_kabupaten = k.nama_kabupaten
        WHERE 1=1 {cond}
        GROUP BY d.nama_kabupaten, d.periode ORDER BY d.periode
    """)

# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("Kondisi Kebun (Analisis NDVI)"),
            html.P("Nilai NDVI berkisar di antara -1 hingga 1, di mana nilai yang lebih tinggi menunjukkan vegetasi yang lebih sehat dan lebat."),
            html.Div(
                "Status Kebun = Ranking Persentil NDVI Bulanan",
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

        # Section 1: Peta Choropleth (Lebar Penuh)
        html.Div([
            html.Div("Peta Spasial Kondisi Lahan (Periode Terkini)", className="chart-card-title"),
            dcc.Graph(id="ndvi-map", config={"displayModeBar": False, "scrollZoom": True}, style={"height": "480px"}),
        ], className="chart-card mb-4"),

        # Section 2: Tren & Mascot Advice
        dbc.Row([
            # Kiri: Tren NDVI
            dbc.Col(html.Div([
                html.Div([
                    html.Div("Tren NDVI Bulanan", className="chart-card-title", style={"margin": "0"}),
                    dcc.Dropdown(
                        id="ndvi-filter-kab",
                        options=get_kabupaten_options(),
                        value="ALL", clearable=False,
                        style={"minWidth": "200px"},
                    )
                ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center", "marginBottom": "16px"}),
                dcc.Graph(id="ndvi-chart-tren", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            
            # Kanan: Mascot Advice
            dbc.Col(html.Div([
                html.Div(className="advice-bg-overlay"),
                html.Div([
                    html.Div("Saran Decision", className="advice-title"),
                    html.Div(id="ndvi-mascot-advice-content", className="advice-text"),
                    html.Div(html.Img(src="/assets/mascot_1.webp", className="advice-mascot"), className="advice-mascot-container")
                ], className="advice-content")
            ], className="mascot-advice-card"), lg=4),
        ], className="g-4 align-items-stretch"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("ndvi-map", "figure"), Input("url", "pathname"))
def update_map(_):
    geojson = get_riau_geojson()
    df = _ndvi_agregat()
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
        hover_data={"ndvi_mean": True, "status_dominan": True, "nama_kabupaten": True, "kode_wilayah": False, "status_score": False},
        mapbox_style="carto-positron",
        center={"lat": 0.5, "lon": 102.0},
        zoom=6.5,
        opacity=0.8,
    )
    
    # Custom colorscale tick labels (Percentile rankings)
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Ranking Persentil",
            tickvals=[1.33, 2, 2.66],
            ticktext=["Rendah (Bottom 33%)", "Sedang (Mid 33%)", "Tinggi (Top 33%)"],
            lenmode="pixels", len=200,
            thickness=15
        )
    )
    
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="white",
        font_family="Inter",
    )
    return fig


@callback(Output("ndvi-chart-tren", "figure"),
          Input("ndvi-filter-kab", "value"))
def update_tren(kab):
    if kab == "ALL":
        df = _ndvi_tren(None)
        if not df.empty:
            df = df.groupby('periode', as_index=False)['ndvi_mean'].mean()
            df['nama_kabupaten'] = 'Rata-rata Seluruh Kabupaten'
    else:
        df = _ndvi_tren(kab)
        
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
        margin=dict(l=0, r=0, t=10, b=20), height=320,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30, tickfont=dict(size=9)),
        legend=dict(font=dict(size=9), orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


@callback(Output("ndvi-mascot-advice-content", "children"),
          Input("ndvi-filter-kab", "value"))
def update_advice(kab):
    df_agregat = _ndvi_agregat()
    if df_agregat.empty:
        return '"Belum ada data analitik yang tersedia."'
        
    if kab == "ALL":
        rendah_kab = df_agregat[df_agregat['status_dominan'] == 'kritis']['nama_kabupaten'].tolist()
        if rendah_kab:
            kab_list = ", ".join(rendah_kab)
            return f'"Lahan di {kab_list} berada di kelompok 33% terbawah se-Riau. Perlu alokasi anggaran replanting dan intervensi pemupukan segera di wilayah tersebut!"'
        else:
            return '"Sistem peringatan dini mendeteksi performa stabil secara merata di seluruh kabupaten."'
    else:
        df_tren = _ndvi_tren(kab)
        if df_tren.empty:
            return '"Tidak ada data untuk wilayah ini."'
        
        nama_kab = df_tren["nama_kabupaten"].iloc[0]
        
        # Ambil status dominan dari data agregat
        kab_row = df_agregat[df_agregat['nama_kabupaten'] == nama_kab]
        status = kab_row['status_dominan'].iloc[0] if not kab_row.empty else "normal"
        
        if status == 'kritis':
            return f'"Lahan sawit di {nama_kab} masuk di kelompok 33% terbawah. Segera agendakan peninjauan lapangan dan evaluasi program replanting."'
        elif status == 'menurun':
            return f'"Kondisi kebun di {nama_kab} berada di tingkat menengah (Mid 33%). Perketat pengawasan pemupukan dan tata kelola air!"'
        else:
            return f'"Keren! Vegetasi sawit di {nama_kab} masuk di kelompok 33% teratas. Pertahankan manajemen perawatan saat ini."'
