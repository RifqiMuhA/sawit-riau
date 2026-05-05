"""
pages/reporting/sdm.py — SDM & Tenaga Kerja
Multi-chart: KPI | Komposisi Jabatan | Aktif/Non-Aktif | Data Table
Sumber data: dim_karyawan + fact_tenaga_kerja
"""

import dash
from dash import html, dcc, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_perusahaan_options

dash.register_page(__name__, path="/reporting/sdm", name="SDM & Karyawan", order=2)

GREEN_PALETTE = ["#2D6A4F", "#40916C", "#52B788", "#74C69D", "#95D5B2", "#B7E4C7"]


# ── Data Loaders ──────────────────────────────────────────────
def _kpi_sdm(perusahaan_id="ALL"):
    w = "" if perusahaan_id == "ALL" else f"WHERE perusahaan_id = '{perusahaan_id}'"
    df = run_query(f"""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE status = 'aktif') AS aktif,
               COUNT(*) FILTER (WHERE status = 'non-aktif') AS non_aktif
        FROM dim_karyawan {w}
    """)
    return df.iloc[0]

def _jabatan_dist(perusahaan_id="ALL"):
    w = "" if perusahaan_id == "ALL" else f"WHERE perusahaan_id = '{perusahaan_id}'"
    return run_query(f"""
        SELECT jabatan, COUNT(*) AS jumlah FROM dim_karyawan {w}
        GROUP BY jabatan ORDER BY jumlah DESC
    """)

def _aktif_per_perusahaan():
    return run_query("""
        SELECT p.nama_perusahaan,
               SUM(CASE WHEN k.status = 'aktif' THEN 1 ELSE 0 END) AS aktif,
               SUM(CASE WHEN k.status != 'aktif' THEN 1 ELSE 0 END) AS non_aktif
        FROM dim_karyawan k
        JOIN dim_perusahaan p ON k.perusahaan_id = p.perusahaan_id
        GROUP BY p.nama_perusahaan ORDER BY aktif DESC
    """)

def _tabel_karyawan(perusahaan_id="ALL"):
    w = "" if perusahaan_id == "ALL" else f"WHERE k.perusahaan_id = '{perusahaan_id}'"
    return run_query(f"""
        SELECT k.karyawan_id AS "ID", k.nama AS "Nama",
               k.jabatan AS "Jabatan", p.nama_perusahaan AS "Perusahaan",
               k.status AS "Status"
        FROM dim_karyawan k
        JOIN dim_perusahaan p ON k.perusahaan_id = p.perusahaan_id
        {w}
        ORDER BY p.nama_perusahaan, k.jabatan
    """)


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("SDM & Tenaga Kerja"),
            html.P("Komposisi karyawan, distribusi jabatan, dan status aktif per perusahaan PKS Riau"),
        ], className="page-header"),

        # Filter
        html.Div([
            html.Label("Filter Perusahaan:"),
            dcc.Dropdown(
                id="sdm-filter-perusahaan",
                options=get_perusahaan_options(),
                value="ALL",
                clearable=False,
                style={"minWidth": "280px"},
            ),
        ], className="filter-bar"),

        # KPI Cards
        html.Div(id="sdm-kpi-row", className="mb-3"),

        # Section 1: Komposisi
        html.P("Komposisi & Distribusi", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Stacked Bar: Aktif vs Non-Aktif per Perusahaan", className="chart-card-title"),
                dcc.Graph(id="sdm-chart-stacked", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Distribusi Jabatan", className="chart-card-title"),
                dcc.Graph(id="sdm-chart-jabatan", config={"displayModeBar": False}),
            ], className="chart-card"), lg=4),
        ], className="g-3 mb-2"),

        # Section 2: Tabel Detail
        html.P("Data Karyawan Detail", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Daftar Karyawan", className="chart-card-title"),
            html.Div(id="sdm-tabel"),
        ], className="chart-card"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("sdm-kpi-row", "children"), Input("sdm-filter-perusahaan", "value"))
def update_kpi(pid):
    k = _kpi_sdm(pid)
    pct_aktif = f"{100 * k['aktif'] / max(k['total'], 1):.1f}%"
    return dbc.Row([
        dbc.Col(_kpi(str(k["total"]),    "Total Karyawan",    "fa-solid fa-users", ""),       lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(str(k["aktif"]),    "Karyawan Aktif",    "fa-solid fa-check", ""),       lg=4, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(pct_aktif,          "Tingkat Keaktifan", "fa-solid fa-chart-simple", "info"),   lg=4, md=4, sm=12, className="mb-2"),
    ], className="g-3")


@callback(Output("sdm-chart-stacked", "figure"), Input("sdm-filter-perusahaan", "value"))
def update_stacked(_):
    df = _aktif_per_perusahaan()
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Aktif",     x=df["nama_perusahaan"], y=df["aktif"],     marker_color="#52B788"))
    fig.add_trace(go.Bar(name="Non-Aktif", x=df["nama_perusahaan"], y=df["non_aktif"], marker_color="#D62839"))
    fig.update_layout(
        barmode="stack", template="plotly_white", height=300,
        margin=dict(l=0, r=0, t=10, b=60), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


@callback(Output("sdm-chart-jabatan", "figure"), Input("sdm-filter-perusahaan", "value"))
def update_jabatan(pid):
    df = _jabatan_dist(pid)
    fig = px.bar(
        df, x="jumlah", y="jabatan", orientation="h",
        color_discrete_sequence=GREEN_PALETTE,
        labels={"jumlah": "Jumlah", "jabatan": ""},
        template="plotly_white",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


@callback(Output("sdm-tabel", "children"), Input("sdm-filter-perusahaan", "value"))
def update_tabel(pid):
    df = _tabel_karyawan(pid)
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#2D6A4F", "color": "white", "fontWeight": "600", "fontSize": "12px"},
        style_cell={"fontSize": "12px", "padding": "8px 12px", "border": "1px solid #D8EAE2"},
        style_data_conditional=[
            {"if": {"filter_query": "{Status} = 'aktif'", "column_id": "Status"},
             "color": "#2D6A4F", "fontWeight": "600"},
            {"if": {"filter_query": "{Status} != 'aktif'", "column_id": "Status"},
             "color": "#D62839"},
            {"if": {"row_index": "odd"}, "backgroundColor": "#F4F7F5"},
        ],
        filter_action="native",
        sort_action="native",
    )


def _kpi(value, label, icon_class, variant):
    return html.Div([
        html.I(className=f"{icon_class} mini-kpi-icon", style={"marginBottom": "8px"}),
        html.Div(label, className="mini-kpi-label"),
        html.Div(value, className="mini-kpi-value"),
    ], className=f"mini-kpi-card {variant}")
