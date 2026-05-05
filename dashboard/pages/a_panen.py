"""
pages/reporting/panen.py — Realisasi vs Target Panen
Multi-chart: KPI | Grouped Bar | Scatter Gap% | Status Donut | Tabel
Sumber data: fact_panen + dim_kebun + dim_perusahaan (DWH langsung)
"""

import dash
from dash import html, dcc, Input, Output, callback, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_perusahaan_options, get_periode_options

dash.register_page(__name__, path="/analytics/panen", name="Realisasi Panen", order=3)

STATUS_COLOR = {"selesai": "#52B788", "tertunda": "#F4A261", "batal": "#D62839"}


# ── Data Loaders ──────────────────────────────────────────────
def _kpi_panen(pid="ALL", periode=None):
    cond = []
    if pid != "ALL":
        cond.append(f"f.perusahaan_id = '{pid}'")
    if periode:
        cond.append(f"f.periode = '{periode}'")
    where = ("WHERE " + " AND ".join(cond)) if cond else ""

    df = run_query(f"""
        SELECT
            COALESCE(SUM(f.target_panen_ton), 0)      AS total_target,
            COALESCE(SUM(f.realisasi_panen_ton), 0)   AS total_realisasi,
            ROUND(AVG(f.gap_persen), 1)               AS avg_gap,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.status_id = 'selesai')
                  / NULLIF(COUNT(*), 0), 1)            AS pct_selesai
        FROM fact_panen f {where}
    """)
    return df.iloc[0]


def _grouped_bar(pid="ALL", periode=None):
    cond = []
    if pid != "ALL":
        cond.append(f"f.perusahaan_id = '{pid}'")
    if periode:
        cond.append(f"f.periode = '{periode}'")
    where = ("WHERE " + " AND ".join(cond)) if cond else ""
    return run_query(f"""
        SELECT p.nama_perusahaan,
               SUM(f.target_panen_ton)     AS target,
               SUM(f.realisasi_panen_ton)  AS realisasi
        FROM fact_panen f
        JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
        {where}
        GROUP BY p.nama_perusahaan ORDER BY target DESC
    """)


def _scatter_gap(pid="ALL"):
    cond = f"WHERE f.perusahaan_id = '{pid}'" if pid != "ALL" else ""
    return run_query(f"""
        SELECT k.nama_kebun, f.periode, f.gap_persen, f.status_id
        FROM fact_panen f
        JOIN dim_kebun k ON f.kebun_id = k.kebun_id
        {cond}
        ORDER BY f.periode
    """)


def _status_dist(pid="ALL"):
    cond = f"WHERE perusahaan_id = '{pid}'" if pid != "ALL" else ""
    return run_query(f"""
        SELECT status_id, COUNT(*) AS jumlah
        FROM fact_panen {cond}
        GROUP BY status_id
    """)


def _tabel_panen(pid="ALL", periode=None):
    cond = []
    if pid != "ALL":
        cond.append(f"f.perusahaan_id = '{pid}'")
    if periode:
        cond.append(f"f.periode = '{periode}'")
    where = ("WHERE " + " AND ".join(cond)) if cond else ""
    return run_query(f"""
        SELECT p.nama_perusahaan AS "Perusahaan",
               k.nama_kebun      AS "Kebun",
               f.periode         AS "Periode",
               f.target_panen_ton   AS "Target (ton)",
               f.realisasi_panen_ton AS "Realisasi (ton)",
               f.gap_persen      AS "Gap (%)",
               f.status_id       AS "Status"
        FROM fact_panen f
        JOIN dim_perusahaan p ON f.perusahaan_id = p.perusahaan_id
        JOIN dim_kebun k ON f.kebun_id = k.kebun_id
        {where}
        ORDER BY f.periode DESC, p.nama_perusahaan
        LIMIT 200
    """)


# ── Layout ────────────────────────────────────────────────────
def layout():
    periode_opts = get_periode_options()
    return html.Div([
        html.Div([
            html.H2("Realisasi vs Target Panen"),
            html.P("Perbandingan target dan realisasi panen TBS per kebun, per perusahaan, dan status pelaksanaan"),
            html.Div(
                "Gap Panen (%) = ((Realisasi - Target) ÷ Target) × 100%",
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
            dcc.Dropdown(id="panen-filter-pks",     options=get_perusahaan_options(),
                         value="ALL", clearable=False, style={"minWidth": "240px"}),
            html.Label("Periode:"),
            dcc.Dropdown(id="panen-filter-periode", options=[{"label": "Semua Periode", "value": "ALL"}] + periode_opts,
                         value="ALL", clearable=False, style={"minWidth": "160px"}),
        ], className="filter-bar"),

        # KPI
        html.Div(id="panen-kpi", className="mb-3"),

        # Section 1: Grouped Bar + Status Donut
        html.P("Target vs Realisasi per Perusahaan", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Grouped Bar: Target vs Realisasi (Ton)", className="chart-card-title"),
                dcc.Graph(id="panen-chart-grouped", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Status Panen", className="chart-card-title"),
                dcc.Graph(id="panen-chart-status", config={"displayModeBar": False}),
            ], className="chart-card"), lg=4),
        ], className="g-3 mb-2"),

        # Section 2: Scatter Gap
        html.P("Distribusi Gap Panen per Kebun", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Scatter: Gap% per Kebun (Hijau = Over-Target, Merah = Under-Target)", className="chart-card-title"),
            dcc.Graph(id="panen-chart-scatter", config={"displayModeBar": False}),
        ], className="chart-card mb-2"),

        # Section 3: Tabel
        html.P("Data Detail Panen", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        html.Div([
            html.Div("Tabel Realisasi Panen", className="chart-card-title"),
            html.Div(id="panen-tabel"),
        ], className="chart-card"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("panen-kpi", "children"),
          Input("panen-filter-pks", "value"),
          Input("panen-filter-periode", "value"))
def update_kpi(pid, periode):
    p = None if periode == "ALL" else periode
    k = _kpi_panen(pid, p)
    
    gap = k['avg_gap'] or 0
    if gap >= 0:
        advice_text = f'"Secara keseluruhan, realisasi panen {gap:.1f}% di atas target. Kinerja panen sangat memuaskan, pertahankan!"'
    else:
        advice_text = f'"Rata-rata panen {abs(gap):.1f}% di bawah target. Segera evaluasi kebun yang berada di zona merah (Under-Target) pada grafik sebaran di bawah."'

    return dbc.Row([
        dbc.Col(_kpi(f"{k['total_target']:,.0f} ton", "Total Target", "fa-solid fa-bullseye", ""), lg=3, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"{k['total_realisasi']:,.0f} ton", "Total Realisasi", "fa-solid fa-seedling", "success" if gap >= 0 else "danger"), lg=3, md=4, sm=12, className="mb-2"),
        dbc.Col(_kpi(f"{gap:.1f}%", "Rata-rata Gap", "fa-solid fa-arrow-trend-up" if gap >= 0 else "fa-solid fa-arrow-trend-down", "success" if gap >= 0 else "danger"), lg=2, md=4, sm=12, className="mb-2"),
        dbc.Col(html.Div([
            html.Div(className="advice-bg-overlay"),
            html.Div([
                html.Div("Saran Decision", className="advice-title"),
                html.Div(advice_text, className="advice-text"),
                html.Div(html.Img(src="/assets/mascot_1.webp", className="advice-mascot"), className="advice-mascot-container")
            ], className="advice-content")
        ], className="mascot-advice-card h-100"), lg=4, md=12, sm=12, className="mb-2"),
    ], className="g-3 align-items-stretch")


@callback(Output("panen-chart-grouped", "figure"),
          Input("panen-filter-pks", "value"),
          Input("panen-filter-periode", "value"))
def update_grouped(pid, periode):
    p = None if periode == "ALL" else periode
    df = _grouped_bar(pid, p)
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Target",    x=df["nama_perusahaan"], y=df["target"],    marker_color="#B7E4C7"))
    fig.add_trace(go.Bar(name="Realisasi", x=df["nama_perusahaan"], y=df["realisasi"], marker_color="#2D6A4F"))
    fig.update_layout(
        barmode="group", template="plotly_white", height=300,
        margin=dict(l=0, r=0, t=10, b=60), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig


@callback(Output("panen-chart-status", "figure"),
          Input("panen-filter-pks", "value"),
          Input("panen-filter-periode", "value"))
def update_status(pid, _):
    df = _status_dist(pid)
    if df.empty:
        return go.Figure()
    fig = px.pie(
        df, values="jumlah", names="status_id",
        color="status_id", color_discrete_map=STATUS_COLOR,
        hole=0.55, template="plotly_white",
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(
        showlegend=False, height=300,
        margin=dict(l=0, r=0, t=10, b=0),
        font_family="Inter", paper_bgcolor="white",
    )
    return fig


@callback(Output("panen-chart-scatter", "figure"),
          Input("panen-filter-pks", "value"),
          Input("panen-filter-periode", "value"))
def update_scatter(pid, _):
    df = _scatter_gap(pid)
    if df.empty:
        return go.Figure()
    df["warna"] = df["gap_persen"].apply(lambda x: "#52B788" if (x or 0) >= 0 else "#D62839")
    fig = px.scatter(
        df, x="periode", y="gap_persen",
        color="status_id", color_discrete_map=STATUS_COLOR,
        hover_data=["nama_kebun"],
        labels={"gap_persen": "Gap (%)", "periode": "Periode"},
        template="plotly_white",
    )
    fig.add_hline(y=0, line_dash="dash", line_color="#6B8F7E", line_width=1.5)
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=280,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig


@callback(Output("panen-tabel", "children"),
          Input("panen-filter-pks", "value"),
          Input("panen-filter-periode", "value"))
def update_tabel(pid, periode):
    p = None if periode == "ALL" else periode
    df = _tabel_panen(pid, p)
    return dash_table.DataTable(
        data=df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in df.columns],
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#2D6A4F", "color": "white", "fontWeight": "600", "fontSize": "12px"},
        style_cell={"fontSize": "12px", "padding": "8px 12px", "border": "1px solid #D8EAE2"},
        style_data_conditional=[
            {"if": {"filter_query": "{Gap (%)} >= 0", "column_id": "Gap (%)"}, "color": "#2D6A4F", "fontWeight": "600"},
            {"if": {"filter_query": "{Gap (%)} < 0",  "column_id": "Gap (%)"}, "color": "#D62839", "fontWeight": "600"},
            {"if": {"row_index": "odd"}, "backgroundColor": "#F4F7F5"},
        ],
        filter_action="native",
        sort_action="native",
    )


def _kpi(value, label, icon_class, variant):
    return html.Div([
        html.I(className=f"{icon_class} mini-kpi-icon", style={"marginBottom": "12px", "fontSize": "40px"}),
        html.Div(label, className="mini-kpi-label", style={"fontSize": "16px", "fontWeight": "600"}),
        html.Div(value, className="mini-kpi-value", style={"fontSize": "32px", "fontWeight": "800", "marginTop": "4px"}),
    ], className=f"mini-kpi-card {variant}", style={"padding": "24px", "height": "100%", "display": "flex", "flexDirection": "column", "justifyContent": "center"})
