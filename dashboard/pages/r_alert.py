"""
pages/reporting/alert.py — Alert Operasional (MongoDB)
Multi-chart: KPI | Stacked Bar Bulanan | Multi-Line Tren | Heatmap Intensitas
Sumber data: fact_alert_operasional (hasil agregat dari MongoDB)
"""

import dash
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from db import run_query, get_perusahaan_options

dash.register_page(__name__, path="/reporting/alert", name="Alert Operasional", order=4)


# ── Data Loaders ──────────────────────────────────────────────
def _kpi_alert(pid="ALL"):
    cond = f"WHERE perusahaan_id = '{pid}'" if pid != "ALL" else ""
    df = run_query(f"""
        SELECT
            COALESCE(SUM(total_alert), 0)           AS total,
            COALESCE(SUM(alert_ditangani), 0)        AS ditangani,
            COALESCE(SUM(alert_tidak_ditangani), 0)  AS tidak_ditangani
        FROM fact_alert_operasional {cond}
    """)
    return df.iloc[0]

def _top_perusahaan():
    return run_query("""
        SELECT p.nama_perusahaan, SUM(a.total_alert) AS total
        FROM fact_alert_operasional a
        JOIN dim_perusahaan p ON a.perusahaan_id = p.perusahaan_id
        GROUP BY p.nama_perusahaan ORDER BY total DESC LIMIT 1
    """)

def _stacked_bulanan(pid="ALL"):
    cond = f"AND a.perusahaan_id = '{pid}'" if pid != "ALL" else ""
    return run_query(f"""
        SELECT a.periode,
               SUM(a.alert_ditangani)       AS ditangani,
               SUM(a.alert_tidak_ditangani) AS tidak_ditangani
        FROM fact_alert_operasional a
        WHERE 1=1 {cond}
        GROUP BY a.periode ORDER BY a.periode
    """)

def _jenis_alert_terbanyak(pid="ALL"):
    cond = f"AND perusahaan_id = '{pid}'" if pid != "ALL" else ""
    return run_query(f"""
        SELECT jenis_alert_terbanyak, COUNT(*) AS frekuensi
        FROM fact_alert_operasional
        WHERE jenis_alert_terbanyak IS NOT NULL {cond}
        GROUP BY jenis_alert_terbanyak ORDER BY frekuensi DESC
    """)


# ── Layout ────────────────────────────────────────────────────
def layout():
    return html.Div([
        html.Div([
            html.H2("Alert Operasional"),
            html.P("Monitoring alert harian per perusahaan — agregasi bulanan dari sistem logging MongoDB"),
        ], className="page-header"),

        # Filter
        html.Div([
            html.Label("Perusahaan:"),
            dcc.Dropdown(
                id="alert-filter-pks",
                options=get_perusahaan_options(),
                value="ALL", clearable=False,
                style={"minWidth": "280px"},
            ),
        ], className="filter-bar"),

        # KPI Cards
        html.Div(id="alert-kpi", className="mb-3"),

        # Section 1: Stacked Bar + Jenis Alert
        html.P("Tren Alert Bulanan", style={"fontWeight": "600", "color": "var(--primary-dark)", "marginBottom": "12px"}),
        dbc.Row([
            dbc.Col(html.Div([
                html.Div("Stacked Bar: Alert Ditangani vs Tidak Ditangani", className="chart-card-title"),
                dcc.Graph(id="alert-chart-stacked", config={"displayModeBar": False}),
            ], className="chart-card"), lg=8),
            dbc.Col(html.Div([
                html.Div("Jenis Alert Terbanyak", className="chart-card-title"),
                dcc.Graph(id="alert-chart-jenis", config={"displayModeBar": False}),
            ], className="chart-card"), lg=4),
        ], className="g-3 mb-2"),
    ])


# ── Callbacks ─────────────────────────────────────────────────
@callback(Output("alert-kpi", "children"), Input("alert-filter-pks", "value"))
def update_kpi(pid):
    k = _kpi_alert(pid)
    total = int(k["total"]) or 1
    pct = f"{100 * int(k['ditangani']) / total:.1f}%"
    top = _top_perusahaan()
    top_name = top["nama_perusahaan"].iloc[0] if not top.empty else "–"
    return dbc.Row([
        dbc.Col(_kpi(str(int(k["total"])),         "Total Alert",           "fa-solid fa-bell", ""),        lg=3, md=6, sm=12, className="mb-2"),
        dbc.Col(_kpi(str(int(k["ditangani"])),      "Alert Ditangani",       "fa-solid fa-check", ""),        lg=3, md=6, sm=12, className="mb-2"),
        dbc.Col(_kpi(str(int(k["tidak_ditangani"])),"Alert Tidak Ditangani", "fa-solid fa-triangle-exclamation", "danger"),  lg=3, md=6, sm=12, className="mb-2"),
        dbc.Col(_kpi(pct,                           "Tingkat Penanganan",    "fa-solid fa-chart-simple", "info"),    lg=3, md=6, sm=12, className="mb-2"),
    ], className="g-3")


@callback(Output("alert-chart-stacked", "figure"), Input("alert-filter-pks", "value"))
def update_stacked(pid):
    df = _stacked_bulanan(pid)
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Bar(name="Ditangani",       x=df["periode"], y=df["ditangani"],       marker_color="#52B788"))
    fig.add_trace(go.Bar(name="Tidak Ditangani", x=df["periode"], y=df["tidak_ditangani"], marker_color="#D62839"))
    fig.update_layout(
        barmode="group", template="plotly_white", height=300,
        margin=dict(l=0, r=0, t=10, b=60), font_family="Inter",
        paper_bgcolor="white", plot_bgcolor="white",
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return fig


@callback(Output("alert-chart-jenis", "figure"), Input("alert-filter-pks", "value"))
def update_jenis(pid):
    df = _jenis_alert_terbanyak(pid)
    if df.empty:
        return go.Figure()
    fig = px.bar(
        df, x="frekuensi", y="jenis_alert_terbanyak", orientation="h",
        color_discrete_sequence=["#F4A261"],
        labels={"frekuensi": "Frekuensi", "jenis_alert_terbanyak": ""},
        template="plotly_white",
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0), height=300,
        font_family="Inter", paper_bgcolor="white", plot_bgcolor="white",
    )
    return fig

def _kpi(value, label, icon_class, variant):
    return html.Div([
        html.I(className=f"{icon_class} mini-kpi-icon", style={"marginBottom": "8px"}),
        html.Div(label, className="mini-kpi-label"),
        html.Div(value, className="mini-kpi-value"),
    ], className=f"mini-kpi-card {variant}")
