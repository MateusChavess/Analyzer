# pages/Analise-aldeia.py
import os
from pathlib import Path
import textwrap
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from streamlit_echarts import st_echarts

# ========= Config =========
st.set_page_config(page_title="Analyzer — Análise de Membros (Aldeia)", layout="wide")

PROJECT_ID   = "leads-ts"
MEMBROS_FQN  = "`leads-ts.Analyzer.aldeia_2026_s`"
METAS_FQN    = "`leads-ts.Analyzer.metas_aldeia_forecast_finalizados`"
TZ           = "America/Sao_Paulo"

# ========= Paleta desta página =========
ACCENT_GREEN = "#C9E34F"
YELLOW_LINE  = "#FACC15"

# ========= CSS externo =========
def load_css(*files: str):
    for f in files:
        p = Path("styles") / f
        if p.exists():
            css = p.read_text(encoding="utf-8")
            st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

load_css("base.css", "sidebar.css", "dashboards.css")

# ========= SIDEBAR =========
st.sidebar.markdown('<div class="sb-wrap">', unsafe_allow_html=True)

if "refresh_key" not in st.session_state:
    st.session_state.refresh_key = 0

if st.sidebar.button("🔄 Atualizar agora", use_container_width=True, key="btn_refresh_now_analise_aldeia"):
    st.session_state.refresh_key += 1
    st.rerun()

st.sidebar.divider()
st.sidebar.markdown('<div class="sb-box">', unsafe_allow_html=True)

if st.sidebar.button("🏠 Home", use_container_width=True, key="nav_home_btn_analise_aldeia"):
    st.switch_page("pages/analyzer-aldeia.py")

st.sidebar.button(
    "📈 Análise de Membros",
    use_container_width=True,
    key="nav_membros_btn_analise_aldeia_disabled",
    disabled=True
)

if st.sidebar.button("⛔ Atrasados", use_container_width=True, key="nav_atrasados_btn_analise_aldeia"):
    st.switch_page("pages/Atrasados-aldeia.py")

if st.sidebar.button("🎟️ Presenciais", use_container_width=True, key="nav_presenciais_btn_analise_aldeia"):
    st.switch_page("pages/Presenciais-aldeia.py")

st.sidebar.markdown('</div>', unsafe_allow_html=True)
st.sidebar.divider()
st.sidebar.markdown('<div class="sb-grow"></div>', unsafe_allow_html=True)

auth_mode = "secrets" if "gcp_service_account" in st.secrets else (
    "arquivo local" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") else "desconhecido"
)
st.sidebar.markdown('<div class="sb-footer">', unsafe_allow_html=True)
_sb_auth_placeholder = st.sidebar.empty()
_sb_last_placeholder = st.sidebar.empty()
_sb_auth_placeholder.caption(f"🔐 Modo de autenticação: {auth_mode}")
st.sidebar.markdown('</div>', unsafe_allow_html=True)
st.sidebar.markdown('</div>', unsafe_allow_html=True)

# ========= TOGGLES =========
st.session_state.setdefault("tit_choice", None)
st.session_state.setdefault("tog_pagante",   st.session_state["tit_choice"] == "Pagante")
st.session_state.setdefault("tog_adicional", st.session_state["tit_choice"] == "Adicional")

def _on_toggle_pagante():
    if st.session_state["tog_pagante"]:
        st.session_state["tit_choice"] = "Pagante"
        st.session_state["tog_adicional"] = False
    else:
        if st.session_state.get("tit_choice") == "Pagante":
            st.session_state["tit_choice"] = None

def _on_toggle_adicional():
    if st.session_state["tog_adicional"]:
        st.session_state["tit_choice"] = "Adicional"
        st.session_state["tog_pagante"] = False
    else:
        if st.session_state.get("tit_choice") == "Adicional":
            st.session_state["tit_choice"] = None

# ===== Header =====
hdr_l, hdr_r = st.columns([8, 4], gap="medium")

with hdr_l:
    st.title("📈 Análise de Membros (ALDEIA)")

with hdr_r:
    if st.button("↩️ Modo Tribo", use_container_width=True, key="btn_modo_tribo_hdr_analise_aldeia"):
        st.switch_page("pages/Analise.py")

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    t1, t2 = st.columns([1, 1], gap="small")
    with t1:
        st.toggle("Pagante", key="tog_pagante", on_change=_on_toggle_pagante)
    with t2:
        st.toggle("Adicional", key="tog_adicional", on_change=_on_toggle_adicional)

st.divider()

# ========= BigQuery =========
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            return bigquery.Client(project=st.secrets.get("gcp_project_id", PROJECT_ID), credentials=creds)
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error("Falha ao criar o client do BigQuery.")
        st.exception(e)
        st.stop()

@st.cache_data(show_spinner=False)
def run_query(sql: str):
    client = get_bq_client()
    job = client.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=False)

cache_bust = f"{st.session_state.refresh_key}"

# ========= Dados (Aldeia) =========
sql_membros = f"""
SELECT
  id, email, titularidade,
  data_primeiro_contato, finalizacao_primeira, finalizado_final,
  target_sup, status_atraso, late_sup_atraso,
  updated_at, ingestion_time
FROM {MEMBROS_FQN}
ORDER BY ingestion_time DESC
-- cache_bust:{cache_bust}
"""

with st.spinner("Consultando BigQuery…"):
    df = run_query(sql_membros)

if df.empty:
    st.info("Sem registros na tabela.")
    st.stop()

last_updated_str = pd.Timestamp.now(tz=TZ).strftime('%d/%m/%Y %H:%M:%S')
_sb_last_placeholder.caption(f"🕒 Última atualização: {last_updated_str}")

# ========= Helpers =========
def classifica_tit(s: str) -> str:
    if not isinstance(s, str):
        return "Pagante"
    s_low = s.strip().lower()
    if "adicional" in s_low:
        return "Adicional"
    if ("benef" in s_low) or ("titular" in s_low):
        return "Pagante"
    return "Pagante"

def is_filled(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s_low = s.str.lower()
    empty = s_low.isin(["", "nan", "none", "null", "nat"])
    return ~empty

def parse_bq_date(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    s = s.replace({
        "": pd.NA,
        "nan": pd.NA,
        "None": pd.NA,
        "none": pd.NA,
        "null": pd.NA,
        "NaT": pd.NA,
        "<NA>": pd.NA
    })
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def fmt_int(n) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except:
        return "0"

def fmt_pct(n: int, den: int):
    if not den or den <= 0:
        return None
    return f"{(100 * float(n) / float(den)):.1f}%"

# ========= Filtro por titularidade =========
df["tipo_titularidade"] = df["titularidade"].apply(classifica_tit)
tit_choice = st.session_state.get("tit_choice")
fdf = df[df["tipo_titularidade"] == tit_choice].copy() if tit_choice in ("Pagante", "Adicional") else df.copy()

# ========= KPIs =========
base_df = fdf.copy()

email_ok = is_filled(base_df["email"])
target_sup_dt = parse_bq_date(base_df["target_sup"])
fin1_dt = parse_bq_date(base_df["finalizacao_primeira"])
fin2_dt = parse_bq_date(base_df["finalizado_final"])

fin1_filled = fin1_dt.notna()
fin2_filled = fin2_dt.notna()

hoje = pd.Timestamp.now(tz=TZ).tz_localize(None).normalize()
cutoff_primeira = target_sup_dt - pd.Timedelta(days=5)

membros_com_equipe = int(email_ok.sum())

finalizados_geral = int((email_ok & fin2_filled).sum())

mask_base_segunda = email_ok & fin1_filled & (~fin2_filled) & target_sup_dt.notna()
pendentes_segunda = int((mask_base_segunda & (target_sup_dt >= hoje)).sum())
atrasados_segunda = int((mask_base_segunda & (target_sup_dt < hoje)).sum())

pct_fin  = fmt_pct(finalizados_geral, membros_com_equipe)
pct_pend = fmt_pct(pendentes_segunda, membros_com_equipe)
pct_atr  = fmt_pct(atrasados_segunda, membros_com_equipe)

# ========= Cards =========
st.markdown("""
<style>
.kpi-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:6px}
.kpi{background:#101828;border:1px solid #1f2937;border-radius:14px;padding:14px 16px;min-height:92px}
.kpi .t{font-size:.9rem;color:#9ca3af;display:flex;align-items:center;gap:8px;margin:0}
.kpi .i{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;
        border-radius:999px;background:rgba(255,255,255,.06);border:1px solid #1f2937}
.kpi .v{font-size:1.8rem;font-weight:800;color:#e5e7eb;margin-top:6px;display:flex;align-items:baseline;gap:8px}
.kpi .p{font-size:.85rem;color:#9ca3af}
@media (max-width: 1100px){ .kpi-row{grid-template-columns:repeat(2,minmax(0,1fr));} }
@media (max-width: 640px){ .kpi-row{grid-template-columns:1fr;} }
</style>
""", unsafe_allow_html=True)

def kpi_card(title: str, value, icon: str = "📈", pct_text: str | None = None) -> str:
    pct_html = f'<span class="p">{pct_text}</span>' if pct_text else ""
    html = f"""
<div class="kpi">
  <p class="t"><span class="i">{icon}</span>{title}</p>
  <div class="v">{fmt_int(value)}{pct_html}</div>
</div>
"""
    return textwrap.dedent(html).strip()

cards_html = f"""
<div class="kpi-row">
{ kpi_card("Membros com Equipe", membros_com_equipe, "👥") }
{ kpi_card("Finalizados (Geral)", finalizados_geral,  "✅", pct_fin) }
{ kpi_card("Pendentes (Geral)",  pendentes_segunda, "⚠️", pct_pend) }
{ kpi_card("Atrasados (Geral)",  atrasados_segunda, "⛔", pct_atr) }
</div>
"""
st.markdown(textwrap.dedent(cards_html).strip(), unsafe_allow_html=True)

st.divider()

# ========= GRÁFICO DE METAS (FORECAST) =========
st.subheader("🎯 Metas (Acumulado) — Forecast")

sql_metas = f"""
SELECT Data, FinalizadoAcumulado, MetaAcumulado
FROM {METAS_FQN}
WHERE Data <= CURRENT_DATE("{TZ}")
ORDER BY Data
-- cache_bust:{cache_bust}
"""
dfm = run_query(sql_metas)

if dfm.empty:
    st.info("Sem dados na tabela de metas.")
else:
    today = pd.Timestamp.now(tz=TZ).normalize().date()
    dfm["Data"] = pd.to_datetime(dfm["Data"], errors="coerce").dt.date
    dfm = dfm[dfm["Data"] <= today].dropna(subset=["Data"]).copy()

    dfm["label"] = pd.to_datetime(dfm["Data"]).dt.strftime("%d/%m")
    labels = dfm["label"].tolist()
    real   = dfm["FinalizadoAcumulado"].fillna(0).astype(int).tolist()
    meta   = dfm["MetaAcumulado"].fillna(0).astype(int).tolist()

    n = len(labels)
    window   = min(20, n if n > 0 else 20)
    start_ix = max(0, n - window)
    end_ix   = max(0, n - 1)

    options = {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "axis"},
        "legend": {
            "data": ["Real acumulado", "Meta acumulada"],
            "top": 0,
            "textStyle": {"color": "#E5E7EB"}
        },
        "grid": {"left": 40, "right": 24, "top": 36, "bottom": 84, "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": labels,
            "axisLabel": {"color": "#E5E7EB", "interval": 0},
            "axisLine": {"lineStyle": {"color": "rgba(255,255,255,0.15)"}}
        },
        "yAxis": {
            "type": "value",
            "axisLabel": {"color": "#A1A1AA"},
            "splitLine": {"lineStyle": {"color": "rgba(255,255,255,0.06)"}}
        },
        "dataZoom": [
            {
                "type": "slider",
                "xAxisIndex": 0,
                "startValue": start_ix,
                "endValue": end_ix,
                "zoomLock": True,
                "minValueSpan": window,
                "maxValueSpan": window,
                "bottom": 24,
                "height": 14,
                "handleSize": 0,
                "moveHandleSize": 0,
                "showDetail": False,
                "brushSelect": False,
                "borderColor": "rgba(0,0,0,0)",
                "backgroundColor": "rgba(255,255,255,0.06)",
                "fillerColor": "rgba(255,255,255,0.28)"
            },
            {
                "type": "inside",
                "xAxisIndex": 0,
                "startValue": start_ix,
                "endValue": end_ix,
                "zoomLock": True,
                "minValueSpan": window,
                "maxValueSpan": window,
                "zoomOnMouseWheel": False,
                "moveOnMouseWheel": False,
                "moveOnMouseMove": False
            },
        ],
        "series": [
            {
                "name": "Real acumulado",
                "type": "bar",
                "data": real,
                "barMaxWidth": 26,
                "itemStyle": {"borderRadius": [6, 6, 0, 0], "color": ACCENT_GREEN},
                "emphasis": {"itemStyle": {"color": ACCENT_GREEN}},
                "label": {"show": True, "position": "top", "fontSize": 11, "color": "#E5E7EB", "formatter": "{c}"},
                "labelLayout": {"hideOverlap": True},
            },
            {
                "name": "Meta acumulada",
                "type": "line",
                "data": meta,
                "smooth": True,
                "symbol": "circle",
                "symbolSize": 6,
                "lineStyle": {"width": 2, "color": YELLOW_LINE},
                "itemStyle": {"color": YELLOW_LINE},
                "label": {"show": False},
                "endLabel": {"show": False},
            },
        ],
    }
    st_echarts(options=options, height="420px", theme="dark")

st.caption("Barras = Real acumulado (verde) | Linha = Meta acumulada (amarela).")

st.divider()

# ========= DOIS GRÁFICOS LADO A LADO =========
col_left, col_right = st.columns(2, gap="large")

# ----- Atrasados por dia (Target SUP) -----
with col_left:
    st.subheader("⏰ Atrasados por dia (Target SUP)")

    fin1_plot = parse_bq_date(fdf["finalizacao_primeira"])
    fin2_plot = parse_bq_date(fdf["finalizado_final"])
    target_plot = parse_bq_date(fdf["target_sup"])
    hoje_plot = pd.Timestamp.now(tz=TZ).tz_localize(None).normalize()

    mask_atrasados = fin1_plot.notna() & fin2_plot.isna() & target_plot.notna() & (hoje_plot > target_plot)

    if not mask_atrasados.any():
        st.info("Nenhum atrasado na janela atual.")
    else:
        tsup_norm = target_plot.dt.normalize()
        g1 = (
            pd.DataFrame({"tsup": tsup_norm[mask_atrasados]})
              .dropna()
              .value_counts()
              .reset_index(name="qtd")
              .rename(columns={"index": "tsup"})
              .sort_values("tsup")
        )
        g1["label"] = g1["tsup"].dt.strftime("%d/%m")

        n   = len(g1)
        win = min(6, n if n > 0 else 6)
        s   = 0
        e   = max(0, win - 1)

        options_g1 = {
            "backgroundColor": "transparent",
            "tooltip": {"trigger": "axis"},
            "grid": {"left": 12, "right": 20, "top": 20, "bottom": 56, "containLabel": True},
            "xAxis": {
                "type": "category",
                "data": g1["label"].tolist(),
                "axisLabel": {"color": "#E5E7EB", "interval": 0},
                "axisLine": {"lineStyle": {"color": "rgba(255,255,255,0.15)"}}
            },
            "yAxis": {
                "type": "value",
                "axisLabel": {"show": False},
                "axisLine": {"show": False},
                "axisTick": {"show": False},
                "splitLine": {"show": False}
            },
            "dataZoom": [
                {"type": "slider", "xAxisIndex": 0, "startValue": s, "endValue": e,
                 "zoomLock": True, "minValueSpan": win, "maxValueSpan": win,
                 "bottom": 12, "height": 12, "handleSize": 0, "moveHandleSize": 0,
                 "showDetail": False, "brushSelect": False,
                 "borderColor": "rgba(0,0,0,0)", "backgroundColor": "rgba(255,255,255,0.06)",
                 "fillerColor": "rgba(255,255,255,0.28)"},
                {"type": "inside", "xAxisIndex": 0, "startValue": s, "endValue": e,
                 "zoomLock": True, "minValueSpan": win, "maxValueSpan": win,
                 "zoomOnMouseWheel": False, "moveOnMouseWheel": False, "moveOnMouseMove": False},
            ],
            "series": [{
                "type": "bar",
                "data": g1["qtd"].tolist(),
                "barMaxWidth": 32,
                "itemStyle": {"borderRadius": [6, 6, 0, 0], "color": ACCENT_GREEN},
                "label": {"show": True, "position": "top", "color": "#E5E7EB"}
            }],
        }
        st_echarts(options=options_g1, height="360px", theme="dark")

# ----- Atrasados por status -----
with col_right:
    st.subheader("🚩 Atrasados por status")

    fin1_plot = parse_bq_date(fdf["finalizacao_primeira"])
    fin2_plot = parse_bq_date(fdf["finalizado_final"])
    target_plot = parse_bq_date(fdf["target_sup"])
    hoje_plot = pd.Timestamp.now(tz=TZ).tz_localize(None).normalize()

    mask_atrasados = fin1_plot.notna() & fin2_plot.isna() & target_plot.notna() & (hoje_plot > target_plot)

    if not mask_atrasados.any():
        st.info("Sem atrasados para agrupar por status.")
    else:
        status_series = (
            fdf.loc[mask_atrasados, "status_atraso"]
               .fillna("—")
               .astype(str)
               .str.strip()
               .replace({"": "—", "nan": "—", "None": "—"})
        )

        g2 = status_series.value_counts().reset_index()
        g2.columns = ["status", "qtd"]
        g2 = g2.sort_values("qtd", ascending=False)

        n   = len(g2)
        win = min(10, n if n > 0 else 10)
        s   = 0
        e   = max(0, win - 1)

        options_g2 = {
            "backgroundColor": "transparent",
            "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
            "grid": {"left": 0, "right": 80, "top": 10, "bottom": 10, "containLabel": True},
            "xAxis": {"type": "value", "axisLabel": {"show": False},
                      "axisLine": {"show": False}, "axisTick": {"show": False},
                      "splitLine": {"show": False}},
            "yAxis": {"type": "category", "data": g2["status"].tolist(),
                      "inverse": True,
                      "axisLabel": {"color": "#E5E7EB", "margin": 8},
                      "axisLine": {"show": False}, "axisTick": {"show": False}},
            "dataZoom": [
                {"type": "slider", "yAxisIndex": 0, "startValue": s, "endValue": e,
                 "zoomLock": True, "minValueSpan": win, "maxValueSpan": win,
                 "right": 6, "width": 10, "handleSize": 0, "moveHandleSize": 0,
                 "showDetail": False, "brushSelect": False,
                 "backgroundColor": "rgba(255,255,255,0.06)",
                 "fillerColor": "rgba(255,255,255,0.28)", "borderColor": "rgba(0,0,0,0)"},
                {"type": "inside", "yAxisIndex": 0, "startValue": s, "endValue": e,
                 "zoomLock": True, "minValueSpan": win, "maxValueSpan": win,
                 "zoomOnMouseWheel": False, "moveOnMouseWheel": False, "moveOnMouseMove": False},
            ],
            "series": [{
                "type": "bar",
                "data": g2["qtd"].tolist(),
                "barCategoryGap": "35%",
                "itemStyle": {"borderRadius": [0, 8, 8, 0], "color": ACCENT_GREEN},
                "label": {"show": True, "position": "right", "color": "#FFFFFF", "fontWeight": "bold"},
            }],
        }
        height = max(300, 38 * min(win, n))
        st_echarts(options=options_g2, height=f"{height}px", theme="dark")