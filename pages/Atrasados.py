# pages/Atrasados.py
import os
from pathlib import Path
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import altair as alt  # ainda útil se quiser
from streamlit_echarts import st_echarts

# ========= Config =========
st.set_page_config(page_title="Analyzer — Atrasados", layout="wide")

PROJECT_ID   = "leads-ts"
MEMBROS_FQN  = "`leads-ts.Analyzer.membros_2025_s`"
TZ           = "America/Sao_Paulo"

# ========= CSS externo =========
def load_css(*files: str):
    for f in files:
        p = Path("styles") / f
        if p.exists():
            st.markdown(f"<style>{p.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

load_css("base.css", "sidebar.css", "dashboards.css")

# ========= SIDEBAR =========
st.sidebar.markdown('<div class="sb-wrap">', unsafe_allow_html=True)

if "refresh_key" not in st.session_state:
    st.session_state.refresh_key = 0
if st.sidebar.button("🔄 Atualizar agora", use_container_width=True, key="btn_refresh_now_atr"):
    st.session_state.refresh_key += 1
cache_bust = f"{st.session_state.refresh_key}"

st.sidebar.divider()
st.sidebar.markdown('<div class="sb-box">', unsafe_allow_html=True)
if st.sidebar.button("🏠 Home", use_container_width=True, key="nav_home_btn_atr"):
    st.switch_page("main.py")
if st.sidebar.button("📈 Análise de Membros", use_container_width=True, key="nav_membros_btn_atr"):
    st.switch_page("pages/Tela2.py")
st.sidebar.button("⛔ Atrasados", use_container_width=True, key="nav_atrasados_btn_atr_disabled", disabled=True)
st.sidebar.markdown('</div>', unsafe_allow_html=True)

st.sidebar.divider()

auth_mode = "secrets" if "gcp_service_account" in st.secrets else (
    "arquivo local" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") else "desconhecido"
)

# espaço flexível + rodapé fixo
st.sidebar.markdown('<div class="sb-grow"></div>', unsafe_allow_html=True)
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
    st.title("⛔ Atrasados")
with hdr_r:
    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    t1, t2 = st.columns([1, 1], gap="small")
    with t1:
        st.toggle("Pagante",   key="tog_pagante",   on_change=_on_toggle_pagante)
    with t2:
        st.toggle("Adicional", key="tog_adicional", on_change=_on_toggle_adicional)

# ===== Estilo (títulos em var; sem ícones) =====
st.markdown("""
<style>
:root{
  --kpi-title-size: .72rem;  /* ajuste fácil aqui */
  --kpi-value-size: 1.88rem;
}
section.main div[data-testid="stHorizontalBlock"] .stToggle { transform: scale(.92); }

.panel{
  background:#0d1323; border:1px solid #1f2937; border-radius:18px;
  padding:10px; box-shadow:0 1px 14px rgba(0,0,0,.25);
}

.kpi-grid{ display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:10px; }
@media (max-width:1500px){ .kpi-grid{ grid-template-columns:repeat(4,minmax(0,1fr)); } }
@media (max-width:1100px){ .kpi-grid{ grid-template-columns:repeat(3,minmax(0,1fr)); } }
@media (max-width:700px) { .kpi-grid{ grid-template-columns:repeat(2,minmax(0,1fr)); } }

.kpi-card{
  background:#101828; border:1px solid #1f2937; border-radius:14px;
  padding:10px 12px; min-height:104px; display:flex; flex-direction:column; justify-content:center;
}

.kpi-value{ font-size:var(--kpi-value-size); font-weight:800; color:#e5e7eb; margin-top:4px; display:flex; align-items:baseline; gap:8px;}
.kpi-pct{   font-size:.78rem; color:#9ca3af; }

.kpi-title{
  font-size:var(--kpi-title-size) !important;
  line-height:1.15;
  margin:0;
  font-weight:600;
  color:#9ca3af;
  letter-spacing:.1px;
  display:block;
}

/* título externo do gráfico */
.chart-title{
  font-size:1.25rem;
  font-weight:800;
  color:#E5E7EB;
  margin:0 0 8px 4px;
  letter-spacing:.2px;
}
</style>
""", unsafe_allow_html=True)

# ========= BigQuery =========
def get_bq_client():
    try:
        if "gcp_service_account" in st.secrets:
            creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            return bigquery.Client(project=st.secrets.get("gcp_project_id", PROJECT_ID), credentials=creds)
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        st.error("Falha ao criar o client do BigQuery.")
        st.exception(e)
        st.stop()

@st.cache_data(show_spinner=False)
def run_query(sql: str):
    client = get_bq_client()
    return client.query(sql).result().to_dataframe(create_bqstorage_client=False)

# ========= Dados base =========
sql_membros = f"""
SELECT
  id, gestor, email, titularidade,
  data_primeiro_contato, finalizacao_primeira, finalizado_final,
  target_sup, status_atraso, updated_at, ingestion_time,
  late_sup_atraso
FROM {MEMBROS_FQN}
ORDER BY ingestion_time DESC
-- cache_bust:{cache_bust}
"""
with st.spinner("Consultando BigQuery…"):
    df = run_query(sql_membros)

if df.empty:
    st.info("Sem registros na tabela.")
    st.stop()

# 👉 Atualiza carimbo de tempo no rodapé da sidebar
_sb_last_placeholder.caption(
    f"🕒 Última atualização: {pd.Timestamp.now(tz=TZ).strftime('%d/%m/%Y %H:%M:%S')}"
)

# ========= Filtro por titularidade =========
def classifica_tit(s: str) -> str:
    if not isinstance(s, str): return "Pagante"
    s_low = s.strip().lower()
    if "adicional" in s_low: return "Adicional"
    if ("benef" in s_low) or ("titular" in s_low): return "Pagante"
    return "Pagante"

df["tipo_titularidade"] = df["titularidade"].apply(classifica_tit)
tit_choice = st.session_state.get("tit_choice")
fdf = df[df["tipo_titularidade"] == tit_choice].copy() if tit_choice in ("Pagante", "Adicional") else df.copy()

# ========= Métricas base =========
fin1 = pd.to_datetime(fdf["finalizacao_primeira"], errors="coerce")
fin2 = pd.to_datetime(fdf["finalizado_final"],    errors="coerce")
d1   = pd.to_datetime(fdf["data_primeiro_contato"], errors="coerce")

# normaliza (naive / dia)
d1_norm   = d1.dt.tz_localize(None).dt.normalize()
fin1_norm = fin1.dt.tz_localize(None).dt.normalize()
fin2_norm = fin2.dt.tz_localize(None).dt.normalize()

today     = pd.Timestamp.now(tz=TZ).normalize().tz_localize(None)
age_days  = (today - d1_norm).dt.days

tem_gestor = fdf["gestor"].notna() & (fdf["gestor"].astype(str).str.strip() != "")
tem_email  = fdf["email"].notna()  & (fdf["email"].astype(str).str.strip()  != "")
membros_com_gestor = int((tem_gestor & tem_email).sum())

# atrasos correntes
atrasados_primeira_mask   = (fin1.isna() & (age_days > 2))
atrasados_primeira        = int(atrasados_primeira_mask.sum())

target_sup  = d1_norm + pd.Timedelta(days=7)
atrasados_segunda_mask    = (fin1.notna() & fin2.isna() & (today > target_sup))
atrasados_segunda         = int(atrasados_segunda_mask.sum())

# resolvidos com atraso
resolvidos_atraso_1 = int((fin1_norm.notna() & d1_norm.notna() & ((fin1_norm - d1_norm).dt.days > 2)).sum())
resolvidos_atraso_2 = int((fin2_norm.notna() & d1_norm.notna() & ((fin2_norm - d1_norm).dt.days > 7)).sum())

# totais
total_atrasados_1 = resolvidos_atraso_1 + atrasados_primeira
total_atrasados_2 = resolvidos_atraso_2 + atrasados_segunda

# buckets (para cards e para tabela)
over1_days = (age_days - 2).clip(lower=0)
over2_days = (today - target_sup).dt.days

ate7_1    = int((atrasados_primeira_mask & over1_days.between(1, 7)).sum())
de8a14_1  = int((atrasados_primeira_mask & over1_days.between(8, 14)).sum())
acima15_1 = int((atrasados_primeira_mask & (over1_days >= 15)).sum())

ate7_2    = int((atrasados_segunda_mask & over2_days.between(1, 7)).sum())
de8a14_2  = int((atrasados_segunda_mask & over2_days.between(8, 14)).sum())
acima15_2 = int((atrasados_segunda_mask & (over2_days >= 15)).sum())

# ========= Helpers de UI =========
def fmt_num(n:int) -> str:
    try: return f"{int(n):,}".replace(",", ".")
    except: return "0"

def fmt_pct(n:int, den:int) -> str:
    if not den or den <= 0: return ""
    return f"{(100*float(n)/float(den)):.1f}%"

def kpi_card(title: str, value: int, den: bool = False) -> str:
    pct = fmt_pct(value, membros_com_gestor) if den else ""
    pct_html = f'<span class="kpi-pct">{pct}</span>' if pct else ""
    return f"""
<div class="kpi-card">
  <p class="kpi-title">{title}</p>
  <div class="kpi-value">{fmt_num(value)}{pct_html}</div>
</div>
"""

# ========= LAYOUT — KPIs =========
top_html = f"""
<div class="panel">
  <div class="kpi-grid">
    {kpi_card("Total de Atrasados 1º Target", total_atrasados_1, den=True)}
    {kpi_card("Resolvidos 1º Target", resolvidos_atraso_1, den=True)}
    {kpi_card("Em Atraso 1º Target", atrasados_primeira, den=True)}
    {kpi_card("Até 7 dias de Atraso 1º Target", ate7_1, den=True)}
    {kpi_card("8 à 14 dias de Atraso 1º Target", de8a14_1, den=True)}
    {kpi_card("Acima de 15 Dias 1º Target", acima15_1, den=True)}
  </div>
</div>
"""

bottom_html = f"""
<div class="panel" style="margin-top:12px;">
  <div class="kpi-grid">
    {kpi_card("Total de Atrasados 2º Target", total_atrasados_2, den=True)}
    {kpi_card("Resolvidos 2º Target", resolvidos_atraso_2, den=True)}
    {kpi_card("Em Atraso 2º Target", atrasados_segunda, den=True)}
    {kpi_card("Até 7 dias de Atraso 2º Target", ate7_2, den=True)}
    {kpi_card("8 à 14 dias de Atraso 2º Target", de8a14_2, den=True)}
    {kpi_card("Acima de 15 Dias 2º Target", acima15_2, den=True)}
  </div>
</div>
"""
st.markdown(top_html + bottom_html, unsafe_allow_html=True)

# ========= GRÁFICO — Atrasados 2ª etapa por Gestor (colunas com scroller) =========
BAR_COLOR = "#93C5FD"  # cor desta página

def _locked_slider_common():
    return {
        "handleSize": 0, "handleStyle": {"opacity": 0}, "showDetail": False, "brushSelect": False,
        "fillerColor": "rgba(255,255,255,0.18)", "backgroundColor": "rgba(255,255,255,0.06)",
        "borderColor": "rgba(255,255,255,0.15)",
    }

def echarts_vertical_bar(labels, values, title=None, bar_color=BAR_COLOR, window=6):
    n = len(labels)
    if n == 0:
        st.info("Sem dados.")
        return
    start_idx = 0
    end_idx   = min(window - 1, n - 1)

    options = {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 40, "right": 20, "top": 16, "bottom": 80, "containLabel": True},
        "xAxis": {
            "type": "category", "data": labels,
            "axisLabel": {"color": "#E5E7EB", "interval": 0},
            "axisLine": {"show": False}, "axisTick": {"show": False},
        },
        "yAxis": {
            "type": "value",
            "axisLabel": {"show": False}, "axisLine": {"show": False},
            "axisTick": {"show": False}, "splitLine": {"show": False},
        },
        "dataZoom": [
            {"type": "slider", "xAxisIndex": 0, "startValue": start_idx, "endValue": end_idx,
             "zoomLock": True, "minValueSpan": window, "maxValueSpan": window, "bottom": 24,
             **_locked_slider_common()},
            {"type": "inside", "xAxisIndex": 0, "startValue": start_idx, "endValue": end_idx,
             "zoomLock": True, "minValueSpan": window, "maxValueSpan": window},
        ],
        "series": [{
            "type": "bar", "data": values,
            "itemStyle": {"color": bar_color, "borderRadius": [8, 8, 0, 0]},
            "label": {"show": True, "position": "top", "color": "#FFFFFF", "fontWeight": "bold"},
        }],
    }
    st_echarts(options=options, height="360px", theme="dark")

st.markdown('<div class="panel" style="margin-top:12px;">', unsafe_allow_html=True)
st.markdown('<div class="chart-title">Atrasados 2º Target por Gestor</div>', unsafe_allow_html=True)

if "gestor" in fdf.columns:
    serie_gestor = (
        fdf.loc[atrasados_segunda_mask, "gestor"]
           .fillna("Sem gestor").astype(str).str.strip()
           .replace({"": "Sem gestor", "nan": "Sem gestor", "None": "Sem gestor"})
    )
    counts = (
        serie_gestor.value_counts(dropna=False)
                    .rename_axis("Gestor").reset_index(name="Quantidade")
                    .sort_values("Quantidade", ascending=False)
    )
    if counts.empty:
        st.info("Sem registros atrasados na 2ª etapa para exibir no gráfico.")
    else:
        labels = counts["Gestor"].tolist()
        values = counts["Quantidade"].tolist()
        echarts_vertical_bar(labels, values, title=None, window=6)
else:
    st.warning("Coluna 'gestor' não encontrada no dataset retornado.")
st.markdown('</div>', unsafe_allow_html=True)

# ========= TABELA — Métricas de atraso por Gestor =========
st.markdown('<div class="panel" style="margin-top:12px;">', unsafe_allow_html=True)

if "gestor" in fdf.columns:
    gestor_norm = (
        fdf["gestor"].fillna("Sem gestor").astype(str).str.strip()
           .replace({"": "Sem gestor", "nan": "Sem gestor", "None": "Sem gestor"})
    )

    mask1 = atrasados_primeira_mask
    mask2 = atrasados_segunda_mask

    b1_ate7    = (mask1 & over1_days.between(1, 7)).astype(int)
    b1_8a14    = (mask1 & over1_days.between(8, 14)).astype(int)
    b1_15plus  = (mask1 & (over1_days >= 15)).astype(int)

    b2_ate7    = (mask2 & over2_days.between(1, 7)).astype(int)
    b2_8a14    = (mask2 & over2_days.between(8, 14)).astype(int)
    b2_15plus  = (mask2 & (over2_days >= 15)).astype(int)

    base = pd.DataFrame({
        "Gestor": gestor_norm,
        "Atrasados 1º Target": mask1.astype(int),
        "Atrasados 2º Target": mask2.astype(int),
        "Até 7 dias 1º": b1_ate7,
        "8–14 dias 1º": b1_8a14,
        "15+ dias 1º": b1_15plus,
        "Até 7 dias 2º": b2_ate7,
        "8–14 dias 2º": b2_8a14,
        "15+ dias 2º": b2_15plus,
    })

    tabela = (
        base.groupby("Gestor", dropna=False, as_index=False).sum(numeric_only=True)
            .sort_values("Atrasados 2º Target", ascending=False)
    )

    st.dataframe(tabela, use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar CSV",
        tabela.to_csv(index=False).encode("utf-8"),
        file_name="atrasos_por_gestor.csv",
        mime="text/csv",
    )
else:
    st.warning("Coluna 'gestor' não encontrada no dataset retornado.")

st.markdown('</div>', unsafe_allow_html=True)
