# pages/Atrasados-aldeia.py
import os
from pathlib import Path
import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from streamlit_echarts import st_echarts

# ========= Config =========
st.set_page_config(page_title="Analyzer — Atrasados (Aldeia)", layout="wide")

PROJECT_ID   = "leads-ts"
MEMBROS_FQN  = "`leads-ts.Analyzer.aldeia_2026_s`"
TZ           = "America/Sao_Paulo"

# ========= Paleta =========
ACCENT_GREEN = "#C9E34F"
ACCENT_RED   = "#F87171"

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
    st.rerun()

cache_bust = f"{st.session_state.refresh_key}"

st.sidebar.divider()
st.sidebar.markdown('<div class="sb-box">', unsafe_allow_html=True)

if st.sidebar.button("🏠 Home", use_container_width=True, key="nav_home_btn_atr"):
    st.switch_page("pages/analyzer-aldeia.py")
if st.sidebar.button("📈 Análise de Membros", use_container_width=True, key="nav_membros_btn_atr"):
    st.switch_page("pages/Analise-aldeia.py")
st.sidebar.button("⛔ Atrasados", use_container_width=True, key="nav_atrasados_btn_atr_disabled", disabled=True)

if st.sidebar.button("🎟️ Presenciais", use_container_width=True, key="nav_presenciais_btn_atrasados_aldeia"):
    st.switch_page("pages/Presenciais-aldeia.py")

st.sidebar.markdown('</div>', unsafe_allow_html=True)
st.sidebar.divider()

auth_mode = "secrets" if "gcp_service_account" in st.secrets else (
    "arquivo local" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") else "desconhecido"
)

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
    st.title("⛔ Atrasados (ALDEIA)")
with hdr_r:
    if st.button("↩️ Modo Tribo", use_container_width=True, key="btn_modo_tribo_hdr_atr_aldeia"):
        st.switch_page("pages/Atrasados.py")

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    t1, t2 = st.columns([1, 1], gap="small")
    with t1:
        st.toggle("Pagante", key="tog_pagante", on_change=_on_toggle_pagante)
    with t2:
        st.toggle("Adicional", key="tog_adicional", on_change=_on_toggle_adicional)

st.divider()

# ===== Estilo =====
st.markdown("""
<style>
:root{
  --kpi-title-size: .72rem;
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

def find_col(dataframe: pd.DataFrame, candidates):
    cols_lower = {c.lower(): c for c in dataframe.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None

# ========= Dados base =========
sql_membros = f"""
SELECT *
FROM {MEMBROS_FQN}
ORDER BY ingestion_time DESC
-- cache_bust:{cache_bust}
"""
with st.spinner("Consultando BigQuery…"):
    df = run_query(sql_membros)

if df.empty:
    st.info("Sem registros na tabela.")
    st.stop()

_sb_last_placeholder.caption(
    f"🕒 Última atualização: {pd.Timestamp.now(tz=TZ).strftime('%d/%m/%Y %H:%M:%S')}"
)

BROKER_COL = find_col(df, ["equipe", "broker", "brokers", "corretora", "empresa"])
if BROKER_COL is None and "gestor" in df.columns:
    BROKER_COL = "gestor"

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

# ========= Filtro por titularidade =========
df["tipo_titularidade"] = df.get("titularidade", "").apply(classifica_tit) if "titularidade" in df.columns else "Pagante"
tit_choice = st.session_state.get("tit_choice")
fdf = df[df["tipo_titularidade"] == tit_choice].copy() if tit_choice in ("Pagante", "Adicional") else df.copy()

# ========= Métricas base =========
email_ok = is_filled(fdf["email"]) if "email" in fdf.columns else pd.Series([False] * len(fdf), index=fdf.index)

target_sup_dt = parse_bq_date(fdf["target_sup"]) if "target_sup" in fdf.columns else pd.Series([pd.NaT] * len(fdf), index=fdf.index)
fin1_dt = parse_bq_date(fdf["finalizacao_primeira"]) if "finalizacao_primeira" in fdf.columns else pd.Series([pd.NaT] * len(fdf), index=fdf.index)
fin2_dt = parse_bq_date(fdf["finalizado_final"]) if "finalizado_final" in fdf.columns else pd.Series([pd.NaT] * len(fdf), index=fdf.index)

fin1_filled = fin1_dt.notna()
fin2_filled = fin2_dt.notna()

today = pd.Timestamp.now(tz=TZ).tz_localize(None).normalize()
cutoff_primeira = target_sup_dt - pd.Timedelta(days=5)

membros_com_equipe = int(email_ok.sum())

# -------- 1ª ETAPA --------
# em atraso agora
atrasados_primeira_mask = email_ok & (~fin1_filled) & cutoff_primeira.notna() & (cutoff_primeira < today)
atrasados_primeira = int(atrasados_primeira_mask.sum())

# resolvidos com atraso
resolvidos_atraso_1_mask = email_ok & fin1_dt.notna() & cutoff_primeira.notna() & (fin1_dt > cutoff_primeira)
resolvidos_atraso_1 = int(resolvidos_atraso_1_mask.sum())

# buckets atraso 1ª etapa
over1_days = (today - cutoff_primeira).dt.days.clip(lower=0)
ate7_1    = int((atrasados_primeira_mask & over1_days.between(1, 7)).sum())
de8a14_1  = int((atrasados_primeira_mask & over1_days.between(8, 14)).sum())
acima15_1 = int((atrasados_primeira_mask & (over1_days >= 15)).sum())

total_atrasados_1 = resolvidos_atraso_1 + atrasados_primeira

# -------- 2ª ETAPA --------
# em atraso agora
atrasados_segunda_mask = email_ok & fin1_filled & (~fin2_filled) & target_sup_dt.notna() & (target_sup_dt < today)
atrasados_segunda = int(atrasados_segunda_mask.sum())

# resolvidos com atraso
resolvidos_atraso_2_mask = email_ok & fin1_filled & fin2_dt.notna() & target_sup_dt.notna() & (fin2_dt > target_sup_dt)
resolvidos_atraso_2 = int(resolvidos_atraso_2_mask.sum())

# buckets atraso 2ª etapa
over2_days = (today - target_sup_dt).dt.days.clip(lower=0)
ate7_2    = int((atrasados_segunda_mask & over2_days.between(1, 7)).sum())
de8a14_2  = int((atrasados_segunda_mask & over2_days.between(8, 14)).sum())
acima15_2 = int((atrasados_segunda_mask & (over2_days >= 15)).sum())

total_atrasados_2 = resolvidos_atraso_2 + atrasados_segunda

# ========= Helpers de UI =========
def fmt_num(n: int) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except:
        return "0"

def fmt_pct(n: int, den: int) -> str:
    if not den or den <= 0:
        return ""
    return f"{(100 * float(n) / float(den)):.1f}%"

def kpi_card(title: str, value: int, den: bool = False) -> str:
    pct = fmt_pct(value, membros_com_equipe) if den else ""
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

# ========= Gráfico — Atrasados (2º Target) por Equipe =========
def _locked_slider_common():
    return {
        "handleSize": 0, "handleStyle": {"opacity": 0}, "showDetail": False, "brushSelect": False,
        "fillerColor": "rgba(255,255,255,0.18)", "backgroundColor": "rgba(255,255,255,0.06)",
        "borderColor": "rgba(255,255,255,0.15)",
    }

def echarts_vertical_bar(labels, values, window=6, bar_color=ACCENT_RED):
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
st.markdown('<div class="chart-title">Atrasados Geral por Equipe</div>', unsafe_allow_html=True)

if BROKER_COL:
    serie_equipe = (
        fdf.loc[atrasados_segunda_mask, BROKER_COL]
            .fillna("Sem equipe").astype(str).str.strip()
            .replace({"": "Sem equipe", "nan": "Sem equipe", "None": "Sem equipe"})
    )
    counts = (
        serie_equipe.value_counts(dropna=False)
                    .rename_axis("Equipe").reset_index(name="Quantidade")
                    .sort_values("Quantidade", ascending=False)
    )
    if counts.empty:
        st.info("Sem registros atrasados na 2ª etapa para exibir no gráfico.")
    else:
        labels = counts["Equipe"].tolist()
        values = counts["Quantidade"].tolist()
        echarts_vertical_bar(labels, values, window=6, bar_color=ACCENT_RED)
else:
    st.warning("Não encontrei uma coluna de Equipe (broker/corretora/empresa).")

st.markdown('</div>', unsafe_allow_html=True)

# ========= TABELA — Métricas de atraso por Equipe =========
st.markdown('<div class="panel" style="margin-top:12px;">', unsafe_allow_html=True)

if BROKER_COL:
    equipe_norm = (
        fdf[BROKER_COL].fillna("Sem equipe").astype(str).str.strip()
           .replace({"": "Sem equipe", "nan": "Sem equipe", "None": "Sem equipe"})
    )

    mask1 = atrasados_primeira_mask
    mask2 = atrasados_segunda_mask

    b1_ate7   = (mask1 & over1_days.between(1, 7)).astype(int)
    b1_8a14   = (mask1 & over1_days.between(8, 14)).astype(int)
    b1_15plus = (mask1 & (over1_days >= 15)).astype(int)

    b2_ate7   = (mask2 & over2_days.between(1, 7)).astype(int)
    b2_8a14   = (mask2 & over2_days.between(8, 14)).astype(int)
    b2_15plus = (mask2 & (over2_days >= 15)).astype(int)

    base = pd.DataFrame({
        "Equipe": equipe_norm,
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
        base.groupby("Equipe", dropna=False, as_index=False).sum(numeric_only=True)
            .sort_values(["Atrasados 2º Target", "Atrasados 1º Target"], ascending=[False, False])
    )

    st.dataframe(tabela, use_container_width=True, hide_index=True)
    st.download_button(
        "Baixar CSV",
        tabela.to_csv(index=False).encode("utf-8"),
        file_name="atrasos_por_equipe.csv",
        mime="text/csv",
    )
else:
    st.warning("Não encontrei uma coluna de Equipe (broker/corretora/empresa).")

st.markdown('</div>', unsafe_allow_html=True)