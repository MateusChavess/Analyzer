# ============================================================
# Analyzer – Dashboard Streamlit (BigQuery + Altair + ECharts)
# ============================================================

import os
import math
import pandas as pd
import numpy as np
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import altair as alt
from streamlit_echarts import st_echarts
import textwrap

# ---------------------------
# 1) CONFIG & STREAMLIT BASE
# ---------------------------
PROJECT_ID = "leads-ts"
TABLE_FQN  = "`leads-ts.Analyzer.aldeia_2026_s`"

st.set_page_config(page_title="Analyzer (ALDEIA)", layout="wide")

# === PALETAS ===
ACCENT_GREEN = "#C9E34F"
ZERO_NAVY = "#0d1323"
CAL_DARK_RANGE = [
    ZERO_NAVY,
    "#0E2F25",
    "#134C3A",
    "#1B5E44",
]
BAR_COLOR = ACCENT_GREEN
GREYS = ["#0f172a", "#111827", "#1f2937", "#374151", "#4b5563", "#6b7280", "#9ca3af"]

# =============== SIDEBAR (layout) ===============
st.sidebar.markdown("""
<style>
[data-testid="stSidebar"] > div:first-child {
  padding-top: 0.75rem; padding-bottom: 0.75rem;
  height: 100dvh; overflow: hidden; box-sizing: border-box;
}
/* Oculta navegação padrão do Streamlit no sidebar */
div[data-testid="stSidebarNav"],
div[data-testid="stSidebarNavSearch"] { display: none !important; }

.sb-wrap{ display:flex; flex-direction:column; height:100%; }
.sb-grow{ flex:1 1 auto; }
.sb-footer{ padding-top:.25rem; }

.sb-box{
  border:1px solid rgba(255,255,255,.10);
  border-radius:12px; padding:10px; margin:8px 0 12px;
}
.sb-box .stButton{ margin:4px 0; }

/* Esconde stButton por padrão; mostra só dentro de .nav-only */
.sb-box .stButton { display:none !important; }
.sb-box .nav-only .stButton { display:block !important; }

/* Remove inputs padrão do sidebar */
[data-testid="stSidebar"] .stTextInput,
[data-testid="stSidebar"] input[type="text"],
[data-testid="stSidebar"] [data-baseweb="input"] {
  display:none !important; height:0 !important; margin:0 !important; padding:0 !important; border:0 !important;
}
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown('<div class="sb-wrap">', unsafe_allow_html=True)

# ---------------------------
# BOTÃO DE ATUALIZAR
# ---------------------------
if "refresh_key" not in st.session_state:
    st.session_state.refresh_key = 0

if st.sidebar.button("🔄 Atualizar agora", use_container_width=True, key="btn_refresh_now"):
    st.session_state.refresh_key += 1
    st.rerun()

st.sidebar.divider()

_nav = st.sidebar.empty()
with _nav.container():
    st.sidebar.markdown('<div class="sb-box"><div class="nav-only">', unsafe_allow_html=True)

    st.sidebar.button("🏠 Home", use_container_width=True, key="nav_home_btn_disabled", disabled=True)

    if st.sidebar.button("📈 Análise de Membros", use_container_width=True, key="nav_membros_btn"):
        st.switch_page("pages/Analise-aldeia.py")
    if st.sidebar.button("⛔ Atrasados", use_container_width=True, key="nav_atrasados_btn"):
        st.switch_page("pages/Atrasados-aldeia.py")
    if st.sidebar.button("🎟️ Presenciais", use_container_width=True, key="nav_presenciais_btn"):
        st.switch_page("pages/Presenciais-aldeia.py")

    st.sidebar.markdown('</div></div>', unsafe_allow_html=True)

st.sidebar.divider()

_sb_auth_placeholder = st.sidebar.empty()
_sb_last_placeholder = st.sidebar.empty()

# ---------------------------
# 2) AUTENTICAÇÃO BIGQUERY
# ---------------------------
auth_mode = "desconhecido"
try:
    if "gcp_service_account" in st.secrets:
        PROJECT_ID = st.secrets.get("gcp_project_id", "leads-ts")
        creds = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        bq = bigquery.Client(project=PROJECT_ID, credentials=creds)
        auth_mode = "secrets"
    else:
        SA_PATH = r"C:\Users\mateu\StreamLit\OtavioPermissao.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
        PROJECT_ID = "leads-ts"
        bq = bigquery.Client(project=PROJECT_ID)
        auth_mode = "arquivo local"
except Exception as e:
    st.error("Falha ao criar o client do BigQuery. Verifique os secrets/JSON.")
    st.exception(e)
    st.stop()

_sb_auth_placeholder.caption(f"🔐 Modo de autenticação: {auth_mode}")

# ---------------------------
# 3) QUERY + CACHE
# ---------------------------
@st.cache_data(show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    job = bq.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=False)

cache_bust = f"{st.session_state.refresh_key}"

sql = f"""
SELECT
  id,
  target_sup,
  late_sup_atraso,
  status_atraso,
  detalhamento,
  data_ultima_acao,
  dias_na_tribo,
  data_primeiro_contato,
  turma, titularidade, nome, email, telefone,
  observacao, boas_vindas, grupos,
  mt5, conta, validacao, corretora,
  finalizacao_primeira, gestao_data, primeira_operacao, finalizado_final,
  ativacao, cancelamento,
  broker,
  empresa,
  updated_at, ingestion_time
FROM {TABLE_FQN}
ORDER BY ingestion_time DESC
-- cache_bust:{cache_bust}
"""

with st.spinner("Consultando BigQuery…"):
    df = run_query(sql)

if df.empty:
    st.warning("Nenhum registro encontrado na tabela.")
    st.stop()

last_updated_str = pd.Timestamp.now(tz='America/Sao_Paulo').strftime('%d/%m/%Y %H:%M:%S')
_sb_last_placeholder.caption(f"🕒 Última atualização: {last_updated_str}")

# acha a coluna de broker caso venha com outro nome
def find_col(dataframe: pd.DataFrame, candidates):
    cols_lower = {c.lower(): c for c in dataframe.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None

BROKER_COL = find_col(df, ["broker", "brokers", "corretora", "empresa"])

status_opts = sorted(df["status_atraso"].dropna().astype(str).unique()) if "status_atraso" in df.columns else []
turma_opts  = sorted(df["turma"].dropna().astype(str).unique()) if "turma" in df.columns else []
broker_opts = sorted(df[BROKER_COL].dropna().astype(str).unique()) if BROKER_COL else []

# ---------------------------
# ESTADO
# ---------------------------
st.session_state.setdefault("status_atraso", [])
st.session_state.setdefault("turma", [])
st.session_state.setdefault("broker", [])
st.session_state.setdefault("meses_label_sel", [])
st.session_state.setdefault("tit_choice", None)

# --- HEADER + botão "Modo Tribo" + toggles ---
st.session_state.setdefault("tog_pagante",   st.session_state["tit_choice"] == "Pagante")
st.session_state.setdefault("tog_adicional", st.session_state["tit_choice"] == "Adicional")

def _on_toggle_pagante():
    if st.session_state["tog_pagante"]:
        st.session_state["tit_choice"] = "Pagante"
        st.session_state["tog_adicional"] = False
    elif st.session_state.get("tit_choice") == "Pagante":
        st.session_state["tit_choice"] = None

def _on_toggle_adicional():
    if st.session_state["tog_adicional"]:
        st.session_state["tit_choice"] = "Adicional"
        st.session_state["tog_pagante"] = False
    elif st.session_state.get("tit_choice") == "Adicional":
        st.session_state["tit_choice"] = None

hdr_l, hdr_r = st.columns([8, 4], gap="medium")
with hdr_l:
    st.title("📊 Analyzer (ALDEIA)")
with hdr_r:
    if st.button("↩️ Modo Tribo", use_container_width=True, key="btn_voltar_tribo_hdr"):
        st.switch_page("main.py")

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
    t1, t2 = st.columns([1, 1], gap="small")
    with t1:
        st.toggle("Pagante", key="tog_pagante", on_change=_on_toggle_pagante)
    with t2:
        st.toggle("Adicional", key="tog_adicional", on_change=_on_toggle_adicional)

st.markdown("""
<style>
section.main div[data-testid="stHorizontalBlock"] .stToggle { transform: scale(.94); }
</style>
""", unsafe_allow_html=True)

# ---------------------------
# 4) CONTROLES – Equipe, Turma e Meses
# ---------------------------
st.markdown('<div class="top-controls">', unsafe_allow_html=True)

_dt_all = pd.to_datetime(df["data_primeiro_contato"], errors="coerce") if "data_primeiro_contato" in df.columns else pd.Series([], dtype="datetime64[ns]")
_periods = sorted(
    _dt_all.dropna().dt.to_period("M").unique().tolist(),
    key=lambda p: (p.year, p.month)
)

MESES_PT = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
            "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
labels = [f"{MESES_PT[p.month-1]} ({p.year})" for p in _periods]
label_to_period = dict(zip(labels, _periods))

c1, c2, c3 = st.columns([2.2, 2.2, 2.2], gap="small")

with c1:
    if BROKER_COL:
        broker_sel = st.multiselect(
            "Equipe",
            options=broker_opts,
            key="broker",
            placeholder="Selecione as equipes",
        )
    else:
        broker_sel = []
        st.caption("⚠️ Coluna de Equipe não encontrada.")

with c2:
    turma_sel = st.multiselect(
        "Turma",
        options=turma_opts,
        key="turma",
        placeholder="Selecione as turmas",
    )

with c3:
    meses_label_sel = st.multiselect(
        "Mês (Primeiro Contato)",
        options=labels,
        key="meses_label_sel",
        placeholder="Selecione os meses",
        help="Filtra pelo(s) mês(es) de Data de Primeiro Contato!",
    )

st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------
# 5) FILTROS
# ---------------------------
def classifica_tit(s: str) -> str:
    if not isinstance(s, str):
        return "Pagante"
    s_low = s.strip().lower()
    if "adicional" in s_low:
        return "Adicional"
    if ("benef" in s_low) or ("titular" in s_low):
        return "Pagante"
    return "Pagante"

df["tipo_titularidade"] = df["titularidade"].apply(classifica_tit) if "titularidade" in df.columns else "Pagante"

tit_choice = st.session_state.get("tit_choice")
fdf = df[df["tipo_titularidade"] == tit_choice].copy() if tit_choice in ("Pagante", "Adicional") else df.copy()

broker_sel = [b for b in st.session_state.get("broker", []) if b in broker_opts]
turma_sel  = [t for t in st.session_state.get("turma", []) if t in turma_opts]
meses_sel  = [m for m in st.session_state.get("meses_label_sel", []) if m in labels]

if BROKER_COL and broker_sel:
    fdf = fdf[fdf[BROKER_COL].astype(str).isin(broker_sel)]
if turma_sel:
    fdf = fdf[fdf["turma"].astype(str).isin(turma_sel)]

if meses_sel:
    _periodos_escolhidos = [label_to_period[l] for l in meses_sel if l in label_to_period]
    dcol_period = pd.to_datetime(fdf["data_primeiro_contato"], errors="coerce").dt.to_period("M")
    fdf = fdf[dcol_period.isin(_periodos_escolhidos)]

if fdf.empty:
    st.info("Sem registros para os filtros atuais.")
    st.stop()

# ---------------------------
# 6) MÉTRICAS (ALINHADAS AO MAIN.PY)
# ---------------------------
base_df = fdf.copy()

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

email_ok = (
    is_filled(base_df["email"])
    if "email" in base_df.columns
    else pd.Series([False] * len(base_df), index=base_df.index)
)

hoje = pd.Timestamp.now(tz="America/Sao_Paulo").tz_localize(None).normalize()

target_sup_dt = (
    parse_bq_date(base_df["target_sup"])
    if "target_sup" in base_df.columns
    else pd.Series([pd.NaT] * len(base_df), index=base_df.index)
)

fin1_dt = (
    parse_bq_date(base_df["finalizacao_primeira"])
    if "finalizacao_primeira" in base_df.columns
    else pd.Series([pd.NaT] * len(base_df), index=base_df.index)
)

fin2_dt = (
    parse_bq_date(base_df["finalizado_final"])
    if "finalizado_final" in base_df.columns
    else pd.Series([pd.NaT] * len(base_df), index=base_df.index)
)

fin1_filled = fin1_dt.notna()
fin2_filled = fin2_dt.notna()

cutoff_primeira = target_sup_dt - pd.Timedelta(days=5)

membros_total = int(email_ok.sum())

finalizados_primeira = int((email_ok & fin1_filled).sum())
finalizados_geral = int((email_ok & fin2_filled).sum())

nao_conc_primeira_mask = email_ok & (~fin1_filled)
nao_finalizados_mask = email_ok & (~fin2_filled)

pendentes_primeira = int(nao_conc_primeira_mask.sum())
nao_finalizados_geral = int(nao_finalizados_mask.sum())

# 1ª etapa
mask_base_primeira = email_ok & (~fin1_filled) & target_sup_dt.notna()
pendentes_primeira_janela = int((mask_base_primeira & (cutoff_primeira >= hoje)).sum())
atrasados_primeira = int((mask_base_primeira & (cutoff_primeira < hoje)).sum())

# 2ª etapa
mask_base_segunda = email_ok & fin1_filled & (~fin2_filled) & target_sup_dt.notna()
pendentes_segunda = int((mask_base_segunda & (target_sup_dt >= hoje)).sum())
atrasados_segunda = int((mask_base_segunda & (target_sup_dt < hoje)).sum())

st.session_state['kpi_membros_gestor'] = membros_total
st.session_state['kpi_finalizados_geral'] = finalizados_geral
st.session_state['kpi_pend2'] = pendentes_segunda
st.session_state['kpi_atr2'] = atrasados_segunda

# ---------------------------
# 7) CSS – KPIs
# ---------------------------
CARD_CSS = """
<style>
:root{
  --panel-bg:#0d1323; --card-bg:#101828; --card-border:#1f2937;
  --txt:#e5e7eb; --muted:#9ca3af; --good:#22c55e; --warn:#f59e0b; --bad:#ef4444;
  --kpi-h: 120px;
}
.panel{ background:var(--panel-bg); border:1px solid var(--card-border);
  border-radius:18px; padding:12px; box-shadow:0 1px 14px rgba(0,0,0,.25); }
.grid-2{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:12px; }
.grid-4{ display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:12px; }
.kpi-card{ background:var(--card-bg); border:1px solid var(--card-border);
  border-radius:14px; padding:12px 14px; min-height:var(--kpi-h);
  display:flex; flex-direction:column; justify-content:center; }
.kpi-title{font-size:.88rem; color:var(--muted); display:flex; align-items:center; gap:8px; margin:0;}
.kpi-icon{display:inline-flex; justify-content:center; align-items:center; width:26px; height:26px;
  border-radius:999px; background:rgba(255,255,255,.06); border:1px solid var(--card-border); font-size:15px;}
.kpi-value{font-size:1.8rem; font-weight:800; color:#e5e7eb; margin-top:6px;}
.kpi-pct{font-size:.8rem; color:#9ca3af; margin-left:8px;}
.small-card{ background:var(--card-bg); border:1px solid var(--card-border);
  border-radius:12px; padding:10px 12px; min-height:calc(var(--kpi-h) - 20px);
  display:flex; flex-direction:column; justify-content:center; }
.small-card.warn{ box-shadow: inset 0 0 0 1px #f59e0b; }
.small-card.bad { box-shadow: inset 0 0 0 1px #ef4444; }
.solo{ background:var(--panel-bg); border:1px solid var(--card-border);
  border-radius:18px; padding:14px; }
</style>
"""
st.markdown(CARD_CSS, unsafe_allow_html=True)

def fmt_num(n:int) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except:
        return "0"

def fmt_pct(n:int, den:int) -> str:
    den = max(int(den or 0), 1)
    return f"{(100*int(n)/den):.1f}%"

def card(title, value, icon="📈", badge_class=None, pct_text=None):
    badge = (f'<span class="kpi-badge {badge_class}"><span class="kpi-icon">{icon}</span>{title}</span>'
             if badge_class else f'<span class="kpi-title"><span class="kpi-icon">{icon}</span>{title}</span>')
    html = f"""
<div class="kpi-card">
  <div class="kpi-title">{badge}</div>
  <div class="kpi-value">{fmt_num(value)}{f'<span class="kpi-pct">{pct_text}</span>' if pct_text else ''}</div>
</div>
"""
    return textwrap.dedent(html).strip()

def small_card(title, value, variant="warn", icon="⚠️", den=None):
    pct = fmt_pct(value, membros_total) if den is not None else None
    html = f"""
<div class="small-card {variant}">
  <div class="kpi-title"><span class="kpi-icon">{icon}</span>{title}</div>
  <div class="kpi-value">{fmt_num(value)}{f'<span class="kpi-pct">{pct}</span>' if pct else ''}</div>
</div>
"""
    return textwrap.dedent(html).strip()

# ---------------------------
# 8) KPIs – LAYOUT (cards)
# ---------------------------
col_left, col_right = st.columns([1, 3], gap="large")

with col_left:
    left_html = f"""
<div class="solo">
  {card("Aldeia com Equipe", membros_total, icon="👥")}
</div>
"""
    st.markdown(textwrap.dedent(left_html).strip(), unsafe_allow_html=True)

with col_right:
    right_html = f"""
<div class="panel">
  <div class="grid-2">
    {card("Finalizados 1ª Etapa", finalizados_primeira, icon="✅", badge_class="good",
          pct_text=fmt_pct(finalizados_primeira, membros_total))}
    {card("Finalizados Geral", finalizados_geral, icon="✅", badge_class="good",
          pct_text=fmt_pct(finalizados_geral, membros_total))}
    {card("Não concluído 1ª Etapa", pendentes_primeira, icon="👤",
          pct_text=fmt_pct(pendentes_primeira, membros_total))}
    {card("Não Finalizados", nao_finalizados_geral, icon="👤",
          pct_text=fmt_pct(nao_finalizados_geral, membros_total))}
  </div>

  <div class="grid-4" style="margin-top:12px;">
    {small_card("Pendentes 1ª Etapa", pendentes_primeira_janela, variant="warn", icon="⚠️", den=membros_total)}
    {small_card("Atrasados 1ª Etapa", atrasados_primeira,       variant="bad",  icon="⛔", den=membros_total)}
    {small_card("Pendentes 2ª Etapa", pendentes_segunda,        variant="warn", icon="⚠️", den=membros_total)}
    {small_card("Atrasados 2ª Etapa", atrasados_segunda,        variant="bad",  icon="⛔", den=membros_total)}
  </div>
</div>
"""
    st.markdown(textwrap.dedent(right_html).strip(), unsafe_allow_html=True)

st.divider()

# ---------------------------
# 9) BASES para gráficos
# ---------------------------
base_kpi = base_df.copy()

if BROKER_COL and not base_kpi.empty:
    by_broker = (
        base_kpi
        .assign(_broker=base_kpi[BROKER_COL].astype(str).str.strip().replace({"": "—"}))
        .groupby("_broker", dropna=False)["id"].size()
        .reset_index(name="membros")
        .rename(columns={"_broker": "broker"})
        .sort_values("membros", ascending=False)
    )
else:
    by_broker = pd.DataFrame(columns=["broker","membros"])

EXCLUDE_TURMAS = {
    "adicional brasil / mundo", "adicional tribo", "adicional",
    "aldeia adicional", "adicional aldeia"
}
if not base_kpi.empty and "turma" in base_kpi.columns:
    turma_series = base_kpi["turma"].astype(str).str.strip()
    keep_mask = ~turma_series.str.lower().isin(EXCLUDE_TURMAS)
    by_turma = (
        base_kpi.loc[keep_mask]
                .assign(turma=turma_series[keep_mask].replace({"": "—"}))
                .groupby("turma", dropna=False)["id"].size()
                .reset_index(name="membros")
                .sort_values("membros", ascending=False)
    )
else:
    by_turma = pd.DataFrame(columns=["turma","membros"])

# ---------------------------
# 10) Calendário estilizado
# ---------------------------
st.subheader("📅 Entradas por dia (Primeiro Contato)")
if "data_primeiro_contato" in base_df.columns and base_df["data_primeiro_contato"].notna().any():
    ts = pd.to_datetime(base_df["data_primeiro_contato"], errors="coerce").dropna()
    if not ts.empty:
        hoje_cal = pd.Timestamp.today()
        c1, c2 = st.columns(2)
        MESES_PT = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
        mes_nome = c1.selectbox("Mês", MESES_PT, index=hoje_cal.month - 1, key="cal_mes")
        mes = MESES_PT.index(mes_nome) + 1
        anos = sorted(ts.dt.year.unique().tolist()) or [hoje_cal.year]
        ano = c2.selectbox("Ano", anos, index=(anos.index(hoje_cal.year) if hoje_cal.year in anos else 0), key="cal_ano")

        start = pd.Timestamp(year=ano, month=mes, day=1)
        end   = start + pd.offsets.MonthEnd(1)
        counts = (
            ts[(ts.dt.year == ano) & (ts.dt.month == mes)]
              .dt.normalize().value_counts()
              .rename_axis("date").reset_index(name="qtd")
        )
        cal = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})
        cal = cal.merge(counts, on="date", how="left").fillna({"qtd": 0})

        cal["dia"] = cal["date"].dt.day
        sunday_offset = (start.weekday() + 1) % 7
        week0 = start - pd.Timedelta(days=sunday_offset)
        cal["week"] = ((cal["date"] - week0).dt.days // 7) + 1
        cal["dow"]  = (cal["date"].dt.weekday + 1) % 7
        DOW_MAP = {0:"Domingo",1:"Segunda",2:"Terça",3:"Quarta",4:"Quinta",5:"Sexta",6:"Sábado"}
        order_cols = ["Domingo","Segunda","Terça","Quarta","Quinta","Sexta","Sábado"]
        cal["dow_label"] = cal["dow"].map(DOW_MAP)

        n_weeks = int(cal["week"].max()) if not cal.empty else 5
        CELL_H  = 64 if n_weeks == 5 else 56
        CHART_H = CELL_H * n_weeks

        max_q = int(cal["qtd"].max())
        max_q = max(1, max_q)

        base = alt.Chart(cal).properties(width="container", height=int(CHART_H))
        heat = base.mark_rect(stroke="#1f2937", strokeWidth=1).encode(
            x=alt.X("dow_label:N", sort=order_cols, axis=alt.Axis(title=None, labelAngle=0, labelPadding=6, ticks=False)),
            y=alt.Y("week:O", axis=alt.Axis(title=None, ticks=False)),
            color=alt.Color(
                "qtd:Q",
                legend=None,
                scale=alt.Scale(domain=[0, max_q], range=CAL_DARK_RANGE, clamp=True)
            ),
            tooltip=[alt.Tooltip("date:T", title="Dia"), alt.Tooltip("qtd:Q", title="Entradas")]
        )
        text = base.mark_text(baseline="middle", fontSize=13, fontWeight=600, color="#E5E7EB").encode(
            x=alt.X("dow_label:N", sort=order_cols), y=alt.Y("week:O"), text=alt.Text("qtd:Q", format="d")
        )
        day_corner = base.mark_text(
            align="right", baseline="top", dx=-8, dy=6, fontSize=12, fontWeight=700, color="#FFFFFF"
        ).encode(
            x=alt.X("dow_label:N", sort=order_cols, bandPosition=1),
            y=alt.Y("week:O", bandPosition=0),
            text=alt.Text("dia:Q")
        )
        st.altair_chart(heat + text + day_corner, use_container_width=True)
    else:
        st.info("Sem datas válidas em 'data_primeiro_contato' para montar o calendário.")
else:
    st.info("Coluna 'data_primeiro_contato' não encontrada ou está vazia na base atual.")

st.divider()

# ============================================================
# 11) GRÁFICOS DE BARRAS – ECHARTS
# ============================================================
def _locked_slider_common():
    return {
        "handleSize": 0, "handleStyle": {"opacity": 0}, "showDetail": False, "brushSelect": False,
        "fillerColor": "rgba(255,255,255,0.18)", "backgroundColor": "rgba(255,255,255,0.06)",
        "borderColor": "rgba(255,255,255,0.15)",
    }

def echarts_horizontal_bar(labels, values, title=None, bar_color=ACCENT_GREEN):
    n = len(labels)
    if n == 0:
        st.info("Sem dados.")
        return

    window = min(6, n)
    start_idx = 0
    end_idx   = min(window - 1, n - 1)

    height_px = max(300, 38 * window)
    options = {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 0, "right": 96, "top": 6, "bottom": 6, "containLabel": True},
        "xAxis": {"type": "value","min": 0,"axisLabel": {"show": False},"axisLine": {"show": False},
                  "axisTick": {"show": False},"splitLine": {"show": False}},
        "yAxis": {"type": "category","data": labels,"inverse": True,
                  "axisLabel": {"color": "#E5E7EB", "margin": 8},
                  "axisLine": {"show": False},"axisTick": {"show": False}},
        "dataZoom": [
            {"type":"slider","yAxisIndex":0,"startValue":start_idx,"endValue":end_idx,
             "zoomLock":True,"minValueSpan":window,"maxValueSpan":window,"right":6,"width":12,**_locked_slider_common()},
            {"type":"inside","yAxisIndex":0,"startValue":start_idx,"endValue":end_idx,
             "zoomLock":True,"minValueSpan":window,"maxValueSpan":window},
        ],
        "series": [{
            "type": "bar","data": values,"barCategoryGap": "35%",
            "itemStyle": {"color": bar_color, "borderRadius": [0, 8, 8, 0]},
            "label": {"show": True, "position": "right", "color": "#FFFFFF","fontWeight": "bold","distance": 8},
        }],
    }
    if title:
        options["title"] = {"text": title, "left": 0, "textStyle": {"color": "#E5E7EB"}}
    st_echarts(options=options, height=f"{height_px}px", theme="dark")

def echarts_vertical_bar(labels, values, title=None, bar_color=ACCENT_GREEN):
    n = len(labels)
    if n == 0:
        st.info("Sem dados.")
        return

    window = min(4, n)
    start_idx = 0
    end_idx   = min(window - 1, n - 1)

    options = {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 40, "right": 20, "top": 16, "bottom": 80, "containLabel": True},
        "xAxis": {"type": "category", "data": labels, "axisLabel": {"color": "#E5E7EB", "interval": 0}},
        "yAxis": {
            "type": "value",
            "axisLabel": {"show": False}, "axisLine": {"show": False},
            "axisTick": {"show": False}, "splitLine": {"show": False},
        },
        "dataZoom": [
            {
                "type": "slider", "xAxisIndex": 0,
                "startValue": start_idx, "endValue": end_idx,
                "zoomLock": True, "minValueSpan": window, "maxValueSpan": window,
                "bottom": 24,
                **_locked_slider_common()
            },
            {
                "type": "inside", "xAxisIndex": 0,
                "startValue": start_idx, "endValue": end_idx,
                "zoomLock": True, "minValueSpan": window, "maxValueSpan": window,
            },
        ],
        "series": [{
            "type": "bar", "data": values,
            "itemStyle": {"color": bar_color, "borderRadius": [8, 8, 0, 0]},
            "label": {"show": True, "position": "top", "color": "#FFFFFF", "fontWeight": "bold"},
        }],
    }
    if title:
        options["title"] = {"text": title, "left": 0, "textStyle": {"color": "#E5E7EB"}}
    st_echarts(options=options, height="360px", theme="dark")

# ---------------------------
# 12) BARRAS LADO A LADO
# ---------------------------
bars_left, bars_right = st.columns(2, gap="large")

with bars_left:
    st.markdown("**Aldeia por Turma**")
    if not by_turma.empty:
        echarts_horizontal_bar(labels=by_turma["turma"].tolist(), values=by_turma["membros"].tolist())
        st.caption(f"Total de turmas: {len(by_turma)} • role para ver mais")
    else:
        st.info("Sem dados para montar o gráfico por Turma (após filtros e base).")

with bars_right:
    st.markdown("**Aldeia por Equipe**")
    if BROKER_COL and not by_broker.empty:
        echarts_vertical_bar(labels=by_broker["broker"].tolist(), values=by_broker["membros"].tolist())
        st.caption(f"Total de equipes: {len(by_broker)} • role para ver mais")
    elif not BROKER_COL:
        st.info("Coluna de Equipe não encontrada.")
    else:
        st.info("Sem dados para montar o gráfico por Equipe (após filtros e base).")

# ---------------------------
# 13) TABELA — Resumo por Equipe (2ª etapa)
# ---------------------------
st.subheader("📋 Resumo por Equipe")
if base_df.empty or not BROKER_COL:
    st.info("Sem registros ou coluna de Equipe ausente para montar a tabela.")
else:
    g = base_df.copy()
    equipe_norm = (
        g[BROKER_COL].fillna("Sem equipe").astype(str).str.strip()
         .replace({"": "Sem equipe", "nan": "Sem equipe", "None": "Sem equipe"})
    )

    def is_filled(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        s_low = s.str.lower()
        empty = s_low.isin(["", "nan", "none", "null", "nat"])
        return ~empty

    email_ok_tab = is_filled(g["email"])
    fin1_filled_tab = is_filled(g["finalizacao_primeira"])
    fin2_filled_tab = is_filled(g["finalizado_final"])

    hoje_tab = pd.Timestamp.now(tz="America/Sao_Paulo").tz_localize(None).normalize()
    target_tab = pd.to_datetime(g["target_sup"], errors="coerce")

    pend2_mask = (
        email_ok_tab
        & fin1_filled_tab
        & (~fin2_filled_tab)
        & target_tab.notna()
        & (target_tab >= hoje_tab)
    )

    atr2_mask = (
        email_ok_tab
        & fin1_filled_tab
        & (~fin2_filled_tab)
        & target_tab.notna()
        & (target_tab < hoje_tab)
    )

    fin2_ok = email_ok_tab & fin2_filled_tab

    base_tab = pd.DataFrame({
        "Equipe": equipe_norm,
        "Total de alunos": email_ok_tab.astype(int),
        "Pendentes Geral": pend2_mask.astype(int),
        "Atrasados Geral": atr2_mask.astype(int),
        "Finalizados Geral": fin2_ok.astype(int),
    })

    tabela_brokers = (
        base_tab.groupby("Equipe", as_index=False)
                .sum(numeric_only=True)
                .sort_values(
                    ["Atrasados Geral", "Pendentes Geral", "Total de alunos"],
                    ascending=[False, False, False]
                )
    )

    st.dataframe(tabela_brokers, use_container_width=True, hide_index=True)
    st.download_button(
        label="⬇️ Baixar CSV",
        data=tabela_brokers.to_csv(index=False).encode("utf-8"),
        file_name="resumo_equipes_2a_etapa.csv",
        mime="text/csv",
    )

st.sidebar.markdown("</div>", unsafe_allow_html=True)