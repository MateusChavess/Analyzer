# ============================================================
# Analyzer – Presenciais (ALDEIA) | Streamlit (BigQuery + ECharts)
# ============================================================

import os
import hashlib
import pandas as pd
import streamlit as st
from streamlit_echarts import st_echarts
from google.cloud import bigquery
from google.oauth2 import service_account


# ---------------------------
# 1) CONFIG
# ---------------------------
PROJECT_ID = "leads-ts"
TABLE_FQN  = "`leads-ts.Analyzer.aldeia_presenciais_s`"

st.set_page_config(page_title="Analyzer – Presenciais (ALDEIA)", layout="wide")

ACCENT_GREEN = "#C9E34F"


# ---------------------------
# 2) SIDEBAR + CSS global
# ---------------------------
st.sidebar.markdown("""
<style>
[data-testid="stSidebar"] > div:first-child {
  padding-top: 0.75rem; padding-bottom: 0.75rem;
  height:100dvh; overflow:hidden; box-sizing:border-box;
}
div[data-testid="stSidebarNav"],
div[data-testid="stSidebarNavSearch"] { display:none !important; }

.sb-wrap{ display:flex; flex-direction:column; height:100%; }
.sb-box{
  border:1px solid rgba(255,255,255,.10);
  border-radius:12px; padding:10px; margin:8px 0 12px;
}
.sb-box .stButton{ margin:4px 0; }
.sb-box .stButton { display:none !important; }
.sb-box .nav-only .stButton { display:block !important; }

[data-testid="stSidebar"] .stTextInput,
[data-testid="stSidebar"] input[type="text"],
[data-testid="stSidebar"] [data-baseweb="input"] {
  display:none !important; height:0 !important; margin:0 !important;
  padding:0 !important; border:0 !important;
}

/* ===== KPI cards ===== */
.kpi-grid{
  display:grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 10px;                 /* ✅ MAIS PRÓXIMO */
  margin-top: 4px;
}
@media (max-width: 1100px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media (max-width: 640px)  { .kpi-grid { grid-template-columns: 1fr; } }

.kpi-card{
  border:1px solid rgba(255,255,255,.10);
  background: rgba(10, 20, 35, .35);
  border-radius:16px;
  padding:16px 18px;
  box-shadow:0 10px 30px rgba(0,0,0,.18);
  min-height: 86px;
  width: 100%;
}
.kpi-top{ display:flex; align-items:center; gap:10px; margin-bottom:6px; }
.kpi-icon{
  width:28px; height:28px; display:flex; align-items:center; justify-content:center;
  border-radius:10px;
  background: rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.08);
  font-size:14px;
}
.kpi-title{ font-size:13px; color: rgba(255,255,255,.80); }
.kpi-value-row{ display:flex; align-items:baseline; gap:10px; }
.kpi-value{ font-size:34px; font-weight:800; letter-spacing:.3px; color: rgba(255,255,255,.96); }
.kpi-pct{ font-size:13px; color: rgba(255,255,255,.70); font-weight:700; }
</style>
""", unsafe_allow_html=True)

st.sidebar.markdown('<div class="sb-wrap">', unsafe_allow_html=True)

# refresh
if "refresh_key" not in st.session_state:
    st.session_state.refresh_key = 0
if st.sidebar.button("🔄 Atualizar agora", use_container_width=True, key="btn_refresh_now_pres"):
    st.session_state.refresh_key += 1
    st.rerun()

st.sidebar.divider()

# navegação
with st.sidebar.container():
    st.sidebar.markdown('<div class="sb-box"><div class="nav-only">', unsafe_allow_html=True)

    if st.sidebar.button("🏠 Home", use_container_width=True, key="nav_home_btn_pres"):
        st.switch_page("pages/analyzer-aldeia.py")

    if st.sidebar.button("📈 Análise de Membros", use_container_width=True, key="nav_membros_btn_pres"):
        st.switch_page("pages/Analise-aldeia.py")

    if st.sidebar.button("⛔ Atrasados", use_container_width=True, key="nav_atrasados_btn_pres"):
        st.switch_page("pages/Atrasados-aldeia.py")

    st.sidebar.button("🎟️ Presenciais", use_container_width=True, key="nav_presenciais_btn_disabled", disabled=True)

    st.sidebar.markdown('</div></div>', unsafe_allow_html=True)

st.sidebar.divider()
_sb_auth_placeholder = st.sidebar.empty()
_sb_last_placeholder = st.sidebar.empty()


# ---------------------------
# 3) AUTENTICAÇÃO BIGQUERY
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
        bq = bigquery.Client(project=PROJECT_ID)
        auth_mode = "arquivo local"
except Exception as e:
    st.error("Falha ao criar o client do BigQuery. Verifique os secrets/JSON.")
    st.exception(e)
    st.stop()

_sb_auth_placeholder.caption(f"🔐 Modo de autenticação: {auth_mode}")


# ---------------------------
# 4) QUERY + CACHE
# ---------------------------
@st.cache_data(show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    job = bq.query(sql)
    return job.result().to_dataframe(create_bqstorage_client=False)

cache_bust = f"{st.session_state.refresh_key}"

sql = f"""
SELECT
  status_atraso,
  detalhamento_prox_passos,
  data_ultima_att,
  dias_na_aldeia,
  data_primeiro_contato,
  turma,
  titularidade,
  nome,
  email,
  telefone,
  observacao,
  conta_titular,
  validacao_titular,
  mt5_titular,
  id_titular,
  contrato_data_titular,
  bonus_titular,
  nome_adicional,
  email_adicional,
  telefone_adicional,
  id_adicional,
  bonus_adicional,
  finalizacao_1_etapa,
  cancelamento,
  broker,
  ingestion_time
FROM {TABLE_FQN}
ORDER BY ingestion_time DESC
-- cache_bust:{cache_bust}
"""

with st.spinner("Consultando BigQuery…"):
    df = run_query(sql)

if df.empty:
    st.warning("Nenhum registro encontrado na tabela de Presenciais.")
    st.stop()

last_updated_str = pd.Timestamp.now(tz="America/Sao_Paulo").strftime("%d/%m/%Y %H:%M:%S")
_sb_last_placeholder.caption(f"🕒 Última atualização: {last_updated_str}")


# ---------------------------
# 5) HASH LOCAL
# ---------------------------
def make_hash(email, phone):
    e = str(email or "").strip().lower()
    p = "".join([c for c in str(phone or "") if c.isdigit()])
    raw = f"{e}_{p}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:12] if raw != "_" else None

df["hash_id"] = [make_hash(e, p) for e, p in zip(df.get("email"), df.get("telefone"))]


# ---------------------------
# 6) HEADER (SEM botão de atualizar ao lado do Modo Tribo)
# ---------------------------
hdr_l, hdr_r = st.columns([8, 4])
with hdr_l:
    st.title("🎟️ Presenciais — Analyzer (ALDEIA)")
with hdr_r:
    if st.button("↩️ Modo Tribo", use_container_width=True, key="btn_voltar_tribo_pres"):
        st.switch_page("main.py")

st.divider()


# ---------------------------
# Helpers
# ---------------------------
def filled(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    return ~s.isin(["", "nan", "none", "null", "nat"])

def uniq_opts(df_: pd.DataFrame, col: str):
    if col not in df_.columns:
        return []
    s = df_[col].astype(str).str.strip()
    s = s[~s.str.lower().isin(["", "nan", "none", "null", "nat"])]
    return sorted(s.unique().tolist())

def fmt_int(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")

def fmt_pct(num: int, den: int) -> str:
    if not den:
        return "0.0%"
    return f"{(num/den)*100:.1f}%"

def kpi_card_html(title: str, value: int, pct: str, icon: str) -> str:
    return (
        f'<div class="kpi-card">'
        f'  <div class="kpi-top">'
        f'    <div class="kpi-icon">{icon}</div>'
        f'    <div class="kpi-title">{title}</div>'
        f'  </div>'
        f'  <div class="kpi-value-row">'
        f'    <div class="kpi-value">{fmt_int(value)}</div>'
        f'    <div class="kpi-pct">{pct}</div>'
        f'  </div>'
        f'</div>'
    )

# ===== ECHARTS =====
def _locked_slider_common():
    return {
        "handleSize": 0,
        "handleStyle": {"opacity": 0},
        "showDetail": False,
        "brushSelect": False,
        "fillerColor": "rgba(255,255,255,0.18)",
        "backgroundColor": "rgba(255,255,255,0.06)",
        "borderColor": "rgba(255,255,255,0.15)",
    }

def echarts_vertical_bar_dates(labels, values, title=None, bar_color=ACCENT_GREEN, window_default=14):
    n = len(labels)
    if n == 0:
        st.info("Sem dados.")
        return

    window = min(int(window_default), n)
    start_idx = max(n - window, 0)
    end_idx   = n - 1

    options = {
        "backgroundColor": "transparent",
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "grid": {"left": 40, "right": 20, "top": 16, "bottom": 80, "containLabel": True},
        "xAxis": {
            "type": "category",
            "data": labels,
            "axisLabel": {"color": "#E5E7EB", "interval": 0},
            "axisLine": {"show": False},
            "axisTick": {"show": False},
        },
        "yAxis": {
            "type": "value",
            "axisLabel": {"show": False},
            "axisLine": {"show": False},
            "axisTick": {"show": False},
            "splitLine": {"show": False},
            "min": 0,
        },
        "dataZoom": [
            {
                "type": "slider",
                "xAxisIndex": 0,
                "startValue": start_idx,
                "endValue": end_idx,
                "zoomLock": True,
                "minValueSpan": window,
                "maxValueSpan": window,
                "bottom": 24,
                **_locked_slider_common(),
            },
            {
                "type": "inside",
                "xAxisIndex": 0,
                "startValue": start_idx,
                "endValue": end_idx,
                "zoomLock": True,
                "minValueSpan": window,
                "maxValueSpan": window,
            },
        ],
        "series": [{
            "type": "bar",
            "data": values,
            "itemStyle": {"color": bar_color, "borderRadius": [8, 8, 0, 0]},
            "label": {"show": True, "position": "top", "color": "#FFFFFF", "fontWeight": "bold"},
        }],
    }
    if title:
        options["title"] = {"text": title, "left": 0, "textStyle": {"color": "#E5E7EB"}}

    st_echarts(options=options, height="360px", theme="dark")


# ---------------------------
# 7) FILTROS
# ---------------------------
turma_opts             = uniq_opts(df, "turma")
conta_titular_opts     = uniq_opts(df, "conta_titular")
validacao_titular_opts = uniq_opts(df, "validacao_titular")
mt5_titular_opts       = uniq_opts(df, "mt5_titular")
broker_opts            = uniq_opts(df, "broker")

st.session_state.setdefault("pres_turma", [])
st.session_state.setdefault("pres_conta_titular", [])
st.session_state.setdefault("pres_validacao_titular", [])
st.session_state.setdefault("pres_mt5_titular", [])
st.session_state.setdefault("pres_broker", [])

c1, c2, c3, c4, c5 = st.columns([2, 2, 2, 2, 2], gap="small")
with c1:
    turma_sel = st.multiselect("Turma", turma_opts, key="pres_turma", placeholder="Selecione as turmas")
with c2:
    conta_sel = st.multiselect("Conta Titular", conta_titular_opts, key="pres_conta_titular", placeholder="Selecione")
with c3:
    valid_sel = st.multiselect("Validação Titular", validacao_titular_opts, key="pres_validacao_titular", placeholder="Selecione")
with c4:
    mt5_sel = st.multiselect("MT5 Titular", mt5_titular_opts, key="pres_mt5_titular", placeholder="Selecione")
with c5:
    broker_sel = st.multiselect("Broker", broker_opts, key="pres_broker", placeholder="Selecione os brokers")

fdf = df.copy()

if turma_sel and "turma" in fdf.columns:
    fdf = fdf[fdf["turma"].astype(str).str.strip().isin(turma_sel)]
if conta_sel and "conta_titular" in fdf.columns:
    fdf = fdf[fdf["conta_titular"].astype(str).str.strip().isin(conta_sel)]
if valid_sel and "validacao_titular" in fdf.columns:
    fdf = fdf[fdf["validacao_titular"].astype(str).str.strip().isin(valid_sel)]
if mt5_sel and "mt5_titular" in fdf.columns:
    fdf = fdf[fdf["mt5_titular"].astype(str).str.strip().isin(mt5_sel)]
if broker_sel and "broker" in fdf.columns:
    fdf = fdf[fdf["broker"].astype(str).str.strip().isin(broker_sel)]

if fdf.empty:
    st.info("Sem registros para os filtros atuais.")
    st.stop()


# ---------------------------
# 8) KPIs (agora em GRID HTML com gap pequeno)
# ---------------------------
has_tel    = filled(fdf["telefone"]) if "telefone" in fdf.columns else pd.Series(False, index=fdf.index)
has_broker = filled(fdf["broker"])   if "broker" in fdf.columns else pd.Series(False, index=fdf.index)
has_fin1   = filled(fdf["finalizacao_1_etapa"]) if "finalizacao_1_etapa" in fdf.columns else pd.Series(False, index=fdf.index)
has_add    = filled(fdf["nome_adicional"]) if "nome_adicional" in fdf.columns else pd.Series(False, index=fdf.index)

base_tel_broker = has_tel & has_broker
total_base = int(base_tel_broker.sum())

q_finalizados   = int((base_tel_broker & has_fin1).sum())
q_pendentes     = int((base_tel_broker & (~has_fin1)).sum())
q_com_adicional = int((has_add & has_broker).sum())

cards_html = "".join([
    kpi_card_html("Membros com Equipe", total_base, "100.0%" if total_base else "0.0%", "👥"),
    kpi_card_html("Finalizados (1ª etapa)", q_finalizados, fmt_pct(q_finalizados, total_base), "✅"),
    kpi_card_html("Pendentes (1ª etapa)", q_pendentes, fmt_pct(q_pendentes, total_base), "⚠️"),
    kpi_card_html("Com adicional", q_com_adicional, fmt_pct(q_com_adicional, total_base), "➕"),
])

st.markdown(f'<div class="kpi-grid">{cards_html}</div>', unsafe_allow_html=True)

st.write("")
st.divider()   # ✅ fica SÓ a linha (sem wrapper que gerava a “forma solta”)


# ---------------------------
# 9) GRÁFICO: Entradas por dia (Data de 1º contato) — ECHARTS
# ---------------------------
st.subheader("📊 Entradas por dia (Data de 1º contato)")

if "data_primeiro_contato" not in fdf.columns:
    st.info("Coluna data_primeiro_contato não encontrada.")
else:
    d = pd.to_datetime(fdf["data_primeiro_contato"], errors="coerce").dt.normalize()

    tmp_day = fdf.copy()
    tmp_day["_dia"] = d
    tmp_day["_base"] = base_tel_broker.values if len(tmp_day) == len(base_tel_broker) else False

    entradas = (
        tmp_day[tmp_day["_base"] & tmp_day["_dia"].notna()]
        .groupby("_dia")
        .size()
        .sort_index()
    )

    if entradas.empty:
        st.info("Sem dados válidos em data_primeiro_contato para montar o gráfico.")
    else:
        all_days = pd.date_range(entradas.index.min(), entradas.index.max(), freq="D")
        entradas = entradas.reindex(all_days, fill_value=0)

        labels = [dt.strftime("%d/%m") for dt in all_days]
        values = entradas.astype(int).tolist()

        echarts_vertical_bar_dates(
            labels=labels,
            values=values,
            title=None,
            bar_color=ACCENT_GREEN,
            window_default=14,
        )

        st.caption(
            f"Período: {all_days.min().strftime('%d/%m/%Y')} → {all_days.max().strftime('%d/%m/%Y')} • role no slider para navegar"
        )

st.write("")
st.divider()


# ---------------------------
# 10) TABELA RESUMO POR EQUIPE (BROKER) + DOWNLOAD
# ---------------------------
st.subheader("📊 Resumo por equipe (Broker)")

tmp = fdf.copy()
tmp["_broker_norm"] = tmp["broker"].astype(str).str.strip().replace({"": "—"}) if "broker" in tmp.columns else "—"
tmp["_has_tel"] = filled(tmp["telefone"]) if "telefone" in tmp.columns else False
tmp["_has_broker"] = filled(tmp["broker"]) if "broker" in tmp.columns else False
tmp["_base"] = tmp["_has_tel"] & tmp["_has_broker"]
tmp["_fin1"] = filled(tmp["finalizacao_1_etapa"]) if "finalizacao_1_etapa" in tmp.columns else False

base_rows = tmp[tmp["_base"]].copy()

if base_rows.empty:
    st.info("Sem registros com Telefone + Broker para montar o resumo por equipe.")
else:
    resumo = (
        base_rows
        .groupby("_broker_norm", dropna=False)
        .agg(
            registros=("telefone", "size"),
            finalizados_1_etapa=("_fin1", "sum"),
        )
        .reset_index()
        .rename(columns={"_broker_norm": "broker"})
    )
    resumo["pendentes_1_etapa"] = resumo["registros"] - resumo["finalizados_1_etapa"]
    resumo = resumo.sort_values(["registros", "broker"], ascending=[False, True]).reset_index(drop=True)

    st.dataframe(resumo, use_container_width=True, hide_index=True)

    st.download_button(
        "⬇️ Baixar CSV (Resumo por Broker)",
        data=resumo.to_csv(index=False).encode("utf-8"),
        file_name="resumo_broker_presenciais_aldeia.csv",
        mime="text/csv",
    )

st.divider()


# ---------------------------
# 11) TABELA ESPELHO (curada) + DOWNLOAD (FILTRADO)
# ---------------------------
st.subheader("📋 Base (espelho do Google Sheets)")

base = fdf.copy()

has_add_tbl = filled(base["nome_adicional"]) if "nome_adicional" in base.columns else pd.Series(False, index=base.index)
has_fin1_tbl = filled(base["finalizacao_1_etapa"]) if "finalizacao_1_etapa" in base.columns else pd.Series(False, index=base.index)
has_cancel_tbl = filled(base["cancelamento"]) if "cancelamento" in base.columns else pd.Series(False, index=base.index)

base["adicional_flag"] = has_add_tbl.map(lambda x: "SIM" if x else "NÃO")
base["status_1_etapa"] = has_fin1_tbl.map(lambda x: "REALIZADO" if x else "PENDENTE")
base["status_cancelamento"] = has_cancel_tbl.map(lambda x: "CANCELADO" if x else "")

out_cols = [
    ("nome", "Nome"),
    ("email", "E-mail"),
    ("telefone", "Telefone"),
    ("dias_na_aldeia", "Dias na Aldeia"),
    ("turma", "Turma"),
    ("conta_titular", "Conta Titular"),
    ("validacao_titular", "Validação Titular"),
    ("mt5_titular", "MT5 Titular"),
    ("adicional_flag", "Adicional"),
    ("status_1_etapa", "Status 1ª etapa"),
    ("status_cancelamento", "Cancelamento"),
]

final_cols = [(c, lbl) for (c, lbl) in out_cols if c in base.columns]
espelho = base[[c for c, _ in final_cols]].rename(columns={c: lbl for c, lbl in final_cols})

st.dataframe(espelho, use_container_width=True, hide_index=True)

st.download_button(
    "⬇️ Baixar CSV (Base filtrada)",
    data=espelho.to_csv(index=False).encode("utf-8"),
    file_name="aldeia_presenciais_base_filtrada.csv",
    mime="text/csv",
)
