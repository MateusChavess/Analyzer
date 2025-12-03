import streamlit as st
from datetime import datetime

# --------- Config ---------
st.set_page_config(page_title="Login — Analyzer", page_icon="🔒", layout="centered")

# --------- CSS ---------
st.markdown("""
<style>
/* Fundo */
html, body, [data-testid="stAppViewContainer"] {
  background:
    radial-gradient(1200px 600px at 20% 10%, rgba(120,94,255,.16), transparent 60%),
    radial-gradient(1000px 500px at 90% 80%, rgba(147,51,234,.16), transparent 60%),
    linear-gradient(135deg, #0b1220 0%, #151a2b 60%, #141126 100%);
}

/* Header/decoration e padding */
header[data-testid="stHeader"] { display: none; }
[data-testid="stDecoration"] { display: none; }

/* >>> Move tudo um pouco mais pra cima <<< */
[data-testid="stAppViewContainer"] > .main { padding-top: 0 !important; }
section.main > div.block-container { padding-top: 8px !important; padding-bottom: 24px !important; }

/* Título / subtítulo brancos e mais próximos do topo */
.h-title, .h-sub { color:#fff !important; }
.h-title { text-align:center; font-weight:800; margin: 12px 0 6px; }
.h-sub   { text-align:center; opacity:1; margin: 0 0 14px; }

/* Card (o próprio st.form) */
[data-testid="stForm"]{
  width:100%; max-width:460px; margin:6px auto 0; /* aproxima do título */
  background:rgba(255,255,255,0.06);
  border:1px solid rgba(255,255,255,0.08);
  backdrop-filter: blur(10px);
  border-radius:18px; box-shadow:0 20px 60px rgba(0,0,0,.35);
  padding:24px 22px 18px 22px;
}

/* Labels / checkbox brancos */
.stTextInput label, .stCheckbox label { color:#ffffff !important; opacity:1 !important; font-weight:600; }

/* Inputs: moldura roxa + interior escuro, sem sobras brancas */
.stTextInput > div{
  background:#6d4aff !important; padding:2px !important;
  border-radius:14px !important; border:none !important;
  overflow:hidden !important; box-shadow:none !important;
}
.stTextInput > div:focus-within{
  background:#7c3aed !important;
  box-shadow:0 0 0 2px #7c3aed66 !important;
  border-radius:14px !important;
}
.stTextInput > div > div{ background:transparent !important; border:none !important; box-shadow:none !important; }
.stTextInput > div > div > input{
  background:#0f1424 !important; color:#fff !important;
  border:none !important; outline:none !important; border-radius:12px !important; padding:12px 12px !important;
  box-shadow:none !important;
}
.stTextInput > div > div > input::placeholder{ color:#e6e7ef !important; }
input:-webkit-autofill{ -webkit-box-shadow:0 0 0px 1000px #0f1424 inset !important; -webkit-text-fill-color:#fff !important; }

/* Botão do olho roxinho */
.stTextInput button{
  background:linear-gradient(90deg, #7c3aed, #6366f1) !important;
  color:#fff !important; border:none !important; outline:none !important; box-shadow:none !important;
}

/* Checkbox: texto branco + caixinha verde */
.stCheckbox label, .stCheckbox span, .stCheckbox p { color:#ffffff !important; opacity:1 !important; }
.stCheckbox input[type="checkbox"] { accent-color: #22c55e !important; }

/* Botões roxos */
.stButton > button,
div[data-testid="baseButton-primary"] > button,
div[data-testid="baseButton-secondary"] > button,
button[kind="primary"], button[kind="secondary"]{
  width:100%; border:none;
  background: linear-gradient(90deg, #7c3aed, #6366f1) !important;
  color:#fff !important; font-weight:800; padding:12px 14px;
  border-radius:12px; transition: transform .08s ease, filter .15s ease;
}
.stButton > button:hover,
div[data-testid="baseButton-primary"] > button:hover,
div[data-testid="baseButton-secondary"] > button:hover,
button[kind="primary"]:hover, button[kind="secondary"]:hover{
  transform: translateY(-1px); filter: brightness(1.08);
}

/* Divider e links */
.divider{ display:flex; align-items:center; gap:10px; margin:18px 0 12px; }
.divider-line{ height:1px; flex:1; background:rgba(255,255,255,.25); }
.divider-text{ font-size:.86rem; color:#fff; opacity:.95; }
.link{ color:#fff !important; text-decoration:none; font-weight:600; opacity:.95; }
.link:hover{ text-decoration:underline; }

/* ====== REMOVE o pill "Press Enter to submit form" (BaseWeb Tag) ====== */
.stTextInput [data-baseweb="tag"],
.stTextInput div[data-baseweb="tag"],
.stTextInput span[data-baseweb="tag"],
/* em algumas builds, o Tag fica logo após o base-input */
.stTextInput [data-baseweb="base-input"] + [data-baseweb="tag"] {
  display: none !important;
  visibility: hidden !important;
  width: 0 !important;
  height: 0 !important;
  padding: 0 !important;
  margin: 0 !important;
}
</style>
""", unsafe_allow_html=True)

# --------- Estado ---------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "user" not in st.session_state:
    st.session_state.user = None

# --------- UI ---------
st.markdown('<h2 class="h-title">Analyzer</h2>', unsafe_allow_html=True)
st.markdown('<p class="h-sub">Faça login para acessar seu painel</p>', unsafe_allow_html=True)

with st.form("login_form", clear_on_submit=False):
    user = st.text_input("Usuário", placeholder="seu.usuario")
    pwd  = st.text_input("Senha", type="password", placeholder="••••••••")

    c1, c2 = st.columns([1,1])
    with c1:
        remember = st.checkbox("Lembrar de mim", value=True)
    with c2:
        st.markdown('<div style="text-align:right;"><a class="link" href="#">Esqueci minha senha</a></div>', unsafe_allow_html=True)

    login_ok = st.form_submit_button("Entrar", use_container_width=True)

# Divider + sociais
st.markdown("""
<div class="divider">
  <div class="divider-line"></div>
  <div class="divider-text">ou continue com</div>
  <div class="divider-line"></div>
</div>
""", unsafe_allow_html=True)

cg, cgh = st.columns(2)
with cg:
    st.button("Google", key="gbtn", use_container_width=True)
with cgh:
    st.button("GitHub", key="ghbtn", use_container_width=True)

# --------- Lógica (demo) ---------
VALID = {"admin": "1234", "user": "senha"}
if login_ok:
    if user and pwd and user in VALID and VALID[user] == pwd:
        st.session_state.logged_in = True
        st.session_state.user = user
        if remember:
            st.session_state["remember_until"] = datetime.utcnow().isoformat()
        st.success("Login realizado com sucesso!")
        try:
            st.switch_page("main.py")
        except Exception:
            st.experimental_rerun()
    else:
        st.error("Usuário ou senha inválidos.")
