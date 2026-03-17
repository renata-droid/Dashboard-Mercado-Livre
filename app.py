import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from pipeline_meli import pipeline

st.set_page_config(page_title="Dashboard Mercado Livre", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

st.markdown("""
<style>
.stApp {
    background: radial-gradient(circle at top, #1f2937, #020617);
    color: white;
}
h1, h2, h3 {
    color: white;
}
.stButton > button {
    background-color: #6d28d9;
    color: white;
    border-radius: 8px;
    border: none;
}
.stButton > button:hover {
    background-color: #7c3aed;
}
[data-testid="stMetricValue"] {
    color: #a855f7;
}
hr {
    border-color: #374151;
}
</style>
""", unsafe_allow_html=True)

col_logo, col_title = st.columns([1,6])
with col_logo:
    logo_path = os.path.join(BASE_DIR, "mercadolivre_logo.png")
    if os.path.exists(logo_path):
        st.image(logo_path, width=260)

with col_title:
    st.markdown("<h1 style='margin-top:10px;'>Dashboard Mercado Livre</h1>", unsafe_allow_html=True)

st.divider()

# CACHE
@st.cache_data
def carregar_consolidado(caminho):
    return pd.read_excel(caminho)

# LOGIN
USERS = {
    "admin": "123",
    "gestor": "123"
}

if "logado" not in st.session_state:
    st.session_state.logado = False

if not st.session_state.logado:
    st.subheader("Login")
    usuario = st.text_input("Usuário")
    senha = st.text_input("Senha", type="password")

    if st.button("Entrar"):
        if usuario in USERS and USERS[usuario] == senha:
            st.session_state.logado = True
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos")

else:
    # Logout
    col_spacer, col_logout = st.columns([10, 1])
    with col_logout:
        if st.button("Logout"):
            st.session_state.logado = False
            st.cache_data.clear()
            st.rerun()

    st.divider()

    # Período
    ontem = datetime.today() - timedelta(days=1)
    data_inicial, data_final = st.date_input("Período", value=(ontem, ontem))

    st.divider()

    # Botão de processamento
    if st.button("Executar Processamento", use_container_width=True):
        with st.spinner("⏳ Processando..."):
            try:
                data_atual = data_inicial
                while data_atual <= data_final:
                    pipeline(data_atual.strftime("%Y-%m-%d"))
                    data_atual += timedelta(days=1)
                st.cache_data.clear()
                st.success("✅ Processamento concluído!")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {str(e)}")

    st.divider()

    # Carregar dados
    dfs = []
    data_atual = data_inicial

    while data_atual <= data_final:
        arquivo = os.path.join(
            BASE_DIR, "data", "consolidado",
            f"consolidado_{data_atual.strftime('%Y-%m-%d')}.xlsx"
        )

        if os.path.exists(arquivo):
            df_temp = carregar_consolidado(arquivo)
            if df_temp is not None:
                dfs.append(df_temp)

        data_atual += timedelta(days=1)

    # Dashboard
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df["sale_date"] = pd.to_datetime(df["sale_date"])

        # KPIs
        faturamento = (df["quantity"] * df["unit_price"]).sum()
        margem_total = df["margem"].sum()
        pedidos = df["order_id"].nunique()
        ticket = faturamento / pedidos if pedidos > 0 else 0
        margem_pct_media = df["margem_pct"].mean()

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Faturamento", f"R$ {faturamento:,.2f}")
        col2.metric("Margem", f"R$ {margem_total:,.2f}")
        col3.metric("Pedidos", int(pedidos))
        col4.metric("Ticket Médio", f"R$ {ticket:,.2f}")
        col5.metric("Margem %", f"{margem_pct_media:.1f}%")

        st.divider()

        # Gráfico: Faturamento por dia
        vendas_dia = df.groupby(df["sale_date"].dt.date).agg({
            "quantity": "sum",
            "unit_price": lambda x: (x * df.loc[x.index, "quantity"]).sum() / df.loc[x.index, "quantity"].sum() if df.loc[x.index, "quantity"].sum() > 0 else 0,
            "margem": "sum"
        }).reset_index()
        vendas_dia.columns = ["data", "quantidade", "preco_medio", "margem"]
        vendas_dia["faturamento"] = vendas_dia["quantidade"] * vendas_dia["preco_medio"]

        fig1 = px.bar(vendas_dia, x="data", y="faturamento", text="faturamento",
                     title="Faturamento por Dia", color_discrete_sequence=["#7c3aed"])
        fig1.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
        fig1.update_layout(plot_bgcolor="#020617", paper_bgcolor="#020617", font=dict(color="white"))
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico: Top 10 produtos
        top_produtos = df.groupby("item_id")["margem"].sum().sort_values(ascending=False).head(10).reset_index()
        fig2 = px.bar(top_produtos, x="item_id", y="margem", text="margem",
                     title="Top 10 Produtos por Margem", color_discrete_sequence=["#7c3aed"])
        fig2.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
        fig2.update_layout(plot_bgcolor="#020617", paper_bgcolor="#020617", font=dict(color="white"))
        st.plotly_chart(fig2, use_container_width=True)

        # Tabela de dados
        st.subheader("Dados Completos")
        st.dataframe(df, use_container_width=True, height=500)

    else:
        st.warning("Nenhum relatório encontrado. Execute o processamento.")
