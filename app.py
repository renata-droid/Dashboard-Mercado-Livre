import streamlit as st
import pandas as pd
import plotly.express as px
import subprocess
import os
from datetime import datetime, timedelta

# CONFIG

st.set_page_config(page_title="Dashboard Mercado Livre", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ESTILO

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

.stProgress > div > div > div > div {
    background-color: #7c3aed;
}

hr {
    border-color: #374151;
}

</style>
""", unsafe_allow_html=True)

# HEADER

col_logo, col_title = st.columns([1,6])

with col_logo:

    logo_path = os.path.join(BASE_DIR, "mercadolivre_logo.png")

    if os.path.exists(logo_path):
        st.image(logo_path, width=260)

with col_title:
    st.markdown(
        "<h1 style='margin-top:10px;'>Dashboard Mercado Livre</h1>",
        unsafe_allow_html=True
    )

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

# DASHBOARD

else:

    ontem = datetime.today() - timedelta(days=1)

    data_inicial, data_final = st.date_input(
        "Período",
        value=(ontem, ontem)
    )

    st.divider()

    if st.button("Executar Processamento"):

        st.info("Iniciando processamento...")

        log_area = st.empty()

        cmd = [
            "python",
            "pipeline_meli.py",
            data_inicial.strftime("%Y-%m-%d"),
            data_final.strftime("%Y-%m-%d")
        ]

        processo = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        logs = []

        while True:

            linha = processo.stdout.readline()

            if not linha and processo.poll() is not None:
                break

            if linha:
                logs.append(linha.strip())
                log_area.code("\n".join(logs[-15:]))

        retorno = processo.poll()

        if retorno == 0:

            st.success("Processamento finalizado")

            st.cache_data.clear()

            st.rerun()

        else:

            st.error("Erro durante processamento")

    st.divider()

    dfs = []

    data_atual = data_inicial

    while data_atual <= data_final:

        arquivo = os.path.join(
            BASE_DIR,
            "data",
            "consolidado",
            f"consolidado_{data_atual}.xlsx"
        )

        if os.path.exists(arquivo):
            dfs.append(carregar_consolidado(arquivo))

        data_atual += timedelta(days=1)

    if dfs:

        df = pd.concat(dfs, ignore_index=True)

        df["sale_date"] = pd.to_datetime(df["sale_date"])

        df["receita"] = df["unit_price"] * df["quantity"]

        faturamento = df["receita"].sum()

        pedidos = df["order_id"].nunique()

        ticket = faturamento / pedidos if pedidos > 0 else 0

        receita_produto = df.groupby("item_id")["receita"].sum()

        receita_pedido = df.groupby("order_id")["receita"].sum().mean()

        produto_mais_vendido = (
            df.groupby("item_id")["quantity"]
            .sum()
            .sort_values(ascending=False)
            .idxmax()
        )

        top10 = receita_produto.sort_values(ascending=False).head(10).sum()
        pct_top10 = (top10 / faturamento) * 100 if faturamento > 0 else 0

        col1, col2, col3, col4, col5, col6 = st.columns(6)

        col1.metric("Faturamento", f"R$ {faturamento:,.2f}")
        col2.metric("Pedidos", pedidos)
        col3.metric("Ticket Médio", f"R$ {ticket:,.2f}")
        col4.metric("Receita por Pedido", f"R$ {receita_pedido:,.2f}")
        col5.metric("% Receita Top 10", f"{pct_top10:.2f}%")
        col6.metric("Produto Mais Vendido", produto_mais_vendido)

        st.divider()

        # FATURAMENTO POR DIA

        vendas_dia = (
            df.groupby(df["sale_date"].dt.date)["receita"]
            .sum()
            .reset_index()
        )

        fig = px.bar(
            vendas_dia,
            x="sale_date",
            y="receita",
            text="receita",
            title="Faturamento por Dia",
            color_discrete_sequence=["#7c3aed"]
        )

        fig.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )

        st.plotly_chart(fig, use_container_width=True)

        # TOP 10 PRODUTOS POR RECEITA

        top_produtos = (
            df.groupby("item_id")["receita"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )

        fig2 = px.bar(
            top_produtos,
            x="item_id",
            y="receita",
            text="receita",
            title="Top 10 Produtos por Receita",
            color_discrete_sequence=["#7c3aed"]
        )

        fig2.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )

        st.plotly_chart(fig2, use_container_width=True)

        # CURVA PARETO

        pareto = receita_produto.sort_values(ascending=False).reset_index()

        pareto.columns = ["item_id", "receita"]

        pareto["pct"] = pareto["receita"] / pareto["receita"].sum()

        pareto["pct_acum"] = pareto["pct"].cumsum()

        fig3 = px.bar(
            pareto.head(20),
            x="item_id",
            y="receita",
            title="Pareto de Produtos (80/20)",
            color_discrete_sequence=["#7c3aed"]
        )

        st.plotly_chart(fig3, use_container_width=True)

        # CURVA ABC

        st.subheader("Curva ABC Produtos")

        abc = receita_produto.sort_values(ascending=False).reset_index()

        abc.columns = ["item_id", "receita"]

        abc["pct"] = abc["receita"] / abc["receita"].sum()

        abc["pct_acum"] = abc["pct"].cumsum()

        def classe(p):

            if p <= 0.8:
                return "A"
            elif p <= 0.95:
                return "B"
            else:
                return "C"

        abc["classe"] = abc["pct_acum"].apply(classe)

        st.dataframe(abc.head(30))

    else:

        st.warning("Nenhum relatório encontrado. Execute o processamento.")

    st.divider()

    if st.button("Logout"):

        st.session_state.logado = False
        st.rerun()
