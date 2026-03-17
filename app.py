import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from pipeline_meli import pipeline

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
    padding: 10px 20px;
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
col_logo, col_title = st.columns([1, 6])

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

        try:
            # Processar cada dia
            data_atual = data_inicial
            while data_atual <= data_final:
                data_str = data_atual.strftime("%Y-%m-%d")
                st.info(f"Processando: {data_str}")
                pipeline(data_str)
                data_atual += timedelta(days=1)
            
            st.cache_data.clear()
            st.success("Processamento finalizado")
            st.rerun()
        except Exception as e:
            st.error(f"Erro no processamento: {str(e)}")

    st.divider()

    dfs = []

    data_atual = data_inicial

    while data_atual <= data_final:

        arquivo = os.path.join(
            BASE_DIR,
            "data",
            "consolidado",
            f"consolidado_{data_atual.strftime('%Y-%m-%d')}.xlsx"
        )

        if os.path.exists(arquivo):
            df_temp = carregar_consolidado(arquivo)
            if df_temp is not None:
                dfs.append(df_temp)

        data_atual += timedelta(days=1)

    if dfs:

        df = pd.concat(dfs, ignore_index=True)

        df["sale_date"] = pd.to_datetime(df["sale_date"])

        # Usar valor_liquido se existir, senão calcular receita simples
        if "valor_liquido" in df.columns:
            faturamento = df["valor_liquido"].sum()
            receita_produto = df.groupby("item_id")["valor_liquido"].sum()
            receita_pedido = df.groupby("order_id")["valor_liquido"].sum().mean()
        else:
            df["receita"] = df["unit_price"] * df["quantity"]
            faturamento = df["receita"].sum()
            receita_produto = df.groupby("item_id")["receita"].sum()
            receita_pedido = df.groupby("order_id")["receita"].sum().mean()

        pedidos = df["order_id"].nunique()

        ticket = faturamento / pedidos if pedidos > 0 else 0

        # Produto mais vendido com tratamento robusto
        try:
            if len(df) > 0:
                vendas_por_produto = df.groupby("item_id")["quantity"].sum()
                produto_mais_vendido = vendas_por_produto.idxmax() if len(vendas_por_produto) > 0 else "N/A"
            else:
                produto_mais_vendido = "N/A"
        except:
            produto_mais_vendido = "N/A"

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

        # ANÁLISES ADICIONAIS
        col_status, col_margin = st.columns(2)
        
        with col_status:
            st.subheader("Status dos Pedidos")
            if "status_gerencial" in df.columns:
                status_count = df["status_gerencial"].value_counts()
                fig_status = px.pie(
                    values=status_count.values,
                    names=status_count.index,
                    title="Distribuição por Status"
                )
                fig_status.update_layout(
                    plot_bgcolor="#020617",
                    paper_bgcolor="#020617",
                    font=dict(color="white")
                )
                st.plotly_chart(fig_status, use_container_width=True)
        
        with col_margin:
            st.subheader("Análise de Custos")
            if all(col in df.columns for col in ["sale_fee_net", "frete_regra_calculada", "ads_rateado"]):
                custos_totais = {
                    "Taxa ML": df["sale_fee_net"].sum(),
                    "Frete": df["frete_regra_calculada"].sum(),
                    "Ads": df["ads_rateado"].sum()
                }
                fig_custos = px.bar(
                    x=list(custos_totais.keys()),
                    y=list(custos_totais.values()),
                    title="Total de Custos",
                    color_discrete_sequence=["#7c3aed"]
                )
                fig_custos.update_layout(
                    plot_bgcolor="#020617",
                    paper_bgcolor="#020617",
                    font=dict(color="white"),
                    xaxis_title="Tipo de Custo",
                    yaxis_title="Valor (R$)"
                )
                st.plotly_chart(fig_custos, use_container_width=True)

        st.divider()
        
        # FATURAMENTO POR DIA
        if "valor_liquido" in df.columns:
            vendas_dia = (
                df.groupby(df["sale_date"].dt.date)["valor_liquido"]
                .sum()
                .reset_index()
            )
            vendas_dia.columns = ["data", "valor"]
        else:
            vendas_dia = (
                df.groupby(df["sale_date"].dt.date)["receita"]
                .sum()
                .reset_index()
            )
            vendas_dia.columns = ["data", "valor"]

        fig = px.bar(
            vendas_dia,
            x="data",
            y="valor",
            text="valor",
            title="Faturamento por Dia",
            color_discrete_sequence=["#7c3aed"]
        )

        fig.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )

        fig.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white")
        )

        st.plotly_chart(fig, use_container_width=True)

        # TOP 10 PRODUTOS POR RECEITA
        top_produtos = receita_produto.sort_values(ascending=False).head(10).reset_index()
        top_produtos.columns = ["item_id", "valor"]

        fig2 = px.bar(
            top_produtos,
            x="item_id",
            y="valor",
            text="valor",
            title="Top 10 Produtos por Receita",
            color_discrete_sequence=["#7c3aed"]
        )

        fig2.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )

        fig2.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white")
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

        fig3.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white")
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

        st.dataframe(
            abc.head(30).style.set_properties(**{
                "background-color": "#020617",
                "color": "white",
                "border-color": "#374151"
            }),
            use_container_width=True
        )

    else:

        st.warning("Nenhum relatório encontrado. Execute o processamento.")

    st.divider()

    if st.button("Logout"):

        st.session_state.logado = False
        st.rerun()
