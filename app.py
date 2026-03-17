import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
from pipeline_meli import pipeline

# ===========================
# CONFIGURAÇÕES
# ===========================
st.set_page_config(page_title="Dashboard Mercado Livre", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "consolidado_diario")

# Criar diretório se não existir
os.makedirs(DATA_DIR, exist_ok=True)

# ===========================
# ESTILO
# ===========================
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
        font-weight: 600;
    }
    
    .stButton > button:hover {
        background-color: #7c3aed;
    }
    
    [data-testid="stMetricValue"] {
        color: #a855f7;
        font-size: 24px;
    }
    
    [data-testid="stMetricLabel"] {
        color: #9ca3af;
    }
    
    .stProgress > div > div > div > div {
        background-color: #7c3aed;
    }
    
    hr {
        border-color: #374151;
    }
    
    .dataframe {
        background-color: #020617 !important;
    }
</style>
""", unsafe_allow_html=True)

# ===========================
# HEADER
# ===========================
col_logo, col_title = st.columns([1, 6])

with col_logo:
    logo_path = os.path.join(BASE_DIR, "mercadolivre_logo.png")
    if os.path.exists(logo_path):
        st.image(logo_path, width=260)

with col_title:
    st.markdown(
        "<h1 style='margin-top:10px; margin-bottom:0px;'>Dashboard Mercado Livre</h1>",
        unsafe_allow_html=True
    )

st.divider()

# ===========================
# CACHE
# ===========================
@st.cache_data
def carregar_consolidado(caminho):
    """Carrega arquivo Excel com cache"""
    try:
        return pd.read_excel(caminho)
    except Exception as e:
        st.error(f"Erro ao carregar arquivo: {e}")
        return None


# ===========================
# AUTENTICAÇÃO
# ===========================
USERS = {
    "admin": "123",
    "gestor": "123"
}

if "logado" not in st.session_state:
    st.session_state.logado = False

if not st.session_state.logado:
    st.subheader("🔐 Login")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col2:
        usuario = st.text_input("Usuário", key="usuario_input")
        senha = st.text_input("Senha", type="password", key="senha_input")
        
        if st.button("Entrar", use_container_width=True):
            if usuario in USERS and USERS[usuario] == senha:
                st.session_state.logado = True
                st.rerun()
            else:
                st.error("❌ Usuário ou senha inválidos")

# ===========================
# DASHBOARD
# ===========================
else:
    
    # Botão de logout no topo
    col_spacer, col_logout = st.columns([10, 1])
    with col_logout:
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logado = False
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    # Seleção de período
    ontem = datetime.today() - timedelta(days=1)
    
    col_date, col_info = st.columns([3, 2])
    
    with col_date:
        data_inicial, data_final = st.date_input(
            "📅 Selecione o período",
            value=(ontem, ontem),
            key="date_range"
        )
    
    with col_info:
        dias_selecionados = (data_final - data_inicial).days + 1
        st.info(f"📊 {dias_selecionados} dia(s) selecionado(s)")
    
    st.divider()
    
    # Processamento
    col_process, col_status = st.columns([2, 3])
    
    with col_process:
        if st.button("▶️ Executar Processamento", use_container_width=True):
            with st.spinner("⏳ Processando..."):
                try:
                    # Processar cada dia
                    data_atual = data_inicial
                    dias_processados = 0
                    
                    while data_atual <= data_final:
                        # Converter para string de forma segura
                        if isinstance(data_atual, datetime):
                            data_str = data_atual.strftime("%Y-%m-%d")
                        else:
                            data_str = str(data_atual)
                        
                        st.info(f"Processando: {data_str}")
                        pipeline(data_str)
                        dias_processados += 1
                        data_atual += timedelta(days=1)
                    
                    # Limpar cache ANTES de rerun
                    st.cache_data.clear()
                    
                    st.success(f"✅ {dias_processados} dia(s) processado(s) com sucesso!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Erro no processamento: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc(), language="python")
    
    st.divider()
    
    # Carregar dados
    dfs = []
    data_atual = data_inicial
    
    while data_atual <= data_final:
        arquivo = os.path.join(
            DATA_DIR,
            f"consolidado_{data_atual.strftime('%Y-%m-%d')}.xlsx"
        )
        
        if os.path.exists(arquivo):
            df_temp = carregar_consolidado(arquivo)
            if df_temp is not None:
                dfs.append(df_temp)
        
        data_atual += timedelta(days=1)
    
    # ===========================
    # EXIBIR DASHBOARD
    # ===========================
    if dfs:
        # Concatenar dados
        df = pd.concat(dfs, ignore_index=True)
        
        # Converter datas
        df["sale_date"] = pd.to_datetime(df["sale_date"])
        
        # Calcular métricas
        df["receita"] = df["unit_price"] * df["quantity"]
        
        faturamento = df["valor_liquido"].sum() if "valor_liquido" in df.columns else df["receita"].sum()
        pedidos = df["order_id"].nunique()
        ticket = faturamento / pedidos if pedidos > 0 else 0
        
        receita_produto = df.groupby("item_id")["receita"].sum()
        receita_pedido = df.groupby("order_id")["receita"].sum().mean()
        
        # Produto mais vendido
        produto_mais_vendido = "N/A"
        try:
            if len(df) > 0 and "item_id" in df.columns:
                vendas_por_produto = df.groupby("item_id")["quantity"].sum()
                if len(vendas_por_produto) > 0:
                    produto_mais_vendido = vendas_por_produto.idxmax()
        except Exception as e:
            print(f"Erro ao calcular produto mais vendido: {e}")
            produto_mais_vendido = "N/A"
        
        top10 = receita_produto.sort_values(ascending=False).head(10).sum()
        pct_top10 = (top10 / faturamento * 100) if faturamento > 0 else 0
        
        # Exibir KPIs
        st.subheader("📊 Indicadores Principais")
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("💰 Faturamento", f"R$ {faturamento:,.2f}")
        
        with col2:
            st.metric("📦 Pedidos", f"{pedidos:,}")
        
        with col3:
            st.metric("🎯 Ticket Médio", f"R$ {ticket:,.2f}")
        
        with col4:
            st.metric("📈 Receita/Pedido", f"R$ {receita_pedido:,.2f}")
        
        with col5:
            st.metric("🔝 % Top 10", f"{pct_top10:.1f}%")
        
        with col6:
            st.metric("⭐ Produto #1", produto_mais_vendido[:10])
        
        st.divider()
        
        # ===========================
        # GRÁFICO 1: FATURAMENTO POR DIA
        # ===========================
        st.subheader("📈 Faturamento por Dia")
        
        vendas_dia = (
            df.groupby(df["sale_date"].dt.date)["receita"]
            .sum()
            .reset_index()
        )
        vendas_dia.columns = ["data", "receita"]
        
        fig1 = px.bar(
            vendas_dia,
            x="data",
            y="receita",
            text="receita",
            title="Vendas Diárias",
            color_discrete_sequence=["#7c3aed"]
        )
        
        fig1.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )
        
        fig1.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white"),
            xaxis_title="Data",
            yaxis_title="Receita (R$)",
            hovermode="x unified"
        )
        
        st.plotly_chart(fig1, use_container_width=True)
        
        st.divider()
        
        # ===========================
        # GRÁFICO 2: TOP 10 PRODUTOS
        # ===========================
        st.subheader("🏆 Top 10 Produtos por Receita")
        
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
            title="Produtos Mais Lucrativos",
            color_discrete_sequence=["#7c3aed"]
        )
        
        fig2.update_traces(
            texttemplate='R$ %{text:,.0f}',
            textposition='outside'
        )
        
        fig2.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white"),
            xaxis_title="ID do Produto",
            yaxis_title="Receita (R$)",
            hovermode="x unified"
        )
        
        st.plotly_chart(fig2, use_container_width=True)
        
        st.divider()
        
        # ===========================
        # GRÁFICO 3: CURVA PARETO
        # ===========================
        st.subheader("📊 Curva Pareto (80/20)")
        
        pareto = receita_produto.sort_values(ascending=False).reset_index()
        pareto.columns = ["item_id", "receita"]
        pareto["pct"] = pareto["receita"] / pareto["receita"].sum()
        pareto["pct_acum"] = pareto["pct"].cumsum()
        
        fig3 = px.bar(
            pareto.head(20),
            x="item_id",
            y="receita",
            title="20% dos Produtos Geram 80% da Receita",
            color_discrete_sequence=["#7c3aed"]
        )
        
        fig3.update_layout(
            plot_bgcolor="#020617",
            paper_bgcolor="#020617",
            font=dict(color="white"),
            xaxis_title="ID do Produto",
            yaxis_title="Receita (R$)"
        )
        
        st.plotly_chart(fig3, use_container_width=True)
        
        st.divider()
        
        # ===========================
        # CURVA ABC
        # ===========================
        st.subheader("🔀 Classificação ABC de Produtos")
        
        abc = receita_produto.sort_values(ascending=False).reset_index()
        abc.columns = ["item_id", "receita"]
        abc["pct"] = abc["receita"] / abc["receita"].sum()
        abc["pct_acum"] = abc["pct"].cumsum()
        
        def classificar_abc(pct_acum):
            if pct_acum <= 0.80:
                return "🔴 A (80%)"
            elif pct_acum <= 0.95:
                return "🟡 B (15%)"
            else:
                return "🟢 C (5%)"
        
        abc["classe"] = abc["pct_acum"].apply(classificar_abc)
        abc["receita_fmt"] = abc["receita"].apply(lambda x: f"R$ {x:,.2f}")
        abc["pct_fmt"] = (abc["pct"] * 100).apply(lambda x: f"{x:.2f}%")
        abc["pct_acum_fmt"] = (abc["pct_acum"] * 100).apply(lambda x: f"{x:.2f}%")
        
        # Reordenar colunas para exibição
        abc_display = abc[["item_id", "receita_fmt", "pct_fmt", "pct_acum_fmt", "classe"]].copy()
        abc_display.columns = ["Produto ID", "Receita", "% Receita", "% Acumulado", "Classe"]
        
        st.dataframe(
            abc_display.head(30),
            use_container_width=True,
            height=400
        )
        
        # Resumo ABC
        st.subheader("📋 Resumo por Classe")
        col_a, col_b, col_c = st.columns(3)
        
        qtd_a = len(abc[abc["pct_acum"] <= 0.80])
        qtd_b = len(abc[(abc["pct_acum"] > 0.80) & (abc["pct_acum"] <= 0.95)])
        qtd_c = len(abc[abc["pct_acum"] > 0.95])
        
        with col_a:
            st.success(f"🔴 **Classe A**: {qtd_a} produtos")
        with col_b:
            st.warning(f"🟡 **Classe B**: {qtd_b} produtos")
        with col_c:
            st.info(f"🟢 **Classe C**: {qtd_c} produtos")
        
        st.divider()
        
        # Tabela de dados brutos (opcional)
        if st.checkbox("📋 Exibir dados brutos completos"):
            st.subheader("Dados Completos")
            st.dataframe(
                df.drop(columns=["pack_id", "seller_sku"] if "seller_sku" in df.columns else []),
                use_container_width=True,
                height=500
            )
    
    else:
        st.warning("⚠️ Nenhum relatório encontrado para o período selecionado.")
        st.info("💡 Execute o processamento para gerar os dados.")
