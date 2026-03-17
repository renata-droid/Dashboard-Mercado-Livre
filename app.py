import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
from pipeline_consolidado import gerar_consolidado
import io

st.set_page_config(page_title="Dashboard Mercado Livre", layout="wide")

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
    font-size: 28px;
}
hr {
    border-color: #374151;
}
</style>
""", unsafe_allow_html=True)

st.title("📊 Dashboard Mercado Livre")

# LOGIN
USERS = {"admin": "123", "gestor": "123"}

if "logado" not in st.session_state:
    st.session_state.logado = False
if "consolidado_data" not in st.session_state:
    st.session_state.consolidado_data = None

if not st.session_state.logado:
    st.subheader("🔐 Login")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        usuario = st.text_input("Usuário", key="user_input")
        senha = st.text_input("Senha", type="password", key="pass_input")
        
        if st.button("Entrar", use_container_width=True):
            if usuario in USERS and USERS[usuario] == senha:
                st.session_state.logado = True
                st.rerun()
            else:
                st.error("❌ Usuário ou senha inválidos")

else:
    # LOGOUT
    col_spacer, col_logout = st.columns([10, 1])
    with col_logout:
        if st.button("Logout", use_container_width=True):
            st.session_state.logado = False
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    # SELEÇÃO DE PERÍODO
    ontem = datetime.today() - timedelta(days=1)
    data_selecionada = st.date_input(
        "📅 Selecione uma data para processar",
        value=ontem,
        key="data_input"
    )
    
    st.divider()
    
    # BOTÃO PROCESSAR
    col1, col2 = st.columns([2, 3])
    
    with col1:
        if st.button("▶️ PROCESSAR", use_container_width=True, key="btn_processar"):
            with st.spinner("⏳ Gerando consolidado..."):
                try:
                    data_str = data_selecionada.strftime("%Y-%m-%d")
                    df_consolidado = gerar_consolidado(data_str)
                    
                    if df_consolidado is not None:
                        st.session_state.consolidado_data = df_consolidado
                        st.success(f"✅ Consolidado gerado para {data_str}!")
                    else:
                        st.error("❌ Nenhum dado encontrado para essa data")
                
                except Exception as e:
                    st.error(f"❌ Erro: {str(e)}")
    
    st.divider()
    
    # EXIBIR DADOS SE HOUVER
    if st.session_state.consolidado_data is not None:
        df = st.session_state.consolidado_data
        
        # BIG NUMBERS
        st.subheader("📈 Resumo do Período")
        
        valor_bruto = df["valor_bruto_item"].sum()
        valor_liquido = df["valor_liquido"].sum()
        pedidos = df["order_id"].nunique()
        tickets = df.groupby("order_id")["valor_bruto_item"].sum().mean()
        margem_pct = (valor_liquido / valor_bruto * 100) if valor_bruto > 0 else 0
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        col1.metric("💰 Faturamento Bruto", f"R$ {valor_bruto:,.2f}")
        col2.metric("💵 Faturamento Líquido", f"R$ {valor_liquido:,.2f}")
        col3.metric("📦 Pedidos", int(pedidos))
        col4.metric("🎯 Ticket Médio", f"R$ {tickets:,.2f}")
        col5.metric("📊 Margem %", f"{margem_pct:.1f}%")
        
        st.divider()
        
        # BOTÃO DOWNLOAD
        st.subheader("⬇️ Exportar Consolidado")
        
        # Converter para Excel em memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Consolidado')
        
        output.seek(0)
        
        data_str = data_selecionada.strftime("%Y-%m-%d")
        
        st.download_button(
            label="📥 Baixar Consolidado (XLSX)",
            data=output.getvalue(),
            file_name=f"consolidado_{data_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
        
        st.divider()
        
        # TABELA (OPCIONAL)
        if st.checkbox("Exibir tabela completa"):
            st.subheader("📋 Dados Completos")
            st.dataframe(df, use_container_width=True, height=500)
    
    else:
        st.info("👆 Selecione uma data e clique em PROCESSAR para gerar o consolidado")
