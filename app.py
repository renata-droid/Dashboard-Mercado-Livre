import streamlit as st
import pandas as pd
import requests
import json
import time
import os
import io
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from auth import get_token

# =============================
# CONFIGURAÇÕES
# =============================
SELLER_ID = 1087616640
ADVERTISER_ID = "40004"
ADVERTISER_SITE_ID = "MLB"

BASE_SEARCH_URL = "https://api.mercadolibre.com/orders/search"
BASE_ORDER_URL = "https://api.mercadolibre.com/orders/{order_id}"
BASE_PAYMENT_URL = "https://api.mercadopago.com/v1/payments/{payment_id}"
BASE_BILLING_URL = "https://api.mercadolibre.com/billing/integration/group/ML/order/details"
BASE_SHIPMENT_COSTS_URL = "https://api.mercadolibre.com/shipments/{shipment_id}/costs"
BASE_ADS_SEARCH = "https://api.mercadolibre.com/advertising/{site}/advertisers/{advertiser}/product_ads/ads/search"
BASE_ADS_METRICS = "https://api.mercadolibre.com/advertising/{site}/product_ads/ads/{item_id}"

MAX_WORKERS = 6
LIMIT = 50
REQUEST_DELAY = 0.1

MAP_STATUS = {
    "paid": "aprovada",
    "approved": "aprovada",
    "cancelled": "cancelada",
    "refunded": "reembolsada",
}


# =============================
# UTILIDADES
# =============================
def request_com_retry(url, headers=None, params=None, max_tentativas=3, timeout=30):
    for tentativa in range(1, max_tentativas + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if tentativa < max_tentativas:
                time.sleep(2 ** tentativa)
            else:
                return None
    return None


def chunked(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]


# =============================
# PIPELINE FUNCTIONS
# =============================
def buscar_orders(session, data, token):
    """Busca todas as orders do dia com desconto de cupom"""
    data_inicio = f"{data}T00:00:00.000-03:00"
    data_fim = f"{data}T23:59:59.999-03:00"
    
    offset = 0
    todas_linhas = []
    
    while True:
        params = {
            "seller": SELLER_ID,
            "order.date_created.from": data_inicio,
            "order.date_created.to": data_fim,
            "limit": LIMIT,
            "offset": offset
        }
        
        r = request_com_retry(BASE_SEARCH_URL, params=params)
        if not r:
            break
            
        results = r.json().get("results", [])
        if not results:
            break
        
        order_ids = [o["id"] for o in results]
        
        for order_id in order_ids:
            headers = {"Authorization": f"Bearer {token}"}
            r = request_com_retry(BASE_ORDER_URL.format(order_id=order_id), headers=headers)
            if not r:
                continue
                
            order = r.json()
            
            pack_id = order.get("pack_id")
            sale_date = order.get("date_created")
            status_raw = order.get("status")
            status_gerencial = MAP_STATUS.get(status_raw, status_raw)
            shipment_id = (order.get("shipping") or {}).get("id")
            tracking_number = (order.get("shipping") or {}).get("receiver_id")
            
            desconto = 0
            for payment in order.get("payments", []):
                payment_id = payment.get("id")
                r_pay = request_com_retry(BASE_PAYMENT_URL.format(payment_id=payment_id), headers=headers)
                if r_pay:
                    for fee in r_pay.json().get("fee_details", []):
                        if fee.get("type") == "coupon_fee":
                            desconto += float(fee.get("amount", 0))
            
            for oi in order.get("order_items", []):
                item = oi.get("item", {})
                
                todas_linhas.append({
                    "order_id": str(order_id),
                    "pack_id": str(pack_id) if pack_id else None,
                    "sale_date": sale_date,
                    "status_raw": status_raw,
                    "status_gerencial": status_gerencial,
                    "item_id": str(item.get("id", "")),
                    "seller_sku": item.get("seller_sku", ""),
                    "quantity": int(oi.get("quantity", 1)),
                    "unit_price": float(oi.get("unit_price", 0)),
                    "discount_real": desconto,
                    "shipment_id": str(shipment_id) if shipment_id else None,
                    "tracking_number": tracking_number
                })
            
            time.sleep(REQUEST_DELAY)
        
        offset += LIMIT
    
    return todas_linhas


def buscar_billing(order_ids, token):
    """Busca taxas (sale_fee e charges)"""
    headers = {"Authorization": f"Bearer {token}"}
    billing_map = {}
    
    for lote in chunked(list(order_ids), 50):
        r = request_com_retry(
            BASE_BILLING_URL,
            headers=headers,
            params={"order_ids": ",".join(map(str, lote))}
        )
        
        if not r:
            continue
        
        for reg in r.json().get("results", []):
            order_id = str(reg.get("order_id"))
            
            sale_fee = reg.get("sale_fee") or {}
            charges = reg.get("charges") or []
            
            charge_amount = sum(c.get("detail_amount", 0) for c in charges)
            charge_desc = " | ".join(c.get("transaction_detail", "") for c in charges if c.get("transaction_detail"))
            
            billing_map[order_id] = {
                "sale_fee_net": float(sale_fee.get("net", 0)),
                "sale_fee_rebate": float(sale_fee.get("rebate", 0)),
                "charge_amount": float(charge_amount),
                "charge_description": charge_desc or None
            }
        
        time.sleep(REQUEST_DELAY)
    
    return billing_map


def buscar_items_com_ads(data, token):
    """Busca items que tiveram ads nesse dia"""
    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }
    
    item_ids = set()
    offset = 0
    
    while True:
        params = {
            "date_from": data,
            "date_to": data,
            "limit": 250,
            "offset": offset,
            "metrics": "clicks"
        }
        
        r = request_com_retry(
            BASE_ADS_SEARCH.format(site=ADVERTISER_SITE_ID, advertiser=ADVERTISER_ID),
            headers=headers,
            params=params
        )
        
        if not r:
            break
        
        results = r.json().get("results", [])
        if not results:
            break
        
        for item in results:
            item_ids.add(item["item_id"])
        
        if len(results) < 250:
            break
        
        offset += 250
        time.sleep(REQUEST_DELAY)
    
    return list(item_ids)


def buscar_ads_metrics(item_id, data, token):
    """Busca custo de ads para um item"""
    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }
    
    params = {
        "date_from": data,
        "date_to": data,
        "metrics": "clicks,prints,cost,direct_items_quantity,indirect_items_quantity,total_amount"
    }
    
    r = request_com_retry(
        BASE_ADS_METRICS.format(site=ADVERTISER_SITE_ID, item_id=item_id),
        headers=headers,
        params=params
    )
    
    if r:
        try:
            return r.json().get("results", [{}])[0]
        except:
            return {}
    return {}


def buscar_frete(shipment_id, token):
    """Busca custo de frete"""
    if not shipment_id:
        return 0
    
    headers = {"Authorization": f"Bearer {token}"}
    
    r = request_com_retry(
        BASE_SHIPMENT_COSTS_URL.format(shipment_id=shipment_id),
        headers=headers
    )
    
    if r:
        data = r.json()
        sender = (data.get("senders") or [{}])[0]
        return float(sender.get("cost") or 0)
    
    return 0


def aplicar_regra_frete(df):
    """Aplica a lógica complexa de frete por tracking"""
    df["frete_regra_calculada"] = df["frete_pago_vendedor"]
    
    for tracking, grupo in df.groupby("tracking_number"):
        if pd.isna(tracking):
            continue
        
        maiores = grupo[grupo["unit_price"] >= 79]
        menores = grupo[grupo["unit_price"] < 79]
        frete_total = grupo["frete_pago_vendedor"].max()
        
        if len(grupo) == 1:
            continue
        
        if len(maiores) == 1 and len(menores) >= 1:
            idx_maior = maiores.index[0]
            df.loc[idx_maior, "frete_regra_calculada"] = frete_total
            
            for idx in menores.index:
                df.loc[idx, "frete_regra_calculada"] = 0
        
        elif len(maiores) > 1:
            soma_maiores = maiores["unit_price"].sum()
            
            for idx, row in maiores.iterrows():
                rateio = frete_total * (row["unit_price"] / soma_maiores)
                df.loc[idx, "frete_regra_calculada"] = round(rateio, 2)
    
    return df


def gerar_consolidado(data):
    """Gera consolidado completo com 22 colunas"""
    
    token = get_token()
    session = requests.Session()
    
    # 1. Buscar orders
    linhas = buscar_orders(session, data, token)
    
    if not linhas:
        return None
    
    df = pd.DataFrame(linhas)
    
    # 2. Buscar billing
    order_ids = df["order_id"].unique()
    billing_map = buscar_billing(order_ids, token)
    
    for col in ["sale_fee_net", "sale_fee_rebate", "charge_amount", "charge_description"]:
        df[col] = df["order_id"].map(lambda x: billing_map.get(x, {}).get(col, 0 if col != "charge_description" else None))
    
    # 3. Buscar histórico de items com ads
    item_ids_com_ads = buscar_items_com_ads(data, token)
    
    # 4. Buscar métricas de ads
    ads_map = {}
    
    if item_ids_com_ads:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(buscar_ads_metrics, item_id, data, token): item_id
                for item_id in item_ids_com_ads
            }
            
            for future in as_completed(futures):
                item_id = futures[future]
                metricas = future.result()
                ads_map[item_id] = float(metricas.get("cost", 0))
    
    # 5. Buscar fretes
    frete_cache = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(buscar_frete, sid, token): sid
            for sid in df["shipment_id"].dropna().unique()
        }
        
        for future in as_completed(futures):
            sid = futures[future]
            frete_cache[sid] = future.result()
    
    df["frete_pago_vendedor"] = df["shipment_id"].map(frete_cache).fillna(0).astype(float)
    
    # 6. Aplicar regra de frete
    df = aplicar_regra_frete(df)
    
    # 7. Calcular valor bruto
    df["valor_bruto_item"] = df["quantity"] * df["unit_price"]
    
    # 8. Ads
    df["ads_total_item"] = df["item_id"].map(lambda x: ads_map.get(x, 0)).fillna(0).astype(float)
    
    total_qtd_por_item = df.groupby("item_id")["quantity"].transform("sum")
    df["ads_unitario"] = (df["ads_total_item"] / total_qtd_por_item).fillna(0).astype(float)
    df["ads_rateado"] = (df["ads_unitario"] * df["quantity"]).fillna(0).astype(float)
    
    # 9. Sale fee bruta
    df["sale_fee_bruta"] = df["sale_fee_net"] + df["sale_fee_rebate"]
    
    # 10. Valor líquido
    df["valor_liquido"] = (
        df["valor_bruto_item"]
        - df["discount_real"]
        - df["sale_fee_net"]
        - df["sale_fee_rebate"]
        - df["frete_regra_calculada"]
        - df["ads_rateado"]
    )
    
    # 11. Sale date only
    df["sale_date_only"] = pd.to_datetime(df["sale_date"]).dt.date
    
    # 12. Reordenar colunas
    colunas_ordem = [
        "pack_id", "order_id", "sale_date", "sale_date_only",
        "status_raw", "status_gerencial",
        "item_id", "seller_sku", "quantity", "unit_price",
        "valor_bruto_item", "discount_real", "tracking_number",
        "sale_fee_net", "sale_fee_bruta", "sale_fee_rebate",
        "charge_amount", "charge_description",
        "frete_regra_calculada", "ads_unitario", "ads_rateado",
        "valor_liquido"
    ]
    
    df = df[colunas_ordem]
    
    return df


# =============================
# STREAMLIT APP
# =============================
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

USERS = {"admin": "123", "gestor": "123"}

if "logado" not in st.session_state:
    st.session_state.logado = False
if "consolidado_data" not in st.session_state:
    st.session_state.consolidado_data = None

if not st.session_state.logado:
    st.subheader("🔐 Login")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        usuario = st.text_input("Usuário")
        senha = st.text_input("Senha", type="password")
        
        if st.button("Entrar", use_container_width=True):
            if usuario in USERS and USERS[usuario] == senha:
                st.session_state.logado = True
                st.rerun()
            else:
                st.error("❌ Usuário ou senha inválidos")

else:
    col_spacer, col_logout = st.columns([10, 1])
    with col_logout:
        if st.button("Logout", use_container_width=True):
            st.session_state.logado = False
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    ontem = datetime.today() - timedelta(days=1)
    data_selecionada = st.date_input(
        "📅 Selecione uma data para processar",
        value=ontem
    )
    
    st.divider()
    
    col1, col2 = st.columns([2, 3])
    
    with col1:
        if st.button("▶️ PROCESSAR", use_container_width=True):
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
    
    if st.session_state.consolidado_data is not None:
        df = st.session_state.consolidado_data
        
        st.subheader("📈 Resumo")
        
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
        
        st.subheader("⬇️ Exportar Consolidado")
        
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
        
        if st.checkbox("Exibir tabela completa"):
            st.subheader("📋 Dados Completos")
            st.dataframe(df, use_container_width=True, height=500)
    
    else:
        st.info("👆 Selecione uma data e clique em PROCESSAR para gerar o consolidado")
