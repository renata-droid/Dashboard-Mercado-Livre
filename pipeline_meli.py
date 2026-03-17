import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import time
from auth import renovar_token

# ===========================
# CONFIGURAÇÕES
# ===========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "consolidado_diario")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SELLER_ID = 1087616640

BASE_SEARCH_URL = "https://api.mercadolibre.com/orders/search"
BASE_ORDER_URL = "https://api.mercadolibre.com/orders/{order_id}"
BASE_PAYMENT_URL = "https://api.mercadopago.com/v1/payments/{payment_id}"
BASE_BILLING_URL = "https://api.mercadolibre.com/billing/integration/group/ML/order/details"
BASE_SHIPMENT_COSTS_URL = "https://api.mercadolibre.com/shipments/{shipment_id}/costs"

LIMIT = 50
MAX_WORKERS = 6
TIMEOUT = 30


# ===========================
# REQUEST COM RETRY
# ===========================
def request_retry(session, url, headers=None, params=None, tentativas=3):
    """Faz requisição com retry automático"""
    for i in range(tentativas):
        try:
            r = session.get(url, headers=headers, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.exceptions.Timeout:
            print(f"⏱️ Timeout na tentativa {i+1}/{tentativas}: {url}")
            if i < tentativas - 1:
                time.sleep(2 ** i)  # Backoff exponencial
        except requests.exceptions.ConnectionError as e:
            print(f"❌ Erro de conexão na tentativa {i+1}/{tentativas}: {e}")
            if i < tentativas - 1:
                time.sleep(2 ** i)
        except requests.exceptions.HTTPError as e:
            print(f"❌ Erro HTTP na tentativa {i+1}/{tentativas}: {e}")
            if i < tentativas - 1:
                time.sleep(2)
            else:
                raise
        except Exception as e:
            print(f"❌ Erro inesperado na tentativa {i+1}/{tentativas}: {e}")
            if i < tentativas - 1:
                time.sleep(2)
            else:
                raise
    
    raise Exception(f"Falha na API após {tentativas} tentativas: {url}")


# ===========================
# BUSCAR ORDERS
# ===========================
def buscar_orders(session, data, token):
    """Busca todas as orders de um dia específico"""
    
    data_inicio = f"{data}T00:00:00.000-03:00"
    data_fim = f"{data}T23:59:59.999-03:00"
    
    headers = {"Authorization": f"Bearer {token}"}
    offset = 0
    ids = []
    
    print(f"📦 Buscando orders para {data}...")
    
    while True:
        params = {
            "seller": SELLER_ID,
            "order.date_created.from": data_inicio,
            "order.date_created.to": data_fim,
            "limit": LIMIT,
            "offset": offset
        }
        
        try:
            r = request_retry(session, BASE_SEARCH_URL, headers=headers, params=params)
            results = r.json().get("results", [])
        except Exception as e:
            print(f"❌ Erro ao buscar orders: {e}")
            break
        
        if not results:
            break
        
        ids.extend(o["id"] for o in results)
        print(f"   ✓ {len(ids)} orders encontradas (offset: {offset})")
        offset += LIMIT
    
    print(f"✅ Total: {len(ids)} orders")
    return ids


# ===========================
# BUSCAR BILLING
# ===========================
def buscar_billing(session, order_ids, token):
    """Busca dados de faturamento (taxas) para as orders"""
    
    if not order_ids:
        return {}
    
    headers = {"Authorization": f"Bearer {token}"}
    billing_map = {}
    chunk = 50
    
    print(f"💰 Buscando billing para {len(order_ids)} orders...")
    
    for i in range(0, len(order_ids), chunk):
        lote = order_ids[i:i+chunk]
        
        try:
            r = request_retry(
                session,
                BASE_BILLING_URL,
                headers=headers,
                params={"order_ids": ",".join(map(str, lote))}
            )
            
            for reg in r.json().get("results", []):
                order_id = str(reg.get("order_id"))
                
                sale_fee = reg.get("sale_fee") or {}
                charges = reg.get("charges") or []
                
                billing_map[order_id] = {
                    "sale_fee_net": float(sale_fee.get("net", 0)),
                    "sale_fee_rebate": float(sale_fee.get("rebate", 0)),
                    "charge_amount": float(sum(c.get("detail_amount", 0) for c in charges))
                }
        except Exception as e:
            print(f"⚠️ Erro ao buscar billing do lote {i}: {e}")
    
    print(f"✅ Billing obtido para {len(billing_map)} orders")
    return billing_map


# ===========================
# PROCESSAR ORDER
# ===========================
def processar_order(session, order_id, token, billing_map):
    """Processa uma order individual extraindo dados relevantes"""
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        r = request_retry(session, BASE_ORDER_URL.format(order_id=order_id), headers=headers)
        order = r.json()
    except Exception as e:
        print(f"❌ Erro ao buscar order {order_id}: {e}")
        return []
    
    pack_id = order.get("pack_id")
    sale_date = order.get("date_created")
    shipment_id = (order.get("shipping") or {}).get("id")
    
    # Buscar desconto de cupom
    desconto = 0
    for payment in order.get("payments", []):
        payment_id = payment.get("id")
        
        if not payment_id:
            continue
        
        try:
            pr = request_retry(
                session,
                BASE_PAYMENT_URL.format(payment_id=payment_id),
                headers=headers
            )
            
            for fee in pr.json().get("fee_details", []):
                if fee.get("type") == "coupon_fee":
                    desconto += float(fee.get("amount", 0))
        except Exception as e:
            print(f"⚠️ Erro ao buscar pagamento {payment_id}: {e}")
    
    # Dados de billing
    billing = billing_map.get(str(order_id), {
        "sale_fee_net": 0,
        "sale_fee_rebate": 0,
        "charge_amount": 0
    })
    
    # Processar itens da order
    linhas = []
    for oi in order.get("order_items", []):
        item = oi.get("item", {})
        
        linhas.append({
            "order_id": str(order_id),
            "pack_id": str(pack_id) if pack_id else None,
            "sale_date": sale_date,
            "item_id": str(item.get("id", "")),
            "seller_sku": item.get("seller_sku", ""),
            "quantity": int(oi.get("quantity", 1)),
            "unit_price": float(oi.get("unit_price", 0)),
            "discount_real": float(desconto),
            "shipment_id": str(shipment_id) if shipment_id else None,
            "sale_fee_net": float(billing.get("sale_fee_net", 0)),
            "sale_fee_rebate": float(billing.get("sale_fee_rebate", 0)),
            "charge_amount": float(billing.get("charge_amount", 0))
        })
    
    return linhas


# ===========================
# BUSCAR FRETE
# ===========================
def buscar_frete(session, shipment_id, token):
    """Busca custo de envio para um shipment"""
    
    if not shipment_id:
        return 0.0
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        r = request_retry(
            session,
            BASE_SHIPMENT_COSTS_URL.format(shipment_id=shipment_id),
            headers=headers
        )
        
        data = r.json()
        sender = (data.get("senders") or [{}])[0]
        
        return float(sender.get("cost") or 0)
    except Exception as e:
        print(f"⚠️ Erro ao buscar frete {shipment_id}: {e}")
        return 0.0


# ===========================
# BUSCAR ADS
# ===========================
def buscar_ads(session, item_ids, data, token):
    """Busca custo de anúncios por item"""
    
    if not item_ids or len(item_ids) == 0:
        return {}
    
    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }
    
    ads_map = {}
    
    print(f"📢 Buscando dados de ads para {len(item_ids)} produtos...")
    
    for item_id in item_ids:
        url = f"https://api.mercadolibre.com/advertising/MLB/product_ads/ads/{item_id}"
        
        try:
            r = request_retry(
                session,
                url,
                headers=headers,
                params={
                    "date_from": data,
                    "date_to": data,
                    "metrics": "cost"
                },
                tentativas=2
            )
            
            results = r.json().get("results", [])
            if results:
                ads_map[item_id] = float(results[0].get("cost", 0))
            else:
                ads_map[item_id] = 0.0
        except Exception as e:
            # Silencioso - nem todas as amostras têm ads
            ads_map[item_id] = 0.0
    
    print(f"✅ Ads obtido para {len(ads_map)} produtos")
    return ads_map


# ===========================
# PIPELINE PRINCIPAL
# ===========================
def pipeline(data):
    """Pipeline completo de extração e processamento de dados"""
    
    print("\n" + "="*60)
    print(f"🚀 INICIANDO PIPELINE PARA {data}")
    print("="*60 + "\n")
    
    # Renovar token
    try:
        token = renovar_token()
    except Exception as e:
        print(f"❌ Erro ao renovar token: {e}")
        return
    
    # Criar sessão
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})
    
    # 1. BUSCAR ORDERS
    try:
        order_ids = buscar_orders(session, data, token)
        
        if not order_ids:
            print("⚠️ Nenhuma order encontrada para este período")
            return
    except Exception as e:
        print(f"❌ Erro ao buscar orders: {e}")
        return
    
    # 2. BUSCAR BILLING
    try:
        billing_map = buscar_billing(session, order_ids, token)
    except Exception as e:
        print(f"❌ Erro ao buscar billing: {e}")
        billing_map = {}
    
    # 3. PROCESSAR ORDERS (PARALELO)
    print(f"\n⚙️ Processando {len(order_ids)} orders...")
    linhas = []
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(processar_order, session, oid, token, billing_map)
                for oid in order_ids
            ]
            
            completed = 0
            for f in as_completed(futures):
                try:
                    resultado = f.result()
                    linhas.extend(resultado)
                    completed += 1
                    if completed % 10 == 0:
                        print(f"   ✓ {completed}/{len(order_ids)} orders processadas")
                except Exception as e:
                    print(f"❌ Erro ao processar order: {e}")
        
        print(f"✅ {completed}/{len(order_ids)} orders processadas com sucesso")
    except Exception as e:
        print(f"❌ Erro no processamento paralelo: {e}")
        return
    
    if not linhas:
        print("⚠️ Nenhuma linha de dados gerada")
        return
    
    df = pd.DataFrame(linhas)
    
    # 4. BUSCAR FRETES (PARALELO)
    print(f"\n🚚 Buscando fretes...")
    fretes = {}
    
    try:
        shipment_ids = df["shipment_id"].dropna().unique()
        
        if len(shipment_ids) > 0:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(buscar_frete, session, sid, token): sid
                    for sid in shipment_ids
                }
                
                for f in as_completed(futures):
                    try:
                        fretes[futures[f]] = f.result()
                    except Exception as e:
                        print(f"❌ Erro ao buscar frete: {e}")
            
            print(f"✅ Fretes obtidos para {len(fretes)} shipments")
        
        df["frete_regra_calculada"] = df["shipment_id"].map(fretes).fillna(0.0)
    except Exception as e:
        print(f"❌ Erro ao processar fretes: {e}")
        df["frete_regra_calculada"] = 0.0
    
    # 5. BUSCAR ADS
    try:
        item_ids = df["item_id"].unique()
        ads_map = buscar_ads(session, item_ids, data, token)
        df["ads_total_item"] = df["item_id"].map(ads_map).fillna(0.0)
    except Exception as e:
        print(f"❌ Erro ao buscar ads: {e}")
        df["ads_total_item"] = 0.0
    
    # 6. CALCULAR MÉTRICAS
    print(f"\n📊 Calculando métricas finais...")
    
    try:
        # Rateio de ads por quantidade
        total_qtd_por_item = df.groupby("item_id")["quantity"].transform("sum")
        df["ads_unitario"] = df.apply(
            lambda row: row["ads_total_item"] / row["quantity"] if row["quantity"] > 0 else 0,
            axis=1
        )
        df["ads_rateado"] = df["ads_unitario"] * df["quantity"]
        
        # Valor bruto
        df["valor_bruto_item"] = df["quantity"] * df["unit_price"]
        
        # Valor líquido
        df["valor_liquido"] = (
            df["valor_bruto_item"]
            - df["discount_real"]
            - df["sale_fee_net"]
            - df["sale_fee_rebate"]
            - df["frete_regra_calculada"]
            - df["ads_rateado"]
        )
        
        # Formatar data
        df["sale_date"] = pd.to_datetime(df["sale_date"])
        df["sale_date_only"] = df["sale_date"].dt.date
        
        print(f"✅ Métricas calculadas")
    except Exception as e:
        print(f"❌ Erro ao calcular métricas: {e}")
        return
    
    # 7. SALVAR ARQUIVO
    try:
        output_path = os.path.join(OUTPUT_DIR, f"consolidado_{data}.xlsx")
        df.to_excel(output_path, index=False)
        
        print(f"\n✅ Arquivo gerado com sucesso!")
        print(f"📁 Caminho: {output_path}")
        print(f"📊 Linhas: {len(df)}")
        print("="*60 + "\n")
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo: {e}")
        return


if __name__ == "__main__":
    # Descomentar apenas para testes locais
    pass
    # from datetime import datetime
    # data_teste = datetime.today().strftime("%Y-%m-%d")
    # pipeline(data_teste)
