import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import time
from auth import renovar_token

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "consolidado")
os.makedirs(OUTPUT_DIR, exist_ok=True)

SELLER_ID = 1087616640
BASE_SEARCH_URL = "https://api.mercadolibre.com/orders/search"
BASE_ORDER_URL = "https://api.mercadolibre.com/orders/{order_id}"
BASE_PAYMENT_URL = "https://api.mercadopago.com/v1/payments/{payment_id}"
BASE_SHIPMENT_COSTS_URL = "https://api.mercadolibre.com/shipments/{shipment_id}/costs"
BASE_BILLING_URL = "https://api.mercadolibre.com/billing/integration/group/ML/order/details"

LIMIT = 50
MAX_WORKERS = 10


def request_retry(session, url, headers=None, params=None, tentativas=5):
    for i in range(tentativas):
        try:
            r = session.get(url, headers=headers, params=params, timeout=60)
            r.raise_for_status()
            return r
        except Exception as e:
            print(f"retry {i+1} erro:", e)
            time.sleep(2)
    raise Exception("falha api")


def buscar_orders(session, data):
    data_inicio = f"{data}T00:00:00.000-03:00"
    data_fim = f"{data}T23:59:59.999-03:00"
    offset = 0
    ids = []

    while True:
        params = {
            "seller": SELLER_ID,
            "order.date_created.from": data_inicio,
            "order.date_created.to": data_fim,
            "limit": LIMIT,
            "offset": offset
        }

        r = request_retry(session, BASE_SEARCH_URL, params=params)
        results = r.json().get("results", [])

        if not results:
            break

        ids.extend(o["id"] for o in results)
        offset += LIMIT

    return ids


def buscar_billing(session, order_ids):
    """Busca taxas da Mercado Livre"""
    if not order_ids:
        return {}
    
    billing_map = {}
    chunk = 50
    
    for i in range(0, len(order_ids), chunk):
        lote = order_ids[i:i+chunk]
        
        try:
            r = request_retry(
                session,
                BASE_BILLING_URL,
                params={"order_ids": ",".join(map(str, lote))}
            )
            
            for reg in r.json().get("results", []):
                order_id = str(reg.get("order_id"))
                sale_fee = reg.get("sale_fee") or {}
                
                billing_map[order_id] = {
                    "sale_fee": float(sale_fee.get("net", 0))
                }
        except:
            pass
    
    return billing_map


def processar_order(session, order_id, token, billing_map):
    try:
        r = request_retry(session, BASE_ORDER_URL.format(order_id=order_id))
        order = r.json()

        pack_id = order.get("pack_id")
        sale_date = order.get("date_created")
        shipment_id = (order.get("shipping") or {}).get("id")

        desconto = 0
        for payment in order.get("payments", []):
            payment_id = payment.get("id")
            pr = request_retry(session, BASE_PAYMENT_URL.format(payment_id=payment_id))
            for fee in pr.json().get("fee_details", []):
                if fee.get("type") == "coupon_fee":
                    desconto += float(fee.get("amount", 0))

        linhas = []
        for oi in order.get("order_items", []):
            item = oi.get("item", {})
            
            billing = billing_map.get(str(order_id), {"sale_fee": 0})

            linhas.append({
                "order_id": order_id,
                "pack_id": pack_id,
                "sale_date": sale_date,
                "item_id": item.get("id"),
                "seller_sku": item.get("seller_sku"),
                "quantity": oi.get("quantity"),
                "unit_price": oi.get("unit_price"),
                "discount_real": desconto,
                "shipment_id": shipment_id,
                "sale_fee": billing.get("sale_fee", 0)
            })

        return linhas
    
    except Exception as e:
        print(f"Erro ao processar order {order_id}: {e}")
        return []


def buscar_frete(session, shipment_id, token):
    if not shipment_id:
        return 0

    try:
        r = request_retry(
            session,
            BASE_SHIPMENT_COSTS_URL.format(shipment_id=shipment_id)
        )
        data = r.json()
        sender = (data.get("senders") or [{}])[0]
        return float(sender.get("cost") or 0)
    except:
        return 0


def pipeline(data):
    print("\n" + "="*60)
    print(f"Processando {data}")
    print("="*60)

    token = renovar_token()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    # 1. Buscar orders
    print("buscando orders...")
    order_ids = buscar_orders(session, data)
    
    if not order_ids:
        print("Nenhuma order encontrada")
        return
    
    print(f"✓ {len(order_ids)} orders encontradas")

    # 2. Buscar billing
    print("buscando billing...")
    billing_map = buscar_billing(session, order_ids)

    # 3. Processar orders
    print("processando orders...")
    linhas = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [
            executor.submit(processar_order, session, oid, token, billing_map)
            for oid in order_ids
        ]

        for f in as_completed(futures):
            linhas.extend(f.result())

    if not linhas:
        print("Nenhuma linha gerada")
        return

    df = pd.DataFrame(linhas)

    # 4. Buscar fretes
    print("buscando fretes...")
    fretes = {}

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = {
            executor.submit(buscar_frete, session, sid, token): sid
            for sid in df["shipment_id"].dropna().unique()
        }

        for f in as_completed(futures):
            fretes[futures[f]] = f.result()

    df["frete_pago_vendedor"] = df["shipment_id"].map(fretes).fillna(0)

    # 5. Calcular margem simples
    df["valor_bruto"] = df["quantity"] * df["unit_price"]
    df["margem"] = df["valor_bruto"] - df["discount_real"] - df["sale_fee"] - df["frete_pago_vendedor"]
    df["margem_pct"] = (df["margem"] / df["valor_bruto"] * 100).round(2)

    # Reordenar colunas
    colunas = [
        "order_id", "pack_id", "sale_date", "item_id", "seller_sku",
        "quantity", "unit_price", "valor_bruto", "discount_real", 
        "sale_fee", "frete_pago_vendedor", "margem", "margem_pct", "shipment_id"
    ]
    df = df[colunas]

    # 6. Salvar
    output = os.path.join(OUTPUT_DIR, f"consolidado_{data}.xlsx")
    df.to_excel(output, index=False)

    print(f"✓ Arquivo: {output}")
    print(f"✓ Linhas: {len(df)}")
    print("="*60 + "\n")


if __name__ == "__main__":
    import sys
    data = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y-%m-%d")
    inicio = time.time()
    pipeline(data)
    print(f"Tempo: {round(time.time() - inicio, 2)}s")
