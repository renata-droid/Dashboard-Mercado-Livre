import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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
BASE_BILLING_URL = "https://api.mercadolibre.com/billing/integration/group/ML/order/details"
BASE_SHIPMENT_COSTS_URL = "https://api.mercadolibre.com/shipments/{shipment_id}/costs"

LIMIT = 50
MAX_WORKERS = 10

# Status mapping
STATUS_MAP = {
    "paid": "aprovada",
    "pending": "pendente",
    "cancelled": "cancelada",
    "refunded": "reembolsada"
}


def request_retry(session, url, headers=None, params=None, tentativas=5):
    """Faz requisição com retry automático"""
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
    """Busca todas as orders de um dia específico"""
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
    """Busca dados de billing (taxas) para as orders"""
    
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
                charges = reg.get("charges") or []
                
                # Extrair descrição da taxa
                charge_desc = None
                if charges:
                    charge_desc = charges[0].get("detail_description", "")
                
                billing_map[order_id] = {
                    "sale_fee_net": float(sale_fee.get("net", 0)),
                    "sale_fee_bruta": float(sale_fee.get("net", 0)),  # Mesmo valor
                    "sale_fee_rebate": float(sale_fee.get("rebate", 0)),
                    "charge_amount": float(sum(c.get("detail_amount", 0) for c in charges)),
                    "charge_description": charge_desc
                }
        except Exception as e:
            print(f"Erro ao buscar billing do lote {i}: {e}")
    
    return billing_map


def processar_order(session, order_id, token, billing_map):
    """Processa uma order individual extraindo dados relevantes"""
    try:
        r = request_retry(session, BASE_ORDER_URL.format(order_id=order_id))
        order = r.json()

        pack_id = order.get("pack_id")
        sale_date = order.get("date_created")
        status_raw = order.get("status")
        status_gerencial = STATUS_MAP.get(status_raw, status_raw)
        
        shipment_id = (order.get("shipping") or {}).get("id")
        tracking_number = (order.get("shipping") or {}).get("receiver_id")

        desconto = 0

        for payment in order.get("payments", []):
            payment_id = payment.get("id")

            pr = request_retry(
                session,
                BASE_PAYMENT_URL.format(payment_id=payment_id)
            )

            for fee in pr.json().get("fee_details", []):
                if fee.get("type") == "coupon_fee":
                    desconto += float(fee.get("amount", 0))

        linhas = []

        for oi in order.get("order_items", []):
            item = oi.get("item", {})
            
            # Dados de billing
            billing = billing_map.get(str(order_id), {
                "sale_fee_net": 0,
                "sale_fee_bruta": 0,
                "sale_fee_rebate": 0,
                "charge_amount": 0,
                "charge_description": None
            })

            linhas.append({
                "pack_id": pack_id,
                "order_id": order_id,
                "sale_date": sale_date,
                "sale_date_only": sale_date[:10] if sale_date else None,
                "status_raw": status_raw,
                "status_gerencial": status_gerencial,
                "item_id": item.get("id"),
                "seller_sku": item.get("seller_sku"),
                "quantity": oi.get("quantity"),
                "unit_price": oi.get("unit_price"),
                "valor_bruto_item": oi.get("quantity", 0) * oi.get("unit_price", 0),
                "discount_real": desconto,
                "tracking_number": tracking_number,
                "sale_fee_net": billing.get("sale_fee_net", 0),
                "sale_fee_bruta": billing.get("sale_fee_bruta", 0),
                "sale_fee_rebate": billing.get("sale_fee_rebate", 0),
                "charge_amount": billing.get("charge_amount", 0),
                "charge_description": billing.get("charge_description"),
                "frete_regra_calculada": 0,  # Será preenchido depois
                "ads_unitario": 0,  # Será preenchido depois
                "ads_rateado": 0,  # Será preenchido depois
                "valor_liquido": 0,  # Será preenchido depois
                "shipment_id": shipment_id
            })

        return linhas
    
    except Exception as e:
        print(f"Erro ao processar order {order_id}: {e}")
        return []


def buscar_frete(session, shipment_id, token):
    """Busca custo de envio para um shipment"""
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
    
    except Exception as e:
        print(f"Erro ao buscar frete {shipment_id}: {e}")
        return 0


def buscar_ads(session, item_ids, data, token):
    """Busca custo de anúncios por item"""
    
    if not item_ids or len(item_ids) == 0:
        return {}
    
    ads_map = {}
    
    for item_id in item_ids:
        url = f"https://api.mercadolibre.com/advertising/MLB/product_ads/ads/{item_id}"
        
        try:
            r = request_retry(
                session,
                url,
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
        except:
            ads_map[item_id] = 0.0
    
    return ads_map


def pipeline(data):
    """Pipeline completo de extração e processamento de dados"""
    
    print("\n" + "="*60)
    print(f"Iniciando processamento para {data}")
    print("="*60)

    token = renovar_token()
    
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    print("buscando orders...")
    order_ids = buscar_orders(session, data)
    
    if not order_ids:
        print("⚠️ Nenhuma order encontrada para este período")
        return
    
    print(f"✓ {len(order_ids)} orders encontradas")

    # Buscar billing antes de processar
    print("buscando billing...")
    billing_map = buscar_billing(session, order_ids)
    print(f"✓ Billing obtido para {len(billing_map)} orders")

    print("processando orders...")
    linhas = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [
            executor.submit(processar_order, session, oid, token, billing_map)
            for oid in order_ids
        ]

        count = 0
        for f in as_completed(futures):
            linhas.extend(f.result())
            count += 1
            if count % 10 == 0:
                print(f"  ✓ {count}/{len(order_ids)} orders processadas")

    if not linhas:
        print("⚠️ Nenhuma linha de dados foi gerada")
        return

    df = pd.DataFrame(linhas)

    # Buscar fretes
    print("buscando fretes...")
    fretes = {}

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = {
            executor.submit(buscar_frete, session, sid, token): sid
            for sid in df["shipment_id"].dropna().unique()
        }

        for f in as_completed(futures):
            fretes[futures[f]] = f.result()

    df["frete_regra_calculada"] = df["shipment_id"].map(fretes).fillna(0)

    # Buscar ADS
    print("buscando ads...")
    item_ids = df["item_id"].unique()
    ads_map = buscar_ads(session, item_ids, data, token)
    df["ads_total_item"] = df["item_id"].map(ads_map).fillna(0)

    # Calcular rateio de ads
    total_qtd_por_item = df.groupby("item_id")["quantity"].transform("sum")
    df["ads_unitario"] = df.apply(
        lambda row: row["ads_total_item"] / row["quantity"] if row["quantity"] > 0 else 0,
        axis=1
    )
    df["ads_rateado"] = df["ads_unitario"] * df["quantity"]

    # Calcular valor líquido
    df["valor_liquido"] = (
        df["valor_bruto_item"]
        - df["discount_real"]
        - df["sale_fee_net"]
        - df["sale_fee_rebate"]
        - df["frete_regra_calculada"]
        - df["ads_rateado"]
    )

    # Remover coluna temporária
    df = df.drop(columns=["shipment_id", "ads_total_item"])

    # Reordenar colunas na ordem correta
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

    # Salvar arquivo
    output = os.path.join(OUTPUT_DIR, f"consolidado_{data}.xlsx")

    df.to_excel(output, index=False)

    print(f"✓ Arquivo gerado: {output}")
    print(f"✓ Total de linhas: {len(df)}")
    print("="*60 + "\n")


if __name__ == "__main__":
    import sys

    data = sys.argv[1]
    inicio = time.time()
    pipeline(data)
    print(f"Tempo total: {round(time.time() - inicio, 2)}s")
