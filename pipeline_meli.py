import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import time
from auth import renovar_token

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


# =============================
# REQUEST RETRY
# =============================
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


# =============================
# BUSCAR ORDERS
# =============================
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


# =============================
# BILLING
# =============================
def buscar_billing(session, order_ids, token):

    headers = {"Authorization": f"Bearer {token}"}
    billing_map = {}

    chunk = 50

    for i in range(0, len(order_ids), chunk):

        lote = order_ids[i:i+chunk]

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
                "sale_fee_net": sale_fee.get("net", 0),
                "sale_fee_rebate": sale_fee.get("rebate", 0),
                "charge_amount": sum(c.get("detail_amount", 0) for c in charges)
            }

    return billing_map


# =============================
# PROCESSAR ORDER
# =============================
def processar_order(session, order_id, token, billing_map):

    headers = {"Authorization": f"Bearer {token}"}

    r = request_retry(session, BASE_ORDER_URL.format(order_id=order_id), headers=headers)
    order = r.json()

    pack_id = order.get("pack_id")
    sale_date = order.get("date_created")
    shipment_id = (order.get("shipping") or {}).get("id")

    desconto = 0

    for payment in order.get("payments", []):
        payment_id = payment.get("id")

        pr = request_retry(
            session,
            BASE_PAYMENT_URL.format(payment_id=payment_id),
            headers=headers
        )

        for fee in pr.json().get("fee_details", []):
            if fee.get("type") == "coupon_fee":
                desconto += float(fee.get("amount", 0))

    billing = billing_map.get(str(order_id), {})

    linhas = []

    for oi in order.get("order_items", []):
        item = oi.get("item", {})

        linhas.append({
            "order_id": str(order_id),
            "pack_id": str(pack_id),
            "sale_date": sale_date,
            "item_id": str(item.get("id")),
            "seller_sku": item.get("seller_sku"),
            "quantity": oi.get("quantity"),
            "unit_price": float(oi.get("unit_price", 0)),
            "discount_real": desconto,
            "shipment_id": shipment_id,

            "sale_fee_net": billing.get("sale_fee_net", 0),
            "sale_fee_rebate": billing.get("sale_fee_rebate", 0),
            "charge_amount": billing.get("charge_amount", 0)
        })

    return linhas


# =============================
# FRETE
# =============================
def buscar_frete(session, shipment_id, token):

    if not shipment_id:
        return 0

    headers = {"Authorization": f"Bearer {token}"}

    r = request_retry(
        session,
        BASE_SHIPMENT_COSTS_URL.format(shipment_id=shipment_id),
        headers=headers
    )

    data = r.json()
    sender = (data.get("senders") or [{}])[0]

    return sender.get("cost") or 0


# =============================
# ADS
# =============================
def buscar_ads(session, item_ids, data, token):

    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }

    ads_map = {}

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
                }
            )

            ads_map[item_id] = r.json()["results"][0]["cost"]

        except:
            ads_map[item_id] = 0

    return ads_map


# =============================
# PIPELINE PRINCIPAL
# =============================
def pipeline(data):

    token = renovar_token()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    print("buscando orders...")
    order_ids = buscar_orders(session, data)

    print("buscando billing...")
    billing_map = buscar_billing(session, order_ids, token)

    print("processando orders...")
    linhas = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = [
            executor.submit(processar_order, session, oid, token, billing_map)
            for oid in order_ids
        ]

        for f in as_completed(futures):
            linhas.extend(f.result())

    df = pd.DataFrame(linhas)

    print("buscando frete...")
    fretes = {}

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = {
            executor.submit(buscar_frete, session, sid, token): sid
            for sid in df["shipment_id"].dropna().unique()
        }

        for f in as_completed(futures):
            fretes[futures[f]] = f.result()

    df["frete_regra_calculada"] = df["shipment_id"].map(fretes)

    print("buscando ads...")
    ads_map = buscar_ads(session, df["item_id"].unique(), data, token)

    df["ads_total_item"] = df["item_id"].map(ads_map)

    total_qtd = df.groupby("item_id")["quantity"].transform("sum")

    df["ads_unitario"] = df["ads_total_item"] / total_qtd.replace(0, 1)
    df["ads_rateado"] = df["ads_unitario"] * df["quantity"]

    print("calculando métricas finais...")

    df["valor_bruto_item"] = df["quantity"] * df["unit_price"]

    df["valor_liquido"] = (
        df["valor_bruto_item"]
        - df["discount_real"]
        - df["sale_fee_net"]
        - df["sale_fee_rebate"]
        - df["frete_regra_calculada"]
        - df["ads_rateado"]
    )

    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["sale_date_only"] = df["sale_date"].dt.date

    output = os.path.join(OUTPUT_DIR, f"consolidado_{data}.xlsx")

    df.to_excel(output, index=False)

    print("arquivo gerado:", output)