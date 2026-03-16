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


def processar_order(session, order_id, token):

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

    linhas = []

    for oi in order.get("order_items", []):

        item = oi.get("item", {})

        linhas.append({
            "order_id": order_id,
            "pack_id": pack_id,
            "sale_date": sale_date,
            "item_id": item.get("id"),
            "seller_sku": item.get("seller_sku"),
            "quantity": oi.get("quantity"),
            "unit_price": oi.get("unit_price"),
            "discount_real": desconto,
            "shipment_id": shipment_id
        })

    return linhas


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


def pipeline(data):

    token = renovar_token()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    print("buscando orders")

    order_ids = buscar_orders(session, data)

    print("orders:", len(order_ids))

    linhas = []

    with ThreadPoolExecutor(MAX_WORKERS) as executor:

        futures = [
            executor.submit(processar_order, session, oid, token)
            for oid in order_ids
        ]

        for f in as_completed(futures):
            linhas.extend(f.result())

    df = pd.DataFrame(linhas)

    print("buscando fretes")

    fretes = {}

    with ThreadPoolExecutor(MAX_WORKERS) as executor:

        futures = {
            executor.submit(buscar_frete, session, sid, token): sid
            for sid in df["shipment_id"].dropna().unique()
        }

        for f in as_completed(futures):
            fretes[futures[f]] = f.result()

    df["frete_pago_vendedor"] = df["shipment_id"].map(fretes)

    output = os.path.join(OUTPUT_DIR, f"consolidado_{data}.xlsx")

    df.to_excel(output, index=False)

    print("arquivo gerado:", output)


if __name__ == "__main__":

    import sys

    data = sys.argv[1]

    inicio = time.time()

    pipeline(data)

    print("tempo total:", round(time.time() - inicio, 2), "s")