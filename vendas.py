import requests
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from auth import renovar_token
import os 

# =============================
# REQUEST COM RETRY
# =============================

def request_com_retry(session, url, headers=None, params=None, max_tentativas=5, timeout=60):

    for tentativa in range(1, max_tentativas + 1):
        try:
            r = session.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )

            r.raise_for_status()
            return r

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Tentativa {tentativa} falhou para {url}: {e}")

            if tentativa < max_tentativas:
                espera = 3 * tentativa
                print(f"⏳ Aguardando {espera} segundos...")
                time.sleep(espera)
            else:
                print("❌ Falhou após várias tentativas.")
                raise

SELLER_ID = 1087616640
DATA_BASE = None  
LIMIT = 50
MAX_WORKERS = 6  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_VENDAS = os.path.join(BASE_DIR, "data", "vendas")

os.makedirs(PASTA_VENDAS, exist_ok=True)

BASE_SEARCH_URL = "https://api.mercadolibre.com/orders/search"
BASE_ORDER_URL = "https://api.mercadolibre.com/orders/{order_id}"
BASE_PAYMENT_URL = "https://api.mercadopago.com/v1/payments/{payment_id}"

MAP_STATUS = {
    "paid": "aprovada",
    "approved": "aprovada",
    "cancelled": "cancelada",
    "refunded": "reembolsada",
}


def gerar_janelas(data_base):
    data = datetime.strptime(data_base, "%Y-%m-%d")
    janelas = []
    inicio = data
    while inicio < data + timedelta(days=1):
        fim = inicio + timedelta(hours=4) - timedelta(milliseconds=1)
        janelas.append((inicio, fim))
        inicio += timedelta(hours=4)
    return janelas


def buscar_order_ids(session, date_from, date_to):
    offset = 0
    order_ids = []

    while True:
        params = {
            "seller": SELLER_ID,
            "order.date_created.from": date_from,
            "order.date_created.to": date_to,
            "limit": LIMIT,
            "offset": offset
        }

        r = request_com_retry(
            session,
            BASE_SEARCH_URL,
            params=params
        )

        results = r.json().get("results", [])
        if not results:
            break

        order_ids.extend(o["id"] for o in results)
        offset += LIMIT

    return order_ids


def processar_order(session, order_id, token):

    headers = {"Authorization": f"Bearer {token}"}

    order_response = request_com_retry(
        session,
        BASE_ORDER_URL.format(order_id=order_id),
        headers=headers
    )

    order_data = order_response.json()
    pack_id = order_data.get("pack_id")

    status_raw = order_data.get("status")
    status_gerencial = MAP_STATUS.get(status_raw, status_raw)

    desconto_total = 0

    for payment in order_data.get("payments", []):
        payment_id = payment.get("id")

        payment_response = request_com_retry(
            session,
            BASE_PAYMENT_URL.format(payment_id=payment_id),
            headers=headers
        )

        for fee in payment_response.json().get("fee_details", []):
            if fee.get("type") == "coupon_fee":
                desconto_total += float(fee.get("amount", 0))

    linhas = []

    for oi in order_data.get("order_items", []):
        item = oi.get("item", {})

        linhas.append({
            "pack_id": str(pack_id) if pack_id else None,
            "order_id": str(order_id),
            "sale_date": order_data.get("date_created"),
            "status_raw": status_raw,
            "status_gerencial": status_gerencial,
            "item_id": item.get("id"),
            "seller_sku": item.get("seller_sku"),
            "quantity": oi.get("quantity"),
            "unit_price": float(oi.get("unit_price", 0)),
            "discount_real": desconto_total
        })

    return linhas


def main(data_base):

    DATA_BASE = data_base

    token = renovar_token()

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    janelas = gerar_janelas(DATA_BASE)

    todas_linhas = []
    todos_order_ids = set()

    for inicio, fim in janelas:
        date_from = inicio.strftime("%Y-%m-%dT%H:%M:%S.000-03:00")
        date_to = fim.strftime("%Y-%m-%dT%H:%M:%S.999-03:00")

        order_ids = buscar_order_ids(session, date_from, date_to)
        todos_order_ids.update(order_ids)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(processar_order, session, order_id, token)
            for order_id in todos_order_ids
        ]

        for future in as_completed(futures):
            todas_linhas.extend(future.result())

    df = pd.DataFrame(todas_linhas)

    df["sale_date"] = pd.to_datetime(df["sale_date"], utc=True)
    df["sale_date_br"] = df["sale_date"].dt.tz_convert("America/Sao_Paulo")

    data_base_dt = pd.to_datetime(DATA_BASE)
    df = df[df["sale_date_br"].dt.date == data_base_dt.date()].copy()

    df["sale_date"] = df["sale_date_br"].dt.tz_localize(None)
    df.drop(columns=["sale_date_br"], inplace=True)

    colunas = [
        "pack_id",
        "order_id",
        "sale_date",
        "status_raw",
        "status_gerencial",
        "item_id",
        "seller_sku",
        "quantity",
        "unit_price",
        "discount_real"
    ]

    df = df[colunas]

    output = os.path.join(PASTA_VENDAS, f"vendas_{DATA_BASE}.xlsx")
    df.to_excel(output, index=False)

    print(f"VENDAS GERADO: {output}")


if __name__ == "__main__":
    import sys
    main(sys.argv[1])