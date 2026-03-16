import sys
import os
import time
import requests
import pandas as pd
from auth import renovar_token
# =============================
# REQUEST COM RETRY
# =============================

def request_com_retry(url, headers=None, params=None, max_tentativas=3, timeout=30):

    for tentativa in range(1, max_tentativas + 1):
        try:
            r = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout
            )

            r.raise_for_status()
            return r

        except requests.exceptions.RequestException as e:
            print(f"Tentativa {tentativa} falhou: {e}")

            if tentativa < max_tentativas:
                print(" Aguardando 5 segundos para tentar novamente...")
                time.sleep(5)
            else:
                print("Falhou após várias tentativas.")
                raise

# =============================
# CONFIG
# =============================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PASTA_VENDAS = os.path.join(BASE_DIR, "data", "vendas")
PASTA_FINANCEIRO = os.path.join(BASE_DIR, "data", "financeiro")

os.makedirs(PASTA_FINANCEIRO, exist_ok=True)

if len(sys.argv) < 2:
    print("❌ Data não informada.")
    sys.exit(1)

DATA_BASE = sys.argv[1]
VENDAS_FILE = os.path.join(PASTA_VENDAS, f"vendas_{DATA_BASE}.xlsx")
OUTPUT_FILE = os.path.join(PASTA_FINANCEIRO, f"financeiro_{DATA_BASE}.xlsx")

BASE_ORDER_URL = "https://api.mercadolibre.com/orders/{order_id}"
BASE_BILLING_URL = "https://api.mercadolibre.com/billing/integration/group/ML/order/details"
BASE_SHIPMENT_COSTS_URL = "https://api.mercadolibre.com/shipments/{shipment_id}/costs"

REQUEST_DELAY = 0.05
BILLING_CHUNK = 50

# =============================
# UTIL
# =============================

def chunked(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

# =============================
# BILLING
# =============================

def buscar_billing_orders(order_ids, token):
    headers = {"Authorization": f"Bearer {token}"}
    billing_map = {}

    for lote in chunked(order_ids, BILLING_CHUNK):

        r = request_com_retry(
            BASE_BILLING_URL,
            headers=headers,
            params={"order_ids": ",".join(lote)}
    )

        r.raise_for_status()

        results = r.json().get("results", [])

        for registro in results:

            order_id = str(registro.get("order_id"))

            # 🔧 CORREÇÃO AQUI — proteção contra sale_fee = None
            sale_fee = registro.get("sale_fee") or {}
            charges = registro.get("charges") or []

            charge_amount = sum(c.get("detail_amount", 0) for c in charges)
            charge_desc = " | ".join(
                c.get("transaction_detail", "")
                for c in charges if c.get("transaction_detail")
            )

            billing_map[order_id] = {
                "sale_fee_net": sale_fee.get("net"),
                "sale_fee_rebate": sale_fee.get("rebate"),
                "charge_amount": charge_amount,
                "charge_description": charge_desc
            }

        time.sleep(REQUEST_DELAY)

    return billing_map

# =============================
# FRETE API
# =============================

def buscar_frete(shipment_id, token):
    if not shipment_id:
        return {"frete_pago_vendedor": 0, "frete_pago_comprador": 0}

    headers = {
        "Authorization": f"Bearer {token}",
        "x-format-new": "true"
    }

    r = request_com_retry(
        BASE_SHIPMENT_COSTS_URL.format(shipment_id=shipment_id),
        headers=headers
)

    data = r.json()

    sender = (data.get("senders") or [{}])[0]
    receiver = data.get("receiver") or {}

    return {
        "frete_pago_vendedor": sender.get("cost") or 0,
        "frete_pago_comprador": receiver.get("cost") or 0
    }

# =============================
# REGRA FRETE DEFINITIVA
# =============================

def aplicar_regra_frete(df):

    df["frete_regra_calculada"] = df["frete_pago_vendedor"]
    df["alerta_regra_frete"] = ""
    df["alerta_operacional_frete"] = ""

    for tracking, grupo in df.groupby("tracking_number"):

        if pd.isna(tracking):
            continue

        maiores = grupo[grupo["unit_price"] >= 79]
        menores = grupo[grupo["unit_price"] < 79]

        frete_total = grupo["frete_pago_vendedor"].max()

        # VENDA NÃO CONJUNTA
        if len(grupo) == 1:

            idx = grupo.index[0]

            if grupo.iloc[0]["unit_price"] < 79 and frete_total > 0:
                df.loc[idx, "alerta_operacional_frete"] = "PREJUIZO_MENOR_79_COM_FRETE"
            else:
                df.loc[idx, "alerta_operacional_frete"] = "OK_UNITARIA"

            continue

        # 1 MAIOR + 1 OU MAIS MENORES
        if len(maiores) == 1 and len(menores) >= 1:

            idx_maior = maiores.index[0]
            df.loc[idx_maior, "frete_regra_calculada"] = frete_total
            df.loc[idx_maior, "alerta_regra_frete"] = "FRETE_INTEGRAL_MAIOR_79"

            for idx in menores.index:
                df.loc[idx, "frete_regra_calculada"] = 0
                df.loc[idx, "alerta_regra_frete"] = "MENOR_79_ZERADO"

                if df.loc[idx, "frete_pago_vendedor"] > 0:
                    df.loc[idx, "alerta_operacional_frete"] = "PREJUIZO_MENOR_79_COM_FRETE"

        # VÁRIOS MAIORES
        elif len(maiores) > 1:

            soma_maiores = maiores["unit_price"].sum()

            for idx, row in maiores.iterrows():
                rateio = frete_total * (row["unit_price"] / soma_maiores)
                df.loc[idx, "frete_regra_calculada"] = round(rateio, 2)
                df.loc[idx, "alerta_regra_frete"] = "RATEIO_ENTRE_MAIORES"

    return df

# =============================
# MAIN
# =============================

def main():

    print(" Lendo vendas...")
    df_vendas = pd.read_excel(
    VENDAS_FILE,
    dtype={
        "pack_id": str,
        "order_id": str
    }
)

    df_vendas = df_vendas.drop_duplicates(
        subset=["order_id", "item_id", "unit_price", "sale_date"],
        keep="first"
    )

    df_vendas["order_id"] = df_vendas["order_id"].astype(str)
    df_vendas["item_id"] = df_vendas["item_id"].astype(str)

    token = renovar_token()

    order_ids = df_vendas["order_id"].unique()

    print(f" Buscando billing de {len(order_ids)} orders...")
    billing_map = buscar_billing_orders(order_ids, token)

    print(" Buscando fretes...")

    frete_cache = {}
    linhas_fin = []

    for _, row in df_vendas.iterrows():

        order_id = row["order_id"]
        item_id = row["item_id"]

        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(BASE_ORDER_URL.format(order_id=order_id), headers=headers)
        order_data = r.json()

        shipment_id = (order_data.get("shipping") or {}).get("id")

        if shipment_id not in frete_cache:
            frete_cache[shipment_id] = buscar_frete(shipment_id, token)

        frete = frete_cache.get(shipment_id, {})
        billing = billing_map.get(order_id, {})

        linhas_fin.append({
            "order_id": order_id,
            "item_id": item_id,
            "tracking_number": shipment_id,

            "sale_fee_net": billing.get("sale_fee_net"),
            "sale_fee_rebate": billing.get("sale_fee_rebate"),
            "charge_amount": billing.get("charge_amount"),
            "charge_description": billing.get("charge_description"),

            "frete_pago_vendedor": frete.get("frete_pago_vendedor"),
            "frete_pago_comprador": frete.get("frete_pago_comprador"),
        })

        time.sleep(REQUEST_DELAY)

    df_fin = pd.DataFrame(linhas_fin)

    df_final = df_vendas.merge(
        df_fin,
        on=["order_id", "item_id"],
        how="left"
    )

    df_final = aplicar_regra_frete(df_final)
    

    df_final.to_excel(OUTPUT_FILE, index=False)

    print(f"\n FINANCEIRO DEFINITIVO GERADO: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
