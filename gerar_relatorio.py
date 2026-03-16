import os
import json
import datetime
import requests
import pandas as pd
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIGURAÇÕES
ADVERTISER_SITE_ID = "MLB"
ADVERTISER_ID = "40004"
TOKEN_FILE = "meli_token.json"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PASTA_HISTORICO = os.path.join(BASE_DIR, "data", "historico_itens")
PASTA_ADS = os.path.join(BASE_DIR, "data", "ads")

os.makedirs(PASTA_ADS, exist_ok=True)


# --------------------------------------------
# FUNÇÃO: carregar access_token
# --------------------------------------------
def carregar_token():
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)["access_token"]


# --------------------------------------------
# FUNÇÃO: buscar métricas de um item
# --------------------------------------------
def buscar_metricas_item(item_id, data):
    token = carregar_token()

    url = f"https://api.mercadolibre.com/advertising/{ADVERTISER_SITE_ID}/product_ads/ads/{item_id}"

    params = {
        "date_from": data,
        "date_to": data,
        "metrics": "clicks,prints,cost,direct_items_quantity,indirect_items_quantity,total_amount",
        "aggregation_type": "DAILY"
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }

    r = requests.get(url, headers=headers, params=params, timeout=30)

    if r.status_code != 200:
        return None

    try:
        return r.json()["results"][0]
    except:
        return None


# --------------------------------------------
# FUNÇÃO: descobrir status do anúncio no dia
# --------------------------------------------
def calcular_status(item_id, data):

    hoje = f"{PASTA_HISTORICO}/{data}.json"
    ontem_data = (
        datetime.datetime.strptime(data, "%Y-%m-%d") 
        - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    ontem = f"{PASTA_HISTORICO}/{ontem_data}.json"

    if not os.path.exists(ontem):
        return "Ativo"

    hoje_ids = json.load(open(hoje))
    ontem_ids = json.load(open(ontem))

    if item_id in hoje_ids and item_id in ontem_ids:
        return "Ativo"

    if item_id in hoje_ids and item_id not in ontem_ids:
        return "Ativo"

    if item_id not in hoje_ids and item_id in ontem_ids:
        return "Desativado"

    return "Ativo"


# --------------------------------------------
# MAIN: gerar relatório do dia
# --------------------------------------------
def main(data):

    caminho_hist = f"{PASTA_HISTORICO}/{data}.json"

    if not os.path.exists(caminho_hist):
        print(f"❌ Nenhum histórico encontrado para {data}. Rode primeiro o script histórico.")
        return

    item_ids = json.load(open(caminho_hist))
    print(f"\n📌 {len(item_ids)} itens carregados do histórico.")

    linhas = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(buscar_metricas_item, item_id, data): item_id
            for item_id in item_ids
        }

        for future in as_completed(futures):
            item_id = futures[future]
            metricas = future.result()

            if metricas is None:
                metricas = {
                    "date": data,
                    "clicks": 0,
                    "prints": 0,
                    "cost": 0,
                    "direct_items_quantity": 0,
                    "indirect_items_quantity": 0,
                    "total_amount": 0
                }

            linha = {
                "item_id": item_id,
                "date": data,
                "status": calcular_status(item_id, data),
                "prints": metricas.get("prints", 0),
                "clicks": metricas.get("clicks", 0),
                "cost": metricas.get("cost", 0),
                "vendas_diretas": metricas.get("direct_items_quantity", 0),
                "vendas_assistidas": metricas.get("indirect_items_quantity", 0),
                "receita_total": metricas.get("total_amount", 0)
            }

            linhas.append(linha)

    df = pd.DataFrame(linhas)

    nome_arquivo = os.path.join(PASTA_ADS, f"relatorio_{data}.xlsx")
    df.to_excel(nome_arquivo, index=False)

    print(f"\n🎉 Relatório gerado com sucesso: {nome_arquivo}")


if __name__ == "__main__":
    main(sys.argv[1])