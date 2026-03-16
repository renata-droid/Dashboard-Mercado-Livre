import os
import json
import time
import requests
from pathlib import Path

# ============================================
# CONFIGURAÇÕES
# ============================================
ADVERTISER_SITE_ID = "MLB"
ADVERTISER_ID = "40004"
TOKEN_FILE = "meli_token.json"


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_HISTORICO = os.path.join(BASE_DIR, "data", "historico_itens")

Path(PASTA_HISTORICO).mkdir(parents=True, exist_ok=True)

# ============================================
# CARREGAR TOKEN COMPLETO
# ============================================
def carregar_token_json():
    if not os.path.exists(TOKEN_FILE):
        raise FileNotFoundError("Arquivo meli_token.json não encontrado!")
    with open(TOKEN_FILE, "r") as f:
        return json.load(f)

# ============================================
# SALVAR TOKEN
# ============================================
def salvar_token_json(data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f)

# ============================================
# RENOVAR TOKEN AUTOMATICAMENTE
# ============================================
def renovar_token():
    token_data = carregar_token_json()

    agora = int(time.time())
    expires_at = token_data["created_at"] + token_data["expires_in"]

    if agora < expires_at - 60:
        return token_data["access_token"]

    print("Token expirado, renovando...")

    url = "https://api.mercadolibre.com/oauth/token"

    payload = {
        "grant_type": "refresh_token",
        "client_id": "1668515020275408",
        "client_secret": "jmI2YhNb6xmBEV4AZtW1UrLHyAGLdU0L",
        "refresh_token": token_data["refresh_token"],
    }

    response = requests.post(url, data=payload).json()

    if "access_token" not in response:
        print(" ERRO ao renovar token:", response)
        return None

    novo_token = {
        "access_token": response["access_token"],
        "refresh_token": response.get("refresh_token", token_data["refresh_token"]),
        "expires_in": response["expires_in"],
        "created_at": int(time.time()),
    }

    salvar_token_json(novo_token)
    print("Token renovado com sucesso!")

    return novo_token["access_token"]

# ============================================
# FUNÇÃO: COLETAR ITEM_IDS COM PAGINAÇÃO
# ============================================
def coletar_itens_por_data(data):
    token = renovar_token()
    if not token:
        return []

    url = (
        f"https://api.mercadolibre.com/advertising/{ADVERTISER_SITE_ID}/advertisers/"
        f"{ADVERTISER_ID}/product_ads/ads/search"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "api-version": "2"
    }

    offset = 0
    limit = 250
    todos_itens = set()

    print(f"\n Consultando itens do dia {data}...")

    while True:

        params = {
            "date_from": data,
            "date_to": data,
            "limit": limit,
            "offset": offset,
            "metrics": "clicks",
        }

        r = requests.get(url, headers=headers, params=params)

        if r.status_code != 200:
            print(f"❌ Erro ao consultar API: {r.status_code}")
            print(r.text)
            break

        dados = r.json()
        results = dados.get("results", [])

        if not results:
            break

        for item in results:
            todos_itens.add(item["item_id"])

        print(f"Página offset {offset} → {len(results)} itens")

        if len(results) < limit:
            break

        offset += limit
        time.sleep(0.3)

    print(f" {len(todos_itens)} itens únicos encontrados.")
    return sorted(todos_itens)

# ============================================
# SALVAR JSON DO HISTÓRICO
# ============================================
def salvar_historico(data, itens):
    caminho = f"{PASTA_HISTORICO}/{data}.json"
    with open(caminho, "w") as f:
        json.dump(itens, f, indent=4)

    print(f" Histórico salvo em: {caminho}")

# ============================================
# MAIN
# ============================================
def main(data):

    itens = coletar_itens_por_data(data)

    if itens:
        salvar_historico(data, itens)
        print("\n Processo concluído com sucesso!")
    else:
        print("\nNenhum item encontrado. Histórico não gerado.")


if __name__ == "__main__":
    import sys
    main(sys.argv[1])