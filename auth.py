import requests
import json
import time
import os

CLIENT_ID = "1668515020275408"
CLIENT_SECRET = "jmI2YhNb6xmBEV4AZtW1UrLHyAGLdU0L"
REDIRECT_URI = "https://icommescola.com.br/"
TOKEN_FILE = "meli_token.json"

AUTH_URL = "https://api.mercadolibre.com/oauth/token"


def gerar_token_inicial(code):
    payload = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI
    }

    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()

    data = response.json()

    salvar_token(data)

    return data["access_token"]


def renovar_token():
    with open(TOKEN_FILE, "r") as f:
        token_data = json.load(f)

    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": token_data["refresh_token"]
    }

    response = requests.post(AUTH_URL, data=payload)
    response.raise_for_status()

    data = response.json()

    salvar_token(data)

    return data["access_token"]


def salvar_token(data):
    token = {
        "access_token": data["access_token"],
        "refresh_token": data["refresh_token"],
        "expires_in": data["expires_in"],
        "created_at": int(time.time())
    }

    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f, indent=2)


def token_expirado():
    if not os.path.exists(TOKEN_FILE):
        return True

    with open(TOKEN_FILE, "r") as f:
        token_data = json.load(f)

    expiracao = token_data["created_at"] + token_data["expires_in"] - 60
    return time.time() > expiracao


def get_token(code=None):
    """
    Função principal que seu sistema deve chamar.
    """

    if not os.path.exists(TOKEN_FILE):
        if not code:
            raise Exception("Token não existe. Forneça o authorization code.")
        return gerar_token_inicial(code)

    if token_expirado():
        print("🔄 Token expirado. Renovando...")
        return renovar_token()

    with open(TOKEN_FILE, "r") as f:
        token_data = json.load(f)

    return token_data["access_token"]

if __name__ == "__main__":
    print("Testando geração de token...")
    token = get_token()
    print("Access token:", token)

