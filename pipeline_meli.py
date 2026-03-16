import os
from vendas import main as gerar_vendas
from financeiro2 import main as gerar_financeiro
from historico_itens_diario import main as gerar_historico
from gerar_relatorio import main as gerar_ads
from merge_ads_financeiro import main as merge_ads

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def pipeline(data):

    print("\n===============================")
    print(f"PROCESSANDO DATA {data}")
    print("===============================\n")

    # 1️⃣ VENDAS
    print("1 - Gerando vendas...")
    gerar_vendas(data)

    # 2️⃣ FINANCEIRO
    print("2 - Gerando financeiro...")
    gerar_financeiro(data)

    # 3️⃣ HISTÓRICO ADS
    print("3 - Gerando histórico de itens Ads...")
    gerar_historico(data)

    # 4️⃣ RELATÓRIO ADS
    print("4 - Gerando relatório Ads...")
    gerar_ads(data)

    # 5️⃣ MERGE FINAL
    print("5 - Executando merge final...")
    merge_ads(data)

    print("\n✅ Pipeline finalizado com sucesso!\n")