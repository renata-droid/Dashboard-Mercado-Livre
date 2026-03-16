import pandas as pd
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PASTA_FINANCEIRO = os.path.join(BASE_DIR, "data", "financeiro")
PASTA_ADS = os.path.join(BASE_DIR, "data", "ads")
PASTA_CONSOLIDADO_DIARIO = os.path.join(BASE_DIR, "data", "consolidado_diario")

os.makedirs(PASTA_CONSOLIDADO_DIARIO, exist_ok=True)

def main(DATA_BASE):

    OUTPUT_FILE = os.path.join(PASTA_CONSOLIDADO_DIARIO, f"consolidado_{DATA_BASE}.xlsx")

    VENDAS_FILE = os.path.join(
        BASE_DIR,
        "data",
        "consolidado",
        f"consolidado_{DATA_BASE}.xlsx"
    )

    print("Lendo vendas do pipeline...")
    df_fin = pd.read_excel(VENDAS_FILE)


    # Garantir tipos corretos
    df_fin["item_id"] = df_fin["item_id"].astype(str)
    df_ads = pd.DataFrame(columns=["item_id", "ads_unitario"])

    # =============================
    # PASSO 1 — TOTAL VENDIDO POR ITEM
    # =============================

    total_vendido = (
        df_fin
        .groupby("item_id")["quantity"]
        .sum()
        .reset_index()
        .rename(columns={"quantity": "total_quantity_dia"})
    )

    # =============================
    # PASSO 2 — MERGE ADS + TOTAL
    # =============================

    df_ads_merge = df_ads.merge(
        total_vendido,
        on="item_id",
        how="left"
    )

    df_ads_merge["total_quantity_dia"] = df_ads_merge["total_quantity_dia"].fillna(0)

    # =============================
    # PASSO 3 — CALCULAR ADS UNITÁRIO
    # =============================

    df_ads_merge["ads_unitario"] = 0.0

    # =============================
    # PASSO 4 — MERGE FINAL COM FINANCEIRO
    # =============================

    df_final = df_fin.merge(
        df_ads_merge[["item_id", "ads_unitario"]],
        on="item_id",
        how="left"
    )

    df_final["ads_unitario"] = df_final["ads_unitario"].fillna(0)

    # =============================
    # PASSO 5 — RATEIO POR LINHA
    # =============================

    df_final["ads_rateado"] = (
        df_final["ads_unitario"] *
        df_final["quantity"]
    )

    df_final["valor_bruto_item"] = (
        df_final["quantity"] *
        df_final["unit_price"]
    )

    pos = df_final.columns.get_loc("unit_price") + 1
    df_final.insert(pos, "valor_bruto_item", df_final.pop("valor_bruto_item"))

    cols_calc = [
        "valor_bruto_item",
        "discount_real",
        "sale_fee_net",
        "sale_fee_rebate",
        "frete_regra_calculada"
]
    for col in cols_calc:
        df_final[col] = pd.to_numeric(df_final[col], errors="coerce").fillna(0)

    df_final["sale_fee_bruta"] = (
        df_final["sale_fee_net"] +
        df_final["sale_fee_rebate"]
    )
    pos = df_final.columns.get_loc("sale_fee_net") + 1
    df_final.insert(pos, "sale_fee_bruta", df_final.pop("sale_fee_bruta"))

    df_final["valor_liquido"] = (
        df_final["valor_bruto_item"]
        - df_final["discount_real"]
        - df_final["sale_fee_net"]
        - df_final["sale_fee_rebate"]
        - df_final["frete_regra_calculada"]
)

    pos = df_final.columns.get_loc("ads_rateado") + 1
    df_final.insert(pos, "valor_liquido", df_final.pop("valor_liquido"))

        
    print("Colunas atuais:", df_final.columns.tolist())


    df_final["sale_date_only"] = pd.to_datetime(df_final["sale_date"]).dt.date

    pos = df_final.columns.get_loc("sale_date") + 1
    df_final.insert(pos, "sale_date_only", df_final.pop("sale_date_only"))

    # =============================
    # VALIDAÇÃO
    # =============================

    print("\n Validação:")
    print("Total Ads Painel:", round(df_ads["cost"].sum(), 2))
    print("Total Ads Rateado:", round(df_final["ads_rateado"].sum(), 2))

    df_final["pack_id"] = df_final["pack_id"].fillna("").astype(str)
    df_final["order_id"] = df_final["order_id"].astype(str)
    df_final["item_id"] = df_final["item_id"].astype(str)

    # =============================
    # EXPORTAR
    # =============================

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        df_final.to_excel(writer, index=False, sheet_name="Sheet1")

        workbook  = writer.book
        worksheet = writer.sheets["Sheet1"]

        text_format = workbook.add_format({'num_format': '@'})

        worksheet.set_column("A:A", None, text_format)  # pack_id
        worksheet.set_column("B:B", None, text_format)  # order_id

    print(f"\n Consolidado gerado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main(sys.argv[1])