import os
import json
import pandas as pd

BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "json")

def load_json(filename: str) -> pd.DataFrame:
    """
    Carga un archivo JSON desde /data/json y lo devuelve como DataFrame.
    Valida que exista y que tenga formato correcto.
    """
    file_path = os.path.join(BASE_PATH, filename)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error leyendo JSON {filename}: {e}")

    # Convertir lista de dicts -> DataFrame
    if isinstance(data, list):
        return pd.DataFrame(data)
    else:
        return pd.DataFrame([data])

def extract_all():
    """Carga todos los datasets necesarios para el ETL."""

    datasets = {
        "customers": load_json("customers_master.json"),
        "reviews_jan": load_json("customer_reviews_jan.json"),
        "reviews_feb": load_json("customer_reviews_feb.json"),
        "competitor": load_json("competitor_pricing.json"),
        "support": load_json("customer_support_tickets.json"),
        "email_sends": load_json("email_marketing_sends.json"),
        "inv_jan": load_json("inventory_adjustments_jan.json"),
        "inv_feb": load_json("inventory_adjustments_feb.json"),
        "campaigns": load_json("marketing_campaigns_q1.json"),
        "payments": load_json("payment_transactions.json"),
    }

    print("EXTRACT: JSON cargados correctamente")
    return datasets
