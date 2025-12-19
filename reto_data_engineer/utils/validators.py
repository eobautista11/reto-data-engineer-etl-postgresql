import pandas as pd

def validate_not_empty(df: pd.DataFrame, name: str):
    """
    Valida que un DataFrame no esté vacío.
    """
    if df is None or df.empty:
        raise ValueError(f"❌ ERROR: El dataset '{name}' está vacío.")
    return True


def validate_columns(df: pd.DataFrame, required_cols: list, name: str):
    """
    Valida que un DataFrame incluya todas las columnas requeridas.
    """
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"❌ '{name}' no contiene columnas requeridas: {missing}")
    return True
