import pandas as pd
import numpy as np

# ==============================
# HELPERS
# ==============================

def normalize_email(email):
    if pd.isna(email):
        return None
    return str(email).strip().lower()

def to_timestamp(x):
    return pd.to_datetime(x, errors="coerce")

def extract_age_range(text):
    if text is None or pd.isna(text):
        return (None, None)
    try:
        parts = str(text).split("-")
        return int(parts[0]), int(parts[1])
    except:
        return (None, None)

# ==============================
# CUSTOMERS
# ==============================

def transform_customers(df):
    df = df.copy()

    # normalizar email y fechas
    df["email"] = df["email"].apply(normalize_email)
    df["birth_date"] = to_timestamp(df["birth_date"]).dt.date
    df["registration_date"] = to_timestamp(df["registration_date"])

    # país normalizado
    df["country"] = (
        df["address"]
        .apply(lambda x: x["country"].strip() if isinstance(x, dict) and "country" in x else None)
        .replace({
            "MX": "Mexico",
            "BR": "Brasil",
            "AR": "Argentina",
            "CO": "Colombia"
        })
    )

    # idioma normalizado
    df["language"] = df["preferred_language"].str.lower().str.strip()

    # nombre estándar
    df = df.rename(columns={
        "name": "full_name"
    })

    # DQ: eliminar registros sin email
    df = df[df["email"].notna()]

    # DQ: evitar duplicados por customer_id
    df = df.drop_duplicates(subset=["customer_id"], keep="first")

    keep_cols = [
        "customer_id", "full_name", "email", "country",
        "language", "birth_date", "registration_date"
    ]

    return df[keep_cols]

# ==============================
# ORDERS — MATCH POR EMAIL
# ==============================

def transform_orders(df, customers_df):
    df = df.copy()

    # monto y fecha
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["payment_date"] = to_timestamp(df["payment_date"])

    # email de la orden (en este dataset sólo viene en paypal_email,
    # pero dejamos lógica extendible por si existiera "email" directo)
    def get_order_email(row):
        # paypal
        if "paypal_email" in row and pd.notna(row["paypal_email"]):
            return normalize_email(row["paypal_email"])
        # fallback por si en algún momento el raw incluye "email"
        if "email" in row and pd.notna(row["email"]):
            return normalize_email(row["email"])
        return None

    df["email"] = df.apply(get_order_email, axis=1)

    # join contra customers usando email (clave común en este dataset)
    merged = df.merge(
        customers_df[["email", "customer_id"]],
        on="email",
        how="left"
    )

    # renombrar a nombres del modelo de órdenes
    merged = merged.rename(columns={
        "transaction_id": "order_id",
        "amount": "total_amount",
        "currency": "currency",
        "payment_date": "order_date",
        "status": "status"
    })

    # reglas de calidad: sólo órdenes enlazadas a un cliente válido
    merged = merged[
        merged["total_amount"].notna() &
        merged["order_id"].notna() &
        merged["customer_id"].notna()
    ]

    keep = [
        "order_id", "total_amount", "currency",
        "order_date", "status", "customer_id"
    ]

    return merged[keep]

# ==============================
# REVIEWS
# ==============================

def transform_reviews(df_jan, df_feb):
    df = pd.concat([df_jan, df_feb], ignore_index=True)

    df["review_date"] = to_timestamp(df["review_date"])

    keep = [
        "review_id", "customer_id", "product_id", "transaction_id",
        "rating", "title", "comment", "review_date",
        "verified_purchase", "helpful_votes", "unhelpful_votes"
    ]

    return df[keep]

# ==============================
# COMPETITOR PRICING
# ==============================

def transform_competitor(df):
    df = df.copy()

    df["snapshot_date"] = to_timestamp(df["snapshot_date"]).dt.date

    df = df.rename(columns={
        "our_product_id": "product_id",
        "competitor": "competitor_name"
    })

    keep = [
        "snapshot_id", "product_id", "snapshot_date",
        "our_price", "competitor_price",
        "competitor_name", "in_stock",
        "num_reviews", "rating", "competitor_url"
    ]

    return df[keep]

# ==============================
# INVENTORY ADJUSTMENTS
# ==============================

def transform_inventory(df_jan, df_feb):
    df = pd.concat([df_jan, df_feb], ignore_index=True)

    df["adjustment_date"] = to_timestamp(df["date"])

    df = df.rename(columns={
        "type": "movement_type",
        "user": "user_name"
    })

    keep = [
        "adjustment_id", "product_id", "movement_type",
        "quantity_change", "previous_stock", "new_stock",
        "warehouse", "adjustment_date", "user_name", "reason"
    ]

    return df[keep]

# ==============================
# SUPPORT TICKETS
# ==============================

def transform_support(df):
    df = df.copy()

    df["created_at"] = to_timestamp(df["created_at"])
    df["updated_at"] = to_timestamp(df["updated_at"])
    df["resolved_at"] = to_timestamp(df["resolved_at"])

    keep = [
        "ticket_id", "customer_id", "transaction_id",
        "subject", "description", "priority", "status",
        "created_at", "updated_at", "resolved_at"
    ]

    return df[keep]

# ==============================
# EMAIL SENDS
# ==============================

def transform_email_sends(df):
    df = df.copy()

    df["sent_date"] = to_timestamp(df["sent_date"])
    df["open_date"] = to_timestamp(df["open_date"])
    df["click_date"] = to_timestamp(df["click_date"])
    df["conversion_date"] = to_timestamp(df["conversion_date"])

    if "bounce_reason" not in df.columns:
        df["bounce_reason"] = None

    keep = [
        "send_id", "customer_id", "campaign_id",
        "sent_date", "open_date", "click_date",
        "conversion_date", "bounced", "bounce_reason"
    ]

    return df[keep]

# ==============================
# CAMPAIGNS
# ==============================

def transform_campaigns(df):
    df = df.copy()

    df["start_date"] = to_timestamp(df["start_date"]).dt.date
    df["end_date"] = to_timestamp(df["end_date"]).dt.date

    df["age_min"], df["age_max"] = zip(*df["target_audience"].apply(
        lambda x: extract_age_range(x.get("age_range")) if isinstance(x, dict) else (None, None)
    ))

    keep = [
        "campaign_id", "name", "channel",
        "budget", "impressions", "clicks",
        "conversions", "revenue_generated",
        "start_date", "end_date",
        "age_min", "age_max"
    ]

    return df[keep]

# ==============================
# TRANSFORM ALL
# ==============================

def transform_all(d):
    customers_df = transform_customers(d["customers"])

    return {
        "customers": customers_df,
        "orders": transform_orders(d["payments"], customers_df),
        "reviews": transform_reviews(d["reviews_jan"], d["reviews_feb"]),
        "competitor_pricing": transform_competitor(d["competitor"]),
        "inventory_adjustments": transform_inventory(d["inv_jan"], d["inv_feb"]),
        "support_tickets": transform_support(d["support"]),
        "email_sends": transform_email_sends(d["email_sends"]),
        "campaigns": transform_campaigns(d["campaigns"])
    }
