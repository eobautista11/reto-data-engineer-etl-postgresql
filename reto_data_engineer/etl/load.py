import pandas as pd
import os
import yaml
import pg8000
from pathlib import Path

# =======================================================
#  Cargar configuración desde config/db_config.yaml
# =======================================================
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "db_config.yaml"
)

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    db_cfg = yaml.safe_load(f)

DB_PARAMS = {
    "host": str(db_cfg["host"]).strip(),
    "port": int(db_cfg["port"]),
    "database": str(db_cfg["database"]).strip(),
    "user": str(db_cfg["user"]).strip(),
    "password": str(db_cfg["password"]).strip()
}


def get_connection():
    return pg8000.connect(
        host=DB_PARAMS["host"],
        port=DB_PARAMS["port"],
        database=DB_PARAMS["database"],
        user=DB_PARAMS["user"],
        password=DB_PARAMS["password"]
    )


# =======================================================
#  Normalizador universal
# =======================================================
def clean_value(v):
    if pd.isna(v) or v is pd.NaT:
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, bool):
        return 1 if v else 0
    return v


# =======================================================
#  LOAD CUSTOMERS
# =======================================================
def load_customers(df: pd.DataFrame) -> dict:
    if df.empty:
        print("No hay customers.")
        return {}

    query = """
        INSERT INTO customers (
            customer_id, full_name, email, country,
            language, birth_date, registration_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            email = EXCLUDED.email,
            country = EXCLUDED.country,
            language = EXCLUDED.language,
            birth_date = EXCLUDED.birth_date,
            registration_date = EXCLUDED.registration_date
        RETURNING customer_pk, customer_id;
    """

    conn = get_connection()
    cur = conn.cursor()
    customer_map = {}

    for _, row in df.iterrows():
        values = tuple(clean_value(row[col]) for col in [
            "customer_id", "full_name", "email", "country",
            "language", "birth_date", "registration_date"
        ])

        cur.execute(query, values)
        result = cur.fetchone()
        if result:
            customer_map[result[1]] = result[0]

    conn.commit()
    cur.close()
    conn.close()

    print(f"Customers insertados/actualizados: {len(customer_map)}")
    return customer_map


# =======================================================
# LOAD ORDERS — FK customer_id real
# =======================================================
def load_orders(df: pd.DataFrame, customer_map: dict):
    if df.empty:
        print("No hay orders.")
        return

    query = """
        INSERT INTO orders (
            order_id, customer_pk, total_amount,
            currency, order_date, status
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    skipped = 0

    for _, row in df.iterrows():

        cid = row["customer_id"]

        if cid not in customer_map:
            skipped += 1
            print(f"Orden {row['order_id']} descartada — customer_id no existe: {cid}")
            continue

        values = tuple(clean_value(v) for v in [
            row["order_id"],             # PK orden
            customer_map[cid],           # FK customers
            row["total_amount"],         # monto
            row["currency"],             # moneda
            row["order_date"],           # fecha
            row["status"]                # estado
        ])

        cur.execute(query, values)
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Orders insertadas correctamente: {inserted}")
    if skipped > 0:
        print(f"Orders descartadas por cliente inexistente: {skipped}")


# =======================================================
# LOAD REVIEWS
# =======================================================
def load_reviews(df: pd.DataFrame, customer_map: dict):
    if df.empty:
        print("No hay reviews.")
        return

    query = """
        INSERT INTO reviews (
            review_id, customer_pk, product_id,
            rating, comment, review_date,
            verified_purchase, helpful_votes, unhelpful_votes
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (review_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cid = row["customer_id"]
        if cid not in customer_map:
            continue

        values = tuple(clean_value(v) for v in [
            row["review_id"], customer_map[cid], row["product_id"],
            row["rating"], row["comment"], row["review_date"],
            row["verified_purchase"], row["helpful_votes"], row["unhelpful_votes"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Reviews cargadas.")


# =======================================================
# LOAD COMPETITOR PRICING
# =======================================================
def load_competitor_pricing(df: pd.DataFrame):
    if df.empty:
        print("⚠ No hay competitor pricing.")
        return

    query = """
        INSERT INTO competitor_pricing (
            product_id, snapshot_date, our_price,
            competitor_price, competitor_name,
            stock, num_reviews, rating
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        values = tuple(clean_value(v) for v in [
            row["product_id"], row["snapshot_date"], row["our_price"],
            row["competitor_price"], row["competitor_name"], row["in_stock"],
            row["num_reviews"], row["rating"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Competitor pricing cargado.")


# =======================================================
# LOAD SUPPORT TICKETS
# =======================================================
def load_support_tickets(df: pd.DataFrame, customer_map: dict):
    if df.empty:
        print("No hay tickets.")
        return

    query = """
        INSERT INTO support_tickets (
            ticket_id, customer_pk, transaction_id,
            subject, description, priority, status,
            created_at, updated_at, resolved_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ticket_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cid = row["customer_id"]
        if cid not in customer_map:
            continue

        values = tuple(clean_value(v) for v in [
            row["ticket_id"], customer_map[cid], row["transaction_id"],
            row["subject"], row["description"], row["priority"], row["status"],
            row["created_at"], row["updated_at"], row["resolved_at"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Support tickets cargados.")


# =======================================================
# LOAD MARKETING SENDS
# =======================================================
def load_marketing_sends(df: pd.DataFrame, customer_map: dict):
    if df.empty:
        print("No hay sends.")
        return

    query = """
        INSERT INTO marketing_sends (
            send_id, customer_pk, campaign_id,
            sent_date, open_date, click_date,
            conversion_date, bounced, bounce_reason
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (send_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cid = row["customer_id"]
        if cid not in customer_map:
            continue

        values = tuple(clean_value(v) for v in [
            row["send_id"], customer_map[cid], row["campaign_id"],
            row["sent_date"], row["open_date"], row["click_date"],
            row["conversion_date"], row["bounced"], row["bounce_reason"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Marketing sends cargados.")


# =======================================================
# LOAD CAMPAIGNS
# =======================================================
def load_campaigns(df: pd.DataFrame):
    if df.empty:
        print("No hay campañas.")
        return

    query = """
        INSERT INTO campaigns (
            campaign_id, name, channel, budget,
            impressions, clicks, conversions,
            revenue_generated, start_date, end_date
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (campaign_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        values = tuple(clean_value(v) for v in [
            row["campaign_id"], row["name"], row["channel"], row["budget"],
            row["impressions"], row["clicks"], row["conversions"],
            row["revenue_generated"], row["start_date"], row["end_date"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Campaigns cargadas.")


# =======================================================
# LOAD INVENTORY
# =======================================================
def load_inventory(df: pd.DataFrame):
    if df.empty:
        print("No hay inventario.")
        return

    query = """
        INSERT INTO inventory_adjustments (
            adjustment_id, product_id, movement_type,
            quantity_change, previous_stock, new_stock,
            warehouse, adjustment_date, user_name
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (adjustment_id) DO NOTHING;
    """

    conn = get_connection()
    cur = conn.cursor()

    for _, row in df.iterrows():
        values = tuple(clean_value(v) for v in [
            row["adjustment_id"], row["product_id"], row["movement_type"],
            row["quantity_change"], row["previous_stock"], row["new_stock"],
            row["warehouse"], row["adjustment_date"], row["user_name"]
        ])

        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()
    print("Inventory adjustments cargados.")
