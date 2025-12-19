import os
os.environ["PYTHONUTF8"] = "1"
import time
from reto_data_engineer.utils.logger import get_logger
from reto_data_engineer.etl.extract import extract_all
from reto_data_engineer.etl.transform import transform_all
from reto_data_engineer.etl.load import (
    load_customers, load_orders, load_reviews, load_competitor_pricing,
    load_support_tickets, load_marketing_sends, load_campaigns, load_inventory
)

logger = get_logger(__name__)


def run_etl():

    logger.info("===== 🚀 INICIANDO ETL COMPLETO =====")
    etl_start = time.time()

    # 1️⃣ EXTRACT
    try:
        t0 = time.time()
        raw = extract_all()
        logger.info(f"EXTRACT completado en {time.time() - t0:.3f} s")
    except Exception as e:
        logger.error(f"FALLO EN EXTRACT: {e}", exc_info=True)
        return

    # 2️⃣ TRANSFORM
    try:
        t0 = time.time()
        data = transform_all(raw)
        logger.info(f"TRANSFORM completado en {time.time() - t0:.3f} s")
    except Exception as e:
        logger.error(f"FALLO EN TRANSFORM: {e}", exc_info=True)
        return

    # 3️⃣ LOAD
    summary = {
        "customers": 0, "orders": 0, "reviews": 0,
        "competitor": 0, "support": 0,
        "marketing": 0, "campaigns": 0, "inventory": 0
    }

    # Customers
    try:
        customer_map = load_customers(data["customers"])
        summary["customers"] = len(customer_map)
    except Exception as e:
        logger.error(f"Error CUSTOMERS: {e}", exc_info=True)
        customer_map = {}

    # Orders
    try:
        load_orders(data["orders"], customer_map)
        summary["orders"] = len(data["orders"])
    except Exception as e:
        logger.error(f"Error ORDERS: {e}", exc_info=True)

    # Reviews
    try:
        load_reviews(data["reviews"], customer_map)
        summary["reviews"] = len(data["reviews"])
    except Exception as e:
        logger.error(f"Error REVIEWS: {e}", exc_info=True)

    # Competitor pricing
    try:
        load_competitor_pricing(data["competitor_pricing"])
        summary["competitor"] = len(data["competitor_pricing"])
    except Exception as e:
        logger.error(f"Error COMPETITOR: {e}", exc_info=True)

    # Support tickets
    try:
        if "support_tickets" in data:
            load_support_tickets(data["support_tickets"], customer_map)
            summary["support"] = len(data["support_tickets"])
        else:
            logger.warning("⚠ No support data found in extract stage.")
    except Exception as e:
        logger.error(f"Error SUPPORT: {e}", exc_info=True)

    # Marketing sends
    try:
        load_marketing_sends(data["email_sends"], customer_map)
        summary["marketing"] = len(data["email_sends"])
    except Exception as e:
        logger.error(f"Error MARKETING: {e}", exc_info=True)

    # Campaigns
    try:
        load_campaigns(data["campaigns"])
        summary["campaigns"] = len(data["campaigns"])
    except Exception as e:
        logger.error(f"Error CAMPAIGNS: {e}", exc_info=True)

    # Inventory adjustments
    try:
        load_inventory(data["inventory_adjustments"])
        summary["inventory"] = len(data["inventory_adjustments"])
    except Exception as e:
        logger.error(f"Error INVENTORY: {e}", exc_info=True)

    # 4️⃣ Summary
    logger.info("\n========== ETL SUMMARY ==========")
    for k, v in summary.items():
        logger.info(f"{k.upper():20} → {v}")

    logger.info(f"⏳ Duración total: {time.time() - etl_start:.3f} s")
    logger.info("===== ✔ ETL COMPLETADO =====")


if __name__ == "__main__":
    run_etl()
