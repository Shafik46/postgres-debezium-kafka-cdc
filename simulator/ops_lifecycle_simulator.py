import argparse
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from faker import Faker
from psycopg2.extras import DictCursor

from utils.db_utils import get_db_connection, print_log, setup_logging

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


logger = setup_logging()
fake = Faker(["en_IE"])

DEFAULT_ORDERS = 10
DEFAULT_ORDERS_UPDATE= 10
DEFAULT_SLEEP_SEC = 3

OPS_STAGES = [
    {
        "event_type": "payment",
        "event_status": "authorized",
        "current_stage": "payment_authorized",
        "payment_status": "authorized",
        "fulfillment_status": "pending",
        "delivery_status": "not_started",
        "notes": "Payment authorization completed."
    },
    {
        "event_type": "fulfillment",
        "event_status": "packed",
        "current_stage": "packed",
        "payment_status": "captured",
        "fulfillment_status": "packed",
        "delivery_status": "not_started",
        "notes": "Warehouse packed the order."
    },
    {
        "event_type": "delivery",
        "event_status": "handover_to_courier",
        "current_stage": "handover_to_courier",
        "payment_status": "captured",
        "fulfillment_status": "shipped",
        "delivery_status": "in_transit",
        "notes": "Courier accepted the package."
    },
    {
        "event_type": "delivery",
        "event_status": "out_for_delivery",
        "current_stage": "out_for_delivery",
        "payment_status": "captured",
        "fulfillment_status": "shipped",
        "delivery_status": "out_for_delivery",
        "notes": "Driver is making the last-mile attempt."
    },
    {
        "event_type": "delivery",
        "event_status": "delivered",
        "current_stage": "delivered",
        "payment_status": "captured",
        "fulfillment_status": "completed",
        "delivery_status": "delivered",
        "notes": "Package delivered successfully."
    }
]

FAILURE_STAGES = {
    "created": {
        "event_type": "payment",
        "event_status": "payment_failed",
        "current_stage": "payment_failed",
        "payment_status": "failed",
        "fulfillment_status": "pending",
        "delivery_status": "not_started",
        "notes": "Payment was declined by the issuer."
    },
    "handover_to_courier": {
        "event_type": "delivery",
        "event_status": "delivery_exception",
        "current_stage": "delivery_exception",
        "payment_status": "captured",
        "fulfillment_status": "shipped",
        "delivery_status": "exception",
        "notes": "Courier reported a delivery exception."
    }
}

MERCHANT_PREFIXES = ["MART", "FOOD", "PHRM", "SHOP", "TECH"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulate transaction and delivery lifecycle changes for CDC."
    )
    parser.add_argument("--new-orders", type=int, default=DEFAULT_ORDERS, help="How many orders to create first.")
    parser.add_argument("--update-cycles", type=int, default=DEFAULT_ORDERS_UPDATE, help="How many status updates to emit.")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SEC, help="Pause between changes.")
    return parser.parse_args()


def run_simulation(new_orders, update_cycles, sleep_seconds):
    conn = get_db_connection()
    try:
        for _ in range(new_orders):
            create_order(conn)
            time.sleep(sleep_seconds)

        for _ in range(update_cycles):
            active_orders = fetch_active_orders(conn)
            if not active_orders:
                print_log("No active orders left to advance.", logger=logger)
                break

            order = random.choice(active_orders)
            advance_order(conn, order["order_id"], order["current_stage"])
            time.sleep(sleep_seconds)
    finally:
        conn.close()


def create_order(conn):
    order_id = str(uuid.uuid4())
    customer_id = fake.bothify(text="CUST-####")
    merchant_id = f"{random.choice(MERCHANT_PREFIXES)}-{fake.bothify(text='##')}"
    amount = round(random.uniform(12.0, 280.0), 2)
    city = fake.city()[:64]
    now = datetime.now(timezone.utc)
    created_note = (
        f"Order created for {fake.first_name()} {fake.last_name()} "
        f"in {city} for {amount:.2f} EUR."
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops_sys.order_lifecycle (
                order_id,
                customer_id,
                merchant_id,
                order_amount,
                currency_code,
                payment_status,
                fulfillment_status,
                delivery_status,
                current_stage,
                city,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                customer_id,
                merchant_id,
                amount,
                "EUR",
                "pending",
                "pending",
                "not_started",
                "created",
                city,
                now,
                now,
            ),
        )
        insert_event(cur, order_id, "order", "created", created_note, now)

    conn.commit()
    print_log(
        f"Created order {order_id} | customer={customer_id} | merchant={merchant_id} | city={city}",
        logger=logger,
    )
    return order_id


def insert_event(cur, order_id, event_type, event_status, notes, event_time):
    cur.execute(
        """
        INSERT INTO ops_sys.order_events (
            order_id,
            event_type,
            event_status,
            event_notes,
            event_time
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (order_id, event_type, event_status, notes, event_time),
    )


def fetch_active_orders(conn):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT order_id, current_stage
            FROM ops_sys.order_lifecycle
            WHERE current_stage NOT IN ('delivered', 'payment_failed')
            ORDER BY updated_at ASC
            """
        )
        return cur.fetchall()


def next_transition(current_stage):
    if current_stage == "created":
        if random.random() < 0.12:
            return FAILURE_STAGES["created"]
        return OPS_STAGES[0]

    ordered_stages = [stage["current_stage"] for stage in OPS_STAGES]
    if current_stage not in ordered_stages:
        return None

    current_idx = ordered_stages.index(current_stage)
    if current_stage == "handover_to_courier" and random.random() < 0.18:
        return FAILURE_STAGES["handover_to_courier"]

    next_idx = current_idx + 1
    if next_idx >= len(OPS_STAGES):
        return None

    return OPS_STAGES[next_idx]


def advance_order(conn, order_id, current_stage):
    transition = next_transition(current_stage)
    if transition is None:
        return False

    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops_sys.order_lifecycle
            SET payment_status = %s,
                fulfillment_status = %s,
                delivery_status = %s,
                current_stage = %s,
                updated_at = %s
            WHERE order_id = %s
            """,
            (
                transition["payment_status"],
                transition["fulfillment_status"],
                transition["delivery_status"],
                transition["current_stage"],
                now,
                order_id,
            ),
        )
        insert_event(
            cur,
            order_id,
            transition["event_type"],
            transition["event_status"],
            transition["notes"],
            now,
        )

    conn.commit()
    print_log(
        f"Advanced order {order_id} -> {transition['current_stage']}",
        logger=logger,
    )
    return True


if __name__ == "__main__":
    args = parse_args()
    run_simulation(
        new_orders=args.new_orders,
        update_cycles=args.update_cycles,
        sleep_seconds=args.sleep_seconds,
    )
