
import io
import csv
import json
import random
import argparse
import boto3
from datetime import datetime, timedelta
from faker import Faker

# ── Config ─────────────────────────────────────────────────────────────────────
BUCKET = "ecommerse-pipeline"
PREFIX = "raw"
REGION = "ap-southeast-1"

NUM_PRODUCTS = 5_000
NUM_ORDERS_FULL = 100_000
NUM_ORDERS_INCREMENTAL = 500

CATEGORIES = ["Electronics", "Fashion", "Home", "Beauty", "Sports", "Books", "Grocery"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "COD", "Wallet"]
PAYMENT_STATUSES = ["SUCCESS", "FAILED", "PENDING", "REFUNDED"]
ORDER_STATUSES = ["DELIVERED", "SHIPPED", "CANCELLED", "RETURNED", "PROCESSING"]
RETURN_REASONS = ["Damaged", "Wrong item", "Not as described", "Changed mind", None]

s3 = boto3.client("s3", region_name=REGION)
fake = Faker("en_IN")


# ── Helpers ────────────────────────────────────────────────────────────────────

def random_date(start_days_ago=365, end_days_ago=0):
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    return start + (end - start) * random.random()


def today_random_time():
    now = datetime.now()
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_elapsed = int((now - start_of_day).total_seconds())
    return start_of_day + timedelta(seconds=random.randint(0, max(seconds_elapsed, 1)))


def to_csv_buffer(rows):
    """Convert list of dicts to an in-memory CSV buffer."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return io.BytesIO(buffer.getvalue().encode("utf-8"))


def upload_to_s3(buffer, s3_key, count):
    """Upload in-memory buffer to S3."""
    buffer.seek(0)
    s3.put_object(
        Bucket=BUCKET,
        Key=s3_key,
        Body=buffer,
        ContentType="text/csv",
    )
    print(f"  Uploaded {count:,} rows → s3://{BUCKET}/{s3_key}")


def read_state():
    """Read incremental state from S3."""
    key = f"{PREFIX}/.state.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def write_state(state):
    """Save incremental state to S3."""
    key = f"{PREFIX}/.state.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(state).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  State saved → s3://{BUCKET}/{key}")


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_products(n=NUM_PRODUCTS, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    print(f"Generating {n} products...")
    products = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        base_price = round(random.uniform(99, 49999), 2)
        products.append({
            "product_id": f"PROD{i:05d}",
            "product_name": fake.catch_phrase()[:80],
            "category": category,
            "sub_category": f"{category}_{fake.word().capitalize()}",
            "brand": fake.company()[:40],
            "mrp": round(base_price * random.uniform(1.1, 1.5), 2),
            "selling_price": base_price,
            "cost_price": round(base_price * random.uniform(0.4, 0.7), 2),
            "stock_quantity": random.randint(0, 5000),
            "is_active": random.choices([True, False], weights=[95, 5])[0],
            "created_at": random_date(730, 365).strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": random_date(365, 0).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return products


def generate_orders(products, n, start_id=1, date_fn=None):
    print(f"Generating {n} orders (start id: ORD{start_id:07d})...")
    product_ids = [p["product_id"] for p in products]
    product_price_map = {p["product_id"]: p["selling_price"] for p in products}
    date_fn = date_fn or (lambda: random_date(365, 0))

    orders = []
    for i in range(start_id, start_id + n):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = product_price_map[product_id]
        discount_pct = random.choices(
            [0, 5, 10, 15, 20, 25, 30],
            weights=[30, 15, 20, 15, 10, 7, 3]
        )[0]
        discount_amount = round(unit_price * quantity * discount_pct / 100, 2)
        gmv = round(unit_price * quantity, 2)
        net_revenue = round(gmv - discount_amount, 2)
        order_status = random.choices(
            ORDER_STATUSES, weights=[60, 15, 10, 10, 5]
        )[0]
        order_date = date_fn()

        orders.append({
            "order_id": f"ORD{i:07d}",
            "customer_id": f"CUST{random.randint(1, 50000):06d}",
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "gmv": gmv,
            "net_revenue": net_revenue,
            "order_status": order_status,
            "is_returned": order_status == "RETURNED",
            "return_reason": random.choice(RETURN_REASONS) if order_status == "RETURNED" else None,
            "city": fake.city(),
            "state": fake.state(),
            "pincode": fake.postcode(),
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "delivery_date": (order_date + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d")
            if order_status in ("DELIVERED", "RETURNED") else None,
            "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return orders


def generate_payments(orders, start_id=1):
    print(f"Generating {len(orders)} payments (start id: PAY{start_id:07d})...")
    payments = []
    for i, order in enumerate(orders, start_id):
        status = random.choices(
            PAYMENT_STATUSES, weights=[85, 5, 3, 7]
        )[0]
        payment_date = datetime.strptime(order["created_at"], "%Y-%m-%d %H:%M:%S")
        payment_date += timedelta(minutes=random.randint(0, 30))

        payments.append({
            "payment_id": f"PAY{i:07d}",
            "order_id": order["order_id"],
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_status": status,
            "amount": order["net_revenue"],
            "currency": "INR",
            "gateway": random.choice(["Razorpay", "PayU", "Paytm", "Stripe", "CCAvenue"]),
            "gateway_txn_id": fake.uuid4(),
            "failure_reason": fake.sentence(nb_words=5) if status == "FAILED" else None,
            "refund_amount": order["net_revenue"] if status == "REFUNDED" else 0,
            "payment_date": payment_date.strftime("%Y-%m-%d %H:%M:%S"),
            "created_at": payment_date.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return payments


# ── Modes ──────────────────────────────────────────────────────────────────────

def run_full():
    """Full historical load — 100K rows, fixed seed, uploads to S3."""
    print("\n[FULL LOAD] Generating 100K rows → S3")
    print("=" * 50)
    random.seed(42)
    Faker.seed(42)

    run_date = datetime.now().strftime("%Y-%m-%d")

    products = generate_products(NUM_PRODUCTS, seed=42)
    upload_to_s3(
        to_csv_buffer(products),
        f"{PREFIX}/products/run_date={run_date}/products.csv",
        len(products)
    )

    orders = generate_orders(products, NUM_ORDERS_FULL, start_id=1)
    upload_to_s3(
        to_csv_buffer(orders),
        f"{PREFIX}/orders/run_date={run_date}/orders.csv",
        len(orders)
    )

    payments = generate_payments(orders, start_id=1)
    upload_to_s3(
        to_csv_buffer(payments),
        f"{PREFIX}/payments/run_date={run_date}/payments.csv",
        len(payments)
    )

    write_state({
        "last_order_id": NUM_ORDERS_FULL,
        "last_payment_id": NUM_ORDERS_FULL,
        "last_product_id": NUM_PRODUCTS,
        "last_run_date": run_date,
        "mode": "full",
    })

    print("\nFull load complete.")
    print(f"Next incremental run starts at ORD{NUM_ORDERS_FULL + 1:07d}")


def run_incremental(n_orders=NUM_ORDERS_INCREMENTAL):
    """Daily incremental — appends today's rows as a new S3 partition."""
    print(f"\n[INCREMENTAL] Generating {n_orders} new orders → S3")
    print("=" * 50)

    state = read_state()
    if not state:
        print("ERROR: No full load state found.")
        print("Run this first:  python ingestion/generate_data.py --mode full")
        return

    next_order_id = state["last_order_id"] + 1
    next_payment_id = state["last_payment_id"] + 1
    run_date = datetime.now().strftime("%Y-%m-%d")
    run_ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    print(f"  Continuing from ORD{next_order_id:07d} | partition: {run_date}")

    products = generate_products(NUM_PRODUCTS, seed=42)

    orders = generate_orders(
        products, n=n_orders,
        start_id=next_order_id,
        date_fn=today_random_time,
    )
    upload_to_s3(
        to_csv_buffer(orders),
        f"{PREFIX}/orders/run_date={run_date}/orders_{run_ts}.csv",
        len(orders)
    )

    payments = generate_payments(orders, start_id=next_payment_id)
    upload_to_s3(
        to_csv_buffer(payments),
        f"{PREFIX}/payments/run_date={run_date}/payments_{run_ts}.csv",
        len(payments)
    )

    write_state({
        "last_order_id": next_order_id + n_orders - 1,
        "last_payment_id": next_payment_id + len(payments) - 1,
        "last_product_id": NUM_PRODUCTS,
        "last_run_date": run_date,
        "mode": "incremental",
    })

    print("\nIncremental load complete.")
    print(f"Next run starts at ORD{next_order_id + n_orders:07d}")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce data generator → S3")
    parser.add_argument(
        "--mode", choices=["full", "incremental"], default="full",
        help="full = historical backfill | incremental = today's new rows only"
    )
    parser.add_argument(
        "--orders", type=int, default=NUM_ORDERS_INCREMENTAL,
        help="Orders to generate in incremental mode (default: 500)"
    )
    args = parser.parse_args()

    if args.mode == "full":
        run_full()
    else:
        run_incremental(n_orders=args.orders)
        
        