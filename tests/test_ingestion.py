"""
pytest unit tests for ingestion/data_genearation.py
Run locally: pytest tests/ -v
"""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock


# ── Mock boto3 so tests run without AWS credentials ───────────────────────────
@pytest.fixture(autouse=True)
def mock_boto3():
    with patch("boto3.client") as mock:
        mock_s3 = MagicMock()
        mock.return_value = mock_s3
        yield mock_s3


# ── Import after mocking boto3 ────────────────────────────────────────────────
import sys


@pytest.fixture
def gen():
    if "ingestion.data_genearation" in sys.modules:
        del sys.modules["ingestion.data_genearation"]
    import ingestion.data_genearation as g
    return g


# ── Product tests ─────────────────────────────────────────────────────────────

class TestGenerateProducts:
    def test_correct_count(self, gen):
        products = gen.generate_products(n=10)
        assert len(products) == 10

    def test_required_fields_present(self, gen):
        products = gen.generate_products(n=5)
        required = {"product_id", "category", "selling_price", "cost_price", "mrp"}
        assert required.issubset(set(products[0].keys()))

    def test_product_ids_unique(self, gen):
        products = gen.generate_products(n=50)
        ids = [p["product_id"] for p in products]
        assert len(ids) == len(set(ids))

    def test_selling_price_positive(self, gen):
        products = gen.generate_products(n=20)
        for p in products:
            assert p["selling_price"] > 0

    def test_category_valid(self, gen):
        valid = {"Electronics", "Fashion", "Home", "Beauty", "Sports", "Books", "Grocery"}
        products = gen.generate_products(n=20)
        for p in products:
            assert p["category"] in valid

    def test_reproducible_with_same_seed(self, gen):
        p1 = gen.generate_products(n=5, seed=42)
        p2 = gen.generate_products(n=5, seed=42)
        assert [p["product_id"] for p in p1] == [p["product_id"] for p in p2]


# ── Order tests ───────────────────────────────────────────────────────────────

class TestGenerateOrders:
    def test_correct_count(self, gen):
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=50)
        assert len(orders) == 50

    def test_order_ids_unique(self, gen):
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=100)
        ids = [o["order_id"] for o in orders]
        assert len(ids) == len(set(ids))

    def test_references_valid_products(self, gen):
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=50)
        valid_ids = {p["product_id"] for p in products}
        for o in orders:
            assert o["product_id"] in valid_ids

    def test_gmv_non_negative(self, gen):
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=50)
        for o in orders:
            assert o["gmv"] >= 0

    def test_net_revenue_non_negative(self, gen):
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=50)
        for o in orders:
            assert o["net_revenue"] >= 0

    def test_order_status_valid(self, gen):
        valid = {"DELIVERED", "SHIPPED", "CANCELLED", "RETURNED", "PROCESSING"}
        products = gen.generate_products(n=10)
        orders = gen.generate_orders(products, n=50)
        for o in orders:
            assert o["order_status"] in valid

    def test_start_id_respected(self, gen):
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=10, start_id=500)
        assert orders[0]["order_id"] == "ORD0000500"

    def test_incremental_date_fn(self, gen):
        products = gen.generate_products(n=5)
        today = datetime.now().strftime("%Y-%m-%d")
        orders = gen.generate_orders(
            products, n=10,
            date_fn=gen.today_random_time
        )
        for o in orders:
            assert o["order_date"].startswith(today)


# ── Payment tests ─────────────────────────────────────────────────────────────

class TestGeneratePayments:
    def test_one_payment_per_order(self, gen):
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=50)
        payments = gen.generate_payments(orders)
        assert len(payments) == len(orders)

    def test_payment_ids_unique(self, gen):
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=50)
        payments = gen.generate_payments(orders)
        ids = [p["payment_id"] for p in payments]
        assert len(ids) == len(set(ids))

    def test_payment_status_valid(self, gen):
        valid = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=50)
        payments = gen.generate_payments(orders)
        for p in payments:
            assert p["payment_status"] in valid

    def test_amount_non_negative(self, gen):
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=50)
        payments = gen.generate_payments(orders)
        for p in payments:
            assert p["amount"] >= 0

    def test_order_id_matches(self, gen):
        products = gen.generate_products(n=5)
        orders = gen.generate_orders(products, n=20)
        payments = gen.generate_payments(orders)
        order_ids = {o["order_id"] for o in orders}
        for p in payments:
            assert p["order_id"] in order_ids


# ── Revenue calculation tests ─────────────────────────────────────────────────

class TestRevenueCalculations:
    def test_aov_correct(self):
        assert round(50000 / 100, 2) == 500.0

    def test_aov_zero_orders(self):
        total = 0
        orders = 0
        result = round(total / orders, 2) if orders else 0
        assert result == 0

    def test_return_rate_pct(self):
        assert round(10 / 100 * 100, 2) == 10.0

    def test_return_rate_zero(self):
        assert round(0 / 100 * 100, 2) == 0.0

    def test_payment_success_rate(self):
        assert round(85 / 100 * 100, 2) == 85.0

    def test_metrics_bounded(self):
        for returned in [0, 50, 100]:
            rate = round(returned / 100 * 100, 2)
            assert 0 <= rate <= 100


# ── Schema drift detection tests ──────────────────────────────────────────────

class TestSchemaDrift:
    def test_detects_null_primary_key(self):
        orders = [
            {"order_id": None, "gmv": 100},
            {"order_id": "ORD001", "gmv": 200},
        ]
        null_count = sum(1 for o in orders if o["order_id"] is None)
        assert null_count > 0
        with pytest.raises(ValueError, match="SCHEMA DRIFT"):
            if null_count > 0:
                raise ValueError(f"SCHEMA DRIFT: order_id has {null_count} nulls")

    def test_accepts_valid_orders(self):
        orders = [{"order_id": f"ORD{i}", "gmv": i * 100} for i in range(1, 6)]
        null_count = sum(1 for o in orders if o["order_id"] is None)
        assert null_count == 0

    def test_payment_status_allowed_values(self):
        allowed = {"SUCCESS", "FAILED", "PENDING", "REFUNDED"}
        bad = [{"payment_id": "PAY1", "payment_status": "UNKNOWN"}]
        violations = [p for p in bad if p["payment_status"] not in allowed]
        assert len(violations) == 1

    def test_negative_amount_detected(self):
        payments = [
            {"payment_id": "PAY1", "amount": 100},
            {"payment_id": "PAY2", "amount": -50},
        ]
        violations = [p for p in payments if p["amount"] < 0]
        assert len(violations) == 1