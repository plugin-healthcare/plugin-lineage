# tests/conftest.py
# Shared fixtures are added here as phases progress.
import pyarrow as pa
import pytest

# ---------------------------------------------------------------------------
# Phase 3 — SQL fixture strings
# ---------------------------------------------------------------------------

SIMPLE_SELECT_SQL = "SELECT price, quantity FROM orders"

JOIN_SQL = """
    SELECT o.customer_id, i.price
    FROM orders o
    JOIN items i ON o.item_id = i.id
"""

CTE_SQL = """
    WITH enriched AS (
        SELECT o.id, i.price * o.qty AS total
        FROM orders o JOIN items i ON o.item_id = i.id
    )
    SELECT id, total FROM enriched
"""

DERIVED_SQL = "SELECT price * 0.9 AS discounted_price FROM products"
RENAME_SQL = "SELECT price AS unit_price FROM products"
STAR_SQL = "SELECT * FROM orders"


@pytest.fixture
def simple_select_sql():
    return SIMPLE_SELECT_SQL


@pytest.fixture
def join_sql():
    return JOIN_SQL


@pytest.fixture
def cte_sql():
    return CTE_SQL


@pytest.fixture
def derived_sql():
    return DERIVED_SQL


@pytest.fixture
def rename_sql():
    return RENAME_SQL


@pytest.fixture
def star_sql():
    return STAR_SQL


@pytest.fixture
def orders_schema():
    """Arrow schema for the 'orders' table used in SELECT * tests."""
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("price", pa.float64()),
            pa.field("qty", pa.int64()),
        ]
    )
