# tests/test_sql_walker.py
import pyarrow as pa
import pytest

from lineage.models import ColumnLineage
from lineage.walkers.sql_walker import SQLLineageWalker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _walk(
    sql: str, asset_name: str = "my_asset", schema: pa.Schema | None = None
) -> list[ColumnLineage]:
    if schema is None:
        schema = pa.schema([])
    walker = SQLLineageWalker()
    return walker.walk(sql, schema, asset_name)


def _walker(sql: str, asset_name: str = "my_asset", schema: pa.Schema | None = None):
    """Return the walker instance so identity_groups can also be inspected."""
    if schema is None:
        schema = pa.schema([])
    walker = SQLLineageWalker()
    walker.walk(sql, schema, asset_name)
    return walker


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSQLLineageWalker:
    def test_simple_direct(self, simple_select_sql):
        results = _walk("SELECT price FROM orders")
        assert len(results) == 1
        r = results[0]
        assert r.source_table == "orders"
        assert r.source_column == "price"
        assert r.target_column == "price"
        assert r.lineage_type == "direct"

    def test_all_output_columns_covered(self, simple_select_sql):
        results = _walk(simple_select_sql)
        target_cols = {r.target_column for r in results}
        assert target_cols == {"price", "quantity"}
        assert len(results) == 2

    def test_join_two_sources(self, join_sql):
        results = _walk(join_sql)
        source_tables = {r.source_table for r in results}
        assert "orders" in source_tables
        assert "items" in source_tables

    def test_derived_expression(self, derived_sql):
        results = _walk(derived_sql)
        derived = [r for r in results if r.target_column == "discounted_price"]
        assert len(derived) >= 1
        assert all(r.lineage_type == "derived" for r in derived)
        # Expression must mention both price and 0.9
        exprs = " ".join(r.expression for r in derived if r.expression)
        assert "price" in exprs
        assert "0.9" in exprs

    def test_cte_resolves_to_base_table(self, cte_sql):
        results = _walk(cte_sql)
        source_tables = {r.source_table for r in results}
        # Should resolve through CTE to base tables — never "enriched"
        assert "enriched" not in source_tables
        assert source_tables <= {"orders", "items"}

    def test_rename_alias(self, rename_sql):
        results = _walk(rename_sql)
        assert len(results) == 1
        r = results[0]
        assert r.source_column == "price"
        assert r.target_column == "unit_price"
        assert r.lineage_type == "direct"

    def test_select_star_expands(self, star_sql, orders_schema):
        results = _walk(star_sql, schema=orders_schema)
        target_cols = {r.target_column for r in results}
        assert target_cols == {"id", "price", "qty"}
        assert all(r.source_table == "orders" for r in results)

    def test_join_registers_identity(self, join_sql):
        walker = _walker(join_sql)
        assert len(walker.identity_groups) >= 1
        keys = {g.key for g in walker.identity_groups}
        assert "items.id::orders.item_id" in keys

    def test_target_table_is_asset_name(self, simple_select_sql):
        results = _walk(simple_select_sql, asset_name="sales_asset")
        assert all(r.target_table == "sales_asset" for r in results)

    def test_invalid_sql_raises(self):
        with pytest.raises((ValueError, Exception)):
            _walk("NOT VALID SQL !@#$")
