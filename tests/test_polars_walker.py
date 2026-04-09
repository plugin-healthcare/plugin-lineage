# tests/test_polars_walker.py
import json
import pathlib

import pyarrow as pa
import pytest

from lineage.models import ColumnLineage
from lineage.walkers.polars_walker import PolarsWalker

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load(name: str) -> str:
    return (FIXTURES / name).read_text()


def _walk(
    fixture: str,
    asset_name: str = "my_asset",
    output_schema: pa.Schema | None = None,
) -> list[ColumnLineage]:
    if output_schema is None:
        output_schema = pa.schema([])
    walker = PolarsWalker()
    return walker.walk(_load(fixture), output_schema, asset_name)


def _walker(
    fixture: str,
    asset_name: str = "my_asset",
    output_schema: pa.Schema | None = None,
) -> PolarsWalker:
    if output_schema is None:
        output_schema = pa.schema([])
    walker = PolarsWalker()
    walker.walk(_load(fixture), output_schema, asset_name)
    return walker


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPolarsWalker:
    # ── Select ──────────────────────────────────────────────────────────────

    def test_simple_select(self):
        """Select with 2 Column exprs → 2 direct ColumnLineage objects."""
        results = _walk("polars_select.json")
        assert len(results) == 2
        cols = {r.target_column for r in results}
        assert cols == {"a", "b"}
        assert all(r.lineage_type == "direct" for r in results)

    # ── HStack ──────────────────────────────────────────────────────────────

    def test_with_columns_derived(self):
        """HStack with BinaryExpr alias → lineage_type='derived', expression set."""
        results = _walk("polars_with_columns.json")
        derived = [r for r in results if r.target_column == "sum_ab"]
        assert len(derived) >= 1
        assert all(r.lineage_type == "derived" for r in derived)
        assert all(r.expression is not None for r in derived)

    def test_with_columns_direct(self):
        """A plain Column alias inside HStack → lineage_type='direct'."""
        # Build a minimal HStack plan where alias wraps a plain Column
        plan = json.dumps(
            {
                "HStack": {
                    "input": {
                        "DataFrameScan": {
                            "schema": {"fields": {"x": "Int64"}, "metadata": None}
                        }
                    },
                    "exprs": [{"Alias": [{"Column": "x"}, "x_copy"]}],
                    "options": {},
                }
            }
        )
        walker = PolarsWalker()
        results = walker.walk(plan, pa.schema([]), "my_asset")
        direct = [r for r in results if r.target_column == "x_copy"]
        assert len(direct) >= 1
        assert all(r.lineage_type == "direct" for r in direct)

    # ── Filter ──────────────────────────────────────────────────────────────

    def test_filter_predicate_columns(self):
        """Filter predicate referencing 'age' → at least one edge with lineage_type='filter'."""
        results = _walk("polars_filter.json")
        filter_edges = [r for r in results if r.lineage_type == "filter"]
        assert len(filter_edges) >= 1
        filter_cols = {r.source_column for r in filter_edges}
        assert "age" in filter_cols

    # ── Join ────────────────────────────────────────────────────────────────

    def test_join_registers_identity(self):
        """left_on=[Column 'id'], right_on=[Column 'user_id'] → one IdentityGroup with sorted key."""
        walker = _walker("polars_join.json")
        assert len(walker.identity_groups) >= 1
        keys = {g.key for g in walker.identity_groups}
        # Key must contain both "id" and "user_id" in sorted order
        matching = [k for k in keys if "id" in k and "user_id" in k]
        assert len(matching) >= 1
        # The key must be in sorted order (alphabetical)
        key = matching[0]
        parts = key.split("::")
        assert parts == sorted(parts)

    def test_join_multikey_two_identities(self):
        """Two key pairs (org_id, user_id) → two IdentityGroup entries."""
        walker = _walker("polars_join_multikey.json")
        assert len(walker.identity_groups) >= 2

    # ── Opaque ──────────────────────────────────────────────────────────────

    def test_opaque_anonymous_function(self):
        """AnonymousFunction with int-array blob → lineage_type='opaque' for the alias."""
        results = _walk("polars_opaque.json")
        opaque = [r for r in results if r.target_column == "b_mapped"]
        assert len(opaque) >= 1
        assert all(r.lineage_type == "opaque" for r in opaque)

    # ── MapFunction Rename ───────────────────────────────────────────────────

    def test_rename_via_map_function(self):
        """MapFunction(Rename) → direct lineage with remapped column names."""
        plan = json.dumps(
            {
                "MapFunction": {
                    "input": {
                        "DataFrameScan": {
                            "schema": {
                                "fields": {"price": "Float64", "qty": "Int64"},
                                "metadata": None,
                            }
                        }
                    },
                    "function": {
                        "Rename": {
                            "existing": ["price"],
                            "new": ["unit_price"],
                            "strict": True,
                        }
                    },
                }
            }
        )
        walker = PolarsWalker()
        results = walker.walk(plan, pa.schema([]), "my_asset")
        renamed = [r for r in results if r.target_column == "unit_price"]
        assert len(renamed) >= 1
        assert all(r.lineage_type == "direct" for r in renamed)
        assert all(r.source_column == "price" for r in renamed)

    # ── Chained pipeline ─────────────────────────────────────────────────────

    def test_chained_pipeline(self):
        """filter→hstack→select chain → includes direct, derived, and filter edges."""
        results = _walk("polars_chained.json")
        types = {r.lineage_type for r in results}
        # Must have at least direct and derived; filter may be included
        assert "direct" in types
        assert "derived" in types

    def test_chained_all_output_columns_covered(self):
        """Chained plan selects patient_id, diagnosis, age_doubled → all three present."""
        results = _walk("polars_chained.json")
        target_cols = {r.target_column for r in results}
        assert "patient_id" in target_cols
        assert "diagnosis" in target_cols
        assert "age_doubled" in target_cols

    # ── Schema extraction ─────────────────────────────────────────────────────

    def test_schema_extraction(self):
        """DataFrameScan schema fields parsed correctly."""
        results = _walk("polars_select.json")
        # Source columns must come from the DataFrameScan schema {a: Int64, b: String}
        source_cols = {r.source_column for r in results}
        assert source_cols == {"a", "b"}

    # ── GroupBy ──────────────────────────────────────────────────────────────

    def test_groupby_key_is_direct(self):
        """GroupBy key column → lineage_type='direct'."""
        results = _walk("polars_groupby.json")
        dept_edges = [r for r in results if r.target_column == "dept"]
        assert len(dept_edges) >= 1
        assert all(r.lineage_type == "direct" for r in dept_edges)

    def test_groupby_agg_is_derived(self):
        """GroupBy agg output → lineage_type='derived'."""
        results = _walk("polars_groupby.json")
        agg_edges = [r for r in results if r.target_column == "total_salary"]
        assert len(agg_edges) >= 1
        assert all(r.lineage_type == "derived" for r in agg_edges)

    # ── Scan node ────────────────────────────────────────────────────────────

    def test_scan_node_uses_output_schema(self):
        """Scan node (file-backed) → column names taken from output_schema arg."""
        plan = json.dumps(
            {
                "Select": {
                    "expr": [{"Column": "patient_id"}, {"Column": "age"}],
                    "input": {
                        "Scan": {
                            "sources": {"Paths": [{"inner": "/data/patients.csv"}]},
                            "unified_scan_args": {},
                            "scan_type": {"Csv": {}},
                        }
                    },
                    "options": {},
                }
            }
        )
        output_schema = pa.schema(
            [
                pa.field("patient_id", pa.int64()),
                pa.field("age", pa.int32()),
            ]
        )
        walker = PolarsWalker()
        results = walker.walk(plan, output_schema, "patients_asset")
        target_cols = {r.target_column for r in results}
        assert "patient_id" in target_cols
        assert "age" in target_cols
        assert all(r.target_table == "patients_asset" for r in results)

    # ── target_table ─────────────────────────────────────────────────────────

    def test_target_table_is_asset_name(self):
        """All ColumnLineage.target_table == asset_name."""
        results = _walk("polars_select.json", asset_name="my_special_asset")
        assert len(results) > 0
        assert all(r.target_table == "my_special_asset" for r in results)
