# tests/test_dagster_sensor.py
"""Phase 7 — Dagster Sensor tests.

Tests call process_materialization() directly with mock AssetMaterialization
objects — no running Dagster instance needed.
"""

from __future__ import annotations

import base64
import json
import logging
import pathlib
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from lineage.dagster_sensor import process_materialization


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mat(asset_key_path: list[str], metadata: dict) -> MagicMock:
    """Build a mock AssetMaterialization."""
    mat = MagicMock()
    mat.asset_key.path = asset_key_path
    mat.metadata = {k: MagicMock(value=v) for k, v in metadata.items()}
    return mat


def _schema_json(schema: pa.Schema) -> str:
    """Serialize Arrow schema as {col: type_str} JSON string."""
    return json.dumps({field.name: str(field.type) for field in schema})


def _schema_b64(schema: pa.Schema) -> str:
    """Serialize Arrow schema as base64-encoded IPC bytes."""
    raw = schema.serialize().to_pybytes()
    return base64.b64encode(raw).decode()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_SQL = "SELECT id, name FROM users"

SIMPLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
    ]
)

POLARS_PLAN = json.dumps(
    {
        "Select": {
            "input": {
                "DataFrameScan": {
                    "schema": {"fields": {"id": "Int64", "amount": "Float64"}},
                    "scan_fn": None,
                    "filter": None,
                    "output_schema": None,
                }
            },
            "expr": [
                {"Alias": {"expr": {"Column": "id"}, "name": "id"}},
                {"Alias": {"expr": {"Column": "amount"}, "name": "amount"}},
            ],
            "schema": {"fields": {"id": "Int64", "amount": "Float64"}},
        }
    }
)

POLARS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("amount", pa.float64()),
    ]
)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSensorExtractsSqlPlan:
    def test_writes_sql_file(self, tmp_path):
        mat = _make_mat(
            ["my_asset"],
            {
                "lineage_plan": SIMPLE_SQL,
                "lineage_schema": _schema_json(SIMPLE_SCHEMA),
            },
        )
        process_materialization(mat, raw_dir=tmp_path)
        sql_file = tmp_path / "my_asset.sql"
        assert sql_file.exists(), ".sql file should be written for SQL plan"
        assert sql_file.read_text() == SIMPLE_SQL

    def test_writes_schema_file(self, tmp_path):
        mat = _make_mat(
            ["my_asset"],
            {
                "lineage_plan": SIMPLE_SQL,
                "lineage_schema": _schema_json(SIMPLE_SCHEMA),
            },
        )
        process_materialization(mat, raw_dir=tmp_path)
        schema_file = tmp_path / "my_asset.schema.json"
        assert schema_file.exists(), "schema JSON file should be written"
        data = json.loads(schema_file.read_text())
        assert data == {"id": "int64", "name": "string"}

    def test_multipart_asset_key(self, tmp_path):
        """Multi-segment asset key path is joined with underscores."""
        mat = _make_mat(
            ["warehouse", "orders"],
            {
                "lineage_plan": SIMPLE_SQL,
                "lineage_schema": _schema_json(SIMPLE_SCHEMA),
            },
        )
        process_materialization(mat, raw_dir=tmp_path)
        assert (tmp_path / "warehouse_orders.sql").exists()
        assert (tmp_path / "warehouse_orders.schema.json").exists()


class TestSensorExtractsPolarsPlan:
    def test_writes_json_file(self, tmp_path):
        mat = _make_mat(
            ["polars_asset"],
            {
                "lineage_plan": POLARS_PLAN,
                "lineage_schema": _schema_json(POLARS_SCHEMA),
            },
        )
        process_materialization(mat, raw_dir=tmp_path)
        json_file = tmp_path / "polars_asset.json"
        assert json_file.exists(), ".json file should be written for Polars plan"
        assert json.loads(json_file.read_text()) == json.loads(POLARS_PLAN)

    def test_writes_schema_file(self, tmp_path):
        mat = _make_mat(
            ["polars_asset"],
            {
                "lineage_plan": POLARS_PLAN,
                "lineage_schema": _schema_json(POLARS_SCHEMA),
            },
        )
        process_materialization(mat, raw_dir=tmp_path)
        schema_file = tmp_path / "polars_asset.schema.json"
        assert schema_file.exists()
        data = json.loads(schema_file.read_text())
        assert "id" in data
        assert "amount" in data


class TestSensorMissingKeyWarns:
    def test_no_lineage_plan_key_returns_none(self, tmp_path, caplog):
        mat = _make_mat(
            ["some_asset"],
            {
                "lineage_schema": _schema_json(SIMPLE_SCHEMA),
            },
        )
        with caplog.at_level(logging.WARNING, logger="lineage.dagster_sensor"):
            result = process_materialization(mat, raw_dir=tmp_path)
        assert result is None
        assert not list(tmp_path.iterdir()), "No files should be written on missing key"

    def test_no_lineage_schema_key_returns_none(self, tmp_path, caplog):
        mat = _make_mat(
            ["some_asset"],
            {
                "lineage_plan": SIMPLE_SQL,
            },
        )
        with caplog.at_level(logging.WARNING, logger="lineage.dagster_sensor"):
            result = process_materialization(mat, raw_dir=tmp_path)
        assert result is None
        assert not list(tmp_path.iterdir())

    def test_warning_message_contains_asset_name(self, tmp_path, caplog):
        mat = _make_mat(["my_missing_asset"], {})
        with caplog.at_level(logging.WARNING, logger="lineage.dagster_sensor"):
            process_materialization(mat, raw_dir=tmp_path)
        assert "my_missing_asset" in caplog.text


class TestSensorSchemaDeserialization:
    def test_json_dict_schema(self, tmp_path):
        """JSON {col: type_str} format → correct pa.Schema reconstructed."""
        mat = _make_mat(
            ["schema_asset"],
            {
                "lineage_plan": SIMPLE_SQL,
                "lineage_schema": _schema_json(SIMPLE_SCHEMA),
            },
        )
        result = process_materialization(mat, raw_dir=tmp_path)
        assert result is not None
        assert result.field("id").type == pa.int64()
        assert result.field("name").type == pa.string()

    def test_base64_ipc_schema(self, tmp_path):
        """base64-encoded IPC bytes → correct pa.Schema reconstructed."""
        mat = _make_mat(
            ["schema_asset"],
            {
                "lineage_plan": SIMPLE_SQL,
                "lineage_schema": _schema_b64(SIMPLE_SCHEMA),
            },
        )
        result = process_materialization(mat, raw_dir=tmp_path)
        assert result is not None
        assert result.field("id").type == pa.int64()
        assert result.field("name").type == pa.string()

    def test_polars_type_strings(self, tmp_path):
        """Polars-style type strings (Int64, Float64) round-trip correctly."""
        schema_dict = {"id": "Int64", "amount": "Float64"}
        mat = _make_mat(
            ["polars_schema_asset"],
            {
                "lineage_plan": POLARS_PLAN,
                "lineage_schema": json.dumps(schema_dict),
            },
        )
        result = process_materialization(mat, raw_dir=tmp_path)
        assert result is not None
        # Int64 / Float64 (Polars-style) round-trip — types are equivalent
        assert result.field("id").type in (pa.int64(),)
        assert result.field("amount").type in (pa.float64(),)
