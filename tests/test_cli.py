# tests/test_cli.py
"""Phase 6 — CLI tests.

All tests use ``typer.testing.CliRunner`` with ``mix_stderr=False`` so that
stdout and stderr can be inspected independently.  Filesystem tests use
``tmp_path`` to stay hermetic.
"""

import json
import pathlib
import subprocess

import pyarrow as pa
import pytest
from typer.testing import CliRunner

from lineage.cli import app

runner = CliRunner(mix_stderr=False)

# ---------------------------------------------------------------------------
# Helpers shared across test classes
# ---------------------------------------------------------------------------


def _write_sql_plan(
    raw_dir: pathlib.Path, asset_name: str, sql: str, schema: pa.Schema
) -> None:
    """Write a .sql plan and companion .schema.json into raw_dir."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"{asset_name}.sql").write_text(sql)
    schema_dict = {
        schema.field(i).name: str(schema.field(i).type) for i in range(len(schema))
    }
    (raw_dir / f"{asset_name}.schema.json").write_text(json.dumps(schema_dict))


def _write_polars_plan(
    raw_dir: pathlib.Path, asset_name: str, plan: dict, schema: pa.Schema
) -> None:
    """Write a .json Polars plan and companion .schema.json into raw_dir."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / f"{asset_name}.json").write_text(json.dumps(plan))
    schema_dict = {
        schema.field(i).name: str(schema.field(i).type) for i in range(len(schema))
    }
    (raw_dir / f"{asset_name}.schema.json").write_text(json.dumps(schema_dict))


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


class TestIngest:
    def test_ingest_sql_plan(self, tmp_path):
        """SQL plan + schema in raw/ → schema and mapping files written."""
        raw = tmp_path / "raw"
        _write_sql_plan(
            raw,
            "orders",
            "SELECT id, price FROM orders",
            pa.schema([pa.field("id", pa.int64()), pa.field("price", pa.float64())]),
        )

        result = runner.invoke(
            app,
            [
                "ingest",
                "--raw-dir",
                str(raw),
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "schemas" / "orders.yaml").exists()
        assert (tmp_path / "mappings" / "orders.mapping.yaml").exists()

    def test_ingest_polars_plan(self, tmp_path):
        """Polars JSON plan in raw/ → correct mapping written."""
        raw = tmp_path / "raw"
        plan = {
            "Select": {
                "expr": [{"Column": "a"}, {"Column": "b"}],
                "input": {
                    "DataFrameScan": {
                        "schema": {
                            "fields": {"a": "Int64", "b": "String"},
                            "metadata": None,
                        }
                    }
                },
                "options": {},
            }
        }
        _write_polars_plan(
            raw,
            "my_asset",
            plan,
            pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.utf8())]),
        )

        result = runner.invoke(
            app,
            [
                "ingest",
                "--raw-dir",
                str(raw),
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert (tmp_path / "mappings" / "my_asset.mapping.yaml").exists()

    def test_ingest_empty_raw(self, tmp_path):
        """Empty raw/ dir → exit 0 with informational message."""
        raw = tmp_path / "raw"
        raw.mkdir()
        result = runner.invoke(
            app,
            [
                "ingest",
                "--raw-dir",
                str(raw),
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 0
        assert (
            "no" in result.output.lower()
            or "empty" in result.output.lower()
            or "0" in result.output
        )

    def test_ingest_invalid_plan(self, tmp_path):
        """Garbled SQL/JSON content → exit 1 with error message."""
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "bad_asset.sql").write_text("NOT !@#$ VALID SQL !!!!")
        (raw / "bad_asset.schema.json").write_text('{"id": "int64"}')
        result = runner.invoke(
            app,
            [
                "ingest",
                "--raw-dir",
                str(raw),
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# lint
# ---------------------------------------------------------------------------


class TestLint:
    def _setup_valid(self, tmp_path: pathlib.Path) -> None:
        """Write a valid schema + mapping pair to tmp_path."""
        raw = tmp_path / "raw"
        _write_sql_plan(
            raw,
            "orders",
            "SELECT id, price FROM orders",
            pa.schema([pa.field("id", pa.int64()), pa.field("price", pa.float64())]),
        )
        runner.invoke(
            app,
            [
                "ingest",
                "--raw-dir",
                str(raw),
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )

    def test_lint_all_valid(self, tmp_path):
        """Valid schemas + mappings → exit 0, 'passed' in output."""
        self._setup_valid(tmp_path)
        result = runner.invoke(
            app,
            [
                "lint",
                "--schemas-dir",
                str(tmp_path / "schemas"),
                "--mappings-dir",
                str(tmp_path / "mappings"),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "pass" in result.output.lower()

    def test_lint_missing_source_column(self, tmp_path):
        """populated_from references unknown column → exit 1, column name in output."""
        from linkml_map.datamodel.transformer_model import (
            TransformationSpecification,
            ClassDerivation,
            SlotDerivation,
        )
        from linkml_runtime.dumpers import YAMLDumper
        from lineage.persistence.schema_writer import write_schema_to_file

        schemas_dir = tmp_path / "schemas"
        mappings_dir = tmp_path / "mappings"
        schemas_dir.mkdir()
        mappings_dir.mkdir()

        # Schema only has "id"
        write_schema_to_file(
            "orders", pa.schema([pa.field("id", pa.int64())]), schemas_dir
        )

        # Mapping references nonexistent "ghost_col"
        ts = TransformationSpecification(id="http://example.org/mapping/orders")
        cd = ClassDerivation(name="orders")
        cd.slot_derivations["ghost_col"] = SlotDerivation(
            name="ghost_col", populated_from="ghost_col"
        )
        ts.class_derivations.append(cd)
        (mappings_dir / "orders.mapping.yaml").write_text(YAMLDumper().dumps(ts))

        result = runner.invoke(
            app,
            [
                "lint",
                "--schemas-dir",
                str(schemas_dir),
                "--mappings-dir",
                str(mappings_dir),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 1
        assert "ghost_col" in result.output

    def test_lint_opaque_unresolved(self, tmp_path):
        """Opaque column without expr override → exit 1."""
        from linkml_map.datamodel.transformer_model import (
            TransformationSpecification,
            ClassDerivation,
            SlotDerivation,
        )
        from linkml_runtime.dumpers import YAMLDumper
        from lineage.persistence.schema_writer import write_schema_to_file

        schemas_dir = tmp_path / "schemas"
        mappings_dir = tmp_path / "mappings"
        schemas_dir.mkdir()
        mappings_dir.mkdir()

        write_schema_to_file(
            "orders", pa.schema([pa.field("val", pa.int64())]), schemas_dir
        )

        ts = TransformationSpecification(id="http://example.org/mapping/orders")
        cd = ClassDerivation(name="orders")
        sd = SlotDerivation(name="val_mapped")
        sd.comments.append("lineage: opaque")
        cd.slot_derivations["val_mapped"] = sd
        ts.class_derivations.append(cd)
        (mappings_dir / "orders.mapping.yaml").write_text(YAMLDumper().dumps(ts))

        result = runner.invoke(
            app,
            [
                "lint",
                "--schemas-dir",
                str(schemas_dir),
                "--mappings-dir",
                str(mappings_dir),
                "--identities-file",
                str(tmp_path / "identities.yaml"),
            ],
        )
        assert result.exit_code == 1
        assert "opaque" in result.output.lower()


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


class TestDiff:
    def test_diff_no_changes(self, tmp_path, monkeypatch):
        """No change from base → 'No changes detected' in output, exit 0."""
        schemas_dir = tmp_path / "schemas"
        mappings_dir = tmp_path / "mappings"
        schemas_dir.mkdir()
        mappings_dir.mkdir()

        # Patch git_show to return the same content as current
        from lineage.persistence.schema_writer import write_schema
        import pyarrow as pa

        schema_yaml = write_schema("orders", pa.schema([pa.field("id", pa.int64())]))
        (schemas_dir / "orders.yaml").write_text(schema_yaml)

        import lineage.cli as cli_module

        monkeypatch.setattr(cli_module, "_git_show", lambda ref, path, cwd: schema_yaml)

        result = runner.invoke(
            app,
            [
                "diff",
                "--schemas-dir",
                str(schemas_dir),
                "--mappings-dir",
                str(mappings_dir),
                "--base",
                "main",
            ],
        )
        assert result.exit_code == 0
        assert "no changes" in result.output.lower()

    def test_diff_breaking_deleted_column(self, tmp_path, monkeypatch):
        """Column present at base but removed now → 'Breaking' in output, exit 1."""
        from lineage.persistence.schema_writer import write_schema

        schemas_dir = tmp_path / "schemas"
        mappings_dir = tmp_path / "mappings"
        schemas_dir.mkdir()
        mappings_dir.mkdir()

        # Current: only "id"
        current_yaml = write_schema("orders", pa.schema([pa.field("id", pa.int64())]))
        (schemas_dir / "orders.yaml").write_text(current_yaml)

        # Base had "id" AND "price"
        base_yaml = write_schema(
            "orders",
            pa.schema([pa.field("id", pa.int64()), pa.field("price", pa.float64())]),
        )

        import lineage.cli as cli_module

        monkeypatch.setattr(cli_module, "_git_show", lambda ref, path, cwd: base_yaml)

        result = runner.invoke(
            app,
            [
                "diff",
                "--schemas-dir",
                str(schemas_dir),
                "--mappings-dir",
                str(mappings_dir),
                "--base",
                "main",
            ],
        )
        assert result.exit_code == 1
        assert "breaking" in result.output.lower()

    def test_diff_logic_drift(self, tmp_path, monkeypatch):
        """expr changed in SlotDerivation → 'Logic Drift' in output, exit 1."""
        from linkml_map.datamodel.transformer_model import (
            TransformationSpecification,
            ClassDerivation,
            SlotDerivation,
        )
        from linkml_runtime.dumpers import YAMLDumper

        schemas_dir = tmp_path / "schemas"
        mappings_dir = tmp_path / "mappings"
        schemas_dir.mkdir()
        mappings_dir.mkdir()

        # Current mapping: expr = "price * 0.9"
        ts = TransformationSpecification(id="http://example.org/mapping/orders")
        cd = ClassDerivation(name="orders")
        cd.slot_derivations["discounted"] = SlotDerivation(
            name="discounted", expr="price * 0.9"
        )
        ts.class_derivations.append(cd)
        current_yaml = YAMLDumper().dumps(ts)
        (mappings_dir / "orders.mapping.yaml").write_text(current_yaml)

        # Base mapping: expr = "price * 0.8"
        ts_base = TransformationSpecification(id="http://example.org/mapping/orders")
        cd_base = ClassDerivation(name="orders")
        cd_base.slot_derivations["discounted"] = SlotDerivation(
            name="discounted", expr="price * 0.8"
        )
        ts_base.class_derivations.append(cd_base)
        base_yaml = YAMLDumper().dumps(ts_base)

        import lineage.cli as cli_module

        monkeypatch.setattr(cli_module, "_git_show", lambda ref, path, cwd: base_yaml)

        result = runner.invoke(
            app,
            [
                "diff",
                "--schemas-dir",
                str(schemas_dir),
                "--mappings-dir",
                str(mappings_dir),
                "--base",
                "main",
            ],
        )
        assert result.exit_code == 1
        assert "drift" in result.output.lower() or "logic" in result.output.lower()


# ---------------------------------------------------------------------------
# name
# ---------------------------------------------------------------------------


class TestName:
    def test_name_no_pending(self, tmp_path):
        """No unresolved identities → 'Nothing to name', exit 0."""
        identities = tmp_path / "identities.yaml"
        identities.write_text("")
        result = runner.invoke(
            app,
            [
                "name",
                "--identities-file",
                str(identities),
            ],
        )
        assert result.exit_code == 0
        assert "nothing" in result.output.lower()

    def test_name_prompt_accepted(self, tmp_path, mocker):
        """questionary.select mocked → identities.yaml updated with friendly_name."""
        import yaml
        from lineage.persistence.identity_registry import IdentityRegistry
        from lineage.models import IdentityGroup, IdentityMember

        identities_path = tmp_path / "identities.yaml"
        reg = IdentityRegistry(identities_path)
        key = "items.id::orders.item_id"
        g = IdentityGroup(
            key=key,
            members=[
                IdentityMember(table="items", column="id"),
                IdentityMember(table="orders", column="item_id"),
            ],
        )
        reg.add(g)

        # Mock questionary: select returns "item_id" (the suggested default),
        # text returns "order_item_link" as the confirmed name.
        mock_q = mocker.MagicMock()
        mock_q.ask.return_value = "order_item_link"
        mocker.patch("questionary.text", return_value=mock_q)

        result = runner.invoke(
            app,
            [
                "name",
                "--identities-file",
                str(identities_path),
            ],
        )
        assert result.exit_code == 0

        # The identities.yaml should now have the friendly_name set
        raw = yaml.safe_load(identities_path.read_text())
        assert raw[key]["friendly_name"] == "order_item_link"
        assert raw[key]["resolved"] is True
