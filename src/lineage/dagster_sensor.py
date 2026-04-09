# src/lineage/dagster_sensor.py
"""Phase 7 — Dagster Sensor.

Monitors AssetMaterialization events and writes raw plan files to
``metadata/raw/`` for offline processing by ``plugin-lineage ingest``.

Two required metadata keys per asset materialisation:

    lineage_plan   — SQL string OR Polars plan JSON string
    lineage_schema — {col: type_str} JSON dict OR base64-encoded Arrow IPC bytes

Public API
----------
process_materialization(mat, raw_dir) -> pa.Schema | None
    Core testable unit. Extracts plan + schema from *mat*, writes files to
    *raw_dir*, and returns the reconstructed Arrow schema (or None on error).

build_lineage_sensor(raw_dir) -> SensorDefinition
    Dagster sensor factory. Returns a ``@sensor`` definition that calls
    ``process_materialization`` for every ``AssetMaterialization`` event.
"""

from __future__ import annotations

import base64
import json
import logging
import pathlib
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (mirrors cli.py)
# ---------------------------------------------------------------------------

_POLARS_NODE_TYPES = {
    "Select",
    "HStack",
    "Filter",
    "Join",
    "DataFrameScan",
    "Scan",
    "IR",
    "MapFunction",
    "GroupBy",
    "Sort",
    "Slice",
}


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def _deserialize_schema(value: str) -> pa.Schema:
    """Reconstruct a ``pa.Schema`` from a JSON dict or base64 IPC bytes string.

    Supports two formats:
    1. JSON ``{col: type_str}`` — e.g. ``{"id": "int64", "name": "string"}``
       or Polars-style ``{"id": "Int64", "amount": "Float64"}``
    2. base64-encoded Arrow IPC schema bytes
    """
    # Try JSON dict first
    try:
        data = json.loads(value)
        if isinstance(data, dict):
            fields = []
            for name, type_str in data.items():
                fields.append(pa.field(name, pa.lib.ensure_type(type_str)))
            return pa.schema(fields)
    except Exception:
        pass

    # Fall back to base64-encoded IPC bytes
    raw = base64.b64decode(value)
    reader = pa.BufferReader(raw)
    return pa.ipc.read_schema(reader)


def _detect_plan_type(content: str) -> str:
    """Return ``'sql'`` or ``'polars'`` based on content heuristic."""
    try:
        data = json.loads(content)
        if isinstance(data, dict) and next(iter(data)) in _POLARS_NODE_TYPES:
            return "polars"
    except (json.JSONDecodeError, StopIteration, ValueError):
        pass
    return "sql"


# ---------------------------------------------------------------------------
# Core function
# ---------------------------------------------------------------------------


def process_materialization(
    mat: Any,
    raw_dir: pathlib.Path,
) -> pa.Schema | None:
    """Extract lineage plan and schema from *mat* and write raw files.

    Parameters
    ----------
    mat:
        A Dagster ``AssetMaterialization`` (or compatible mock).
    raw_dir:
        Directory to write ``<asset_name>.<ext>`` and
        ``<asset_name>.schema.json`` files into.

    Returns
    -------
    pa.Schema | None
        The reconstructed Arrow output schema, or ``None`` if required
        metadata keys are missing.
    """
    asset_name = "_".join(mat.asset_key.path)

    # Validate required keys
    if "lineage_plan" not in mat.metadata:
        logger.warning(
            "Asset '%s' is missing 'lineage_plan' metadata key — skipping.",
            asset_name,
        )
        return None

    if "lineage_schema" not in mat.metadata:
        logger.warning(
            "Asset '%s' is missing 'lineage_schema' metadata key — skipping.",
            asset_name,
        )
        return None

    plan_content: str = mat.metadata["lineage_plan"].value
    schema_content: str = mat.metadata["lineage_schema"].value

    # Reconstruct Arrow schema
    try:
        schema = _deserialize_schema(schema_content)
    except Exception as exc:
        logger.warning(
            "Asset '%s': failed to deserialize 'lineage_schema': %s — skipping.",
            asset_name,
            exc,
        )
        return None

    # Determine plan type and extension
    plan_type = _detect_plan_type(plan_content)
    ext = ".json" if plan_type == "polars" else ".sql"

    # Ensure output directory exists
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Write plan file
    plan_file = raw_dir / f"{asset_name}{ext}"
    plan_file.write_text(plan_content, encoding="utf-8")

    # Write schema as {col: type_str} JSON
    schema_dict = {field.name: str(field.type) for field in schema}
    schema_file = raw_dir / f"{asset_name}.schema.json"
    schema_file.write_text(json.dumps(schema_dict, indent=2), encoding="utf-8")

    logger.info(
        "Asset '%s': wrote %s and %s.",
        asset_name,
        plan_file.name,
        schema_file.name,
    )
    return schema


# ---------------------------------------------------------------------------
# Dagster sensor factory
# ---------------------------------------------------------------------------


def build_lineage_sensor(
    raw_dir: pathlib.Path = pathlib.Path("metadata/raw"),
) -> Any:
    """Return a Dagster ``@sensor`` that calls ``process_materialization``.

    Import is deferred so that the module can be imported in environments
    where ``dagster`` is not installed (e.g. during unit tests).

    Parameters
    ----------
    raw_dir:
        Directory where raw plan files are written.
    """
    try:
        from dagster import RunRequest, sensor, SensorEvaluationContext
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "dagster is required to use build_lineage_sensor(). "
            "Install it with: pip install dagster"
        ) from exc

    @sensor(name="lineage_sensor", minimum_interval_seconds=30)
    def _lineage_sensor(context: SensorEvaluationContext):
        for event in context.instance.get_event_records():
            mat = event.asset_materialization
            if mat is not None:
                process_materialization(mat, raw_dir=raw_dir)

    return _lineage_sensor
