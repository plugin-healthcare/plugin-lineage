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
    ``process_materialization`` for every new ``AssetMaterialization`` event,
    using a cursor to avoid re-processing historical records on each tick.
"""

from __future__ import annotations

import base64
import json
import logging
import pathlib
from typing import Any

import pyarrow as pa

from lineage.models import POLARS_NODE_TYPES, arrow_type_from_string

logger = logging.getLogger(__name__)


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
                fields.append(pa.field(name, arrow_type_from_string(type_str)))
            return pa.schema(fields)
    except Exception:
        pass

    # Fall back to base64-encoded IPC bytes
    raw = base64.b64decode(value)
    reader = pa.BufferReader(raw)
    return pa.ipc.read_schema(reader)


def _detect_plan_type(content: str) -> str:
    """Return ``'sql'`` or ``'polars'`` based on content heuristic.

    Parses the content as JSON and checks whether *any* top-level key matches
    a known Polars IR node type.  Checking any key (rather than only the first)
    avoids false negatives when the dict ordering places non-node keys first.
    """
    try:
        data = json.loads(content)
        if isinstance(data, dict) and any(k in POLARS_NODE_TYPES for k in data):
            return "polars"
    except (json.JSONDecodeError, StopIteration):
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

    Notes
    -----
    If a plan file for the same asset already exists and its contents differ
    from the new plan, a warning is logged before overwriting.  This surfaces
    potential naming collisions (e.g. two assets whose key paths join to the
    same string).
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

    # Write plan file — warn if an existing file would be overwritten with
    # different content (potential asset-name collision or stale data).
    plan_file = raw_dir / f"{asset_name}{ext}"
    if plan_file.exists():
        existing = plan_file.read_text(encoding="utf-8")
        if existing != plan_content:
            logger.warning(
                "Asset '%s': overwriting existing plan file '%s' with different content. "
                "Check for asset-key naming collisions.",
                asset_name,
                plan_file.name,
            )
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

    The sensor uses a cursor to track the last-processed event storage ID so
    that only *new* ``AssetMaterialization`` events are processed on each tick.
    This prevents the unbounded historical scan that would occur if
    ``get_event_records()`` were called without filtering on every poll.

    Import is deferred so that the module can be imported in environments
    where ``dagster`` is not installed (e.g. during unit tests).

    Parameters
    ----------
    raw_dir:
        Directory where raw plan files are written.
    """
    try:
        from dagster import (
            DagsterEventType,
            EventRecordsFilter,
            RunRequest,
            SkipReason,
            sensor,
        )
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "dagster is required to use build_lineage_sensor(). "
            "Install it with: pip install dagster"
        ) from exc

    @sensor(name="lineage_sensor", minimum_interval_seconds=30)
    def _lineage_sensor(context):  # type: ignore[no-untyped-def]
        after_cursor = context.cursor

        records = list(
            context.instance.get_event_records(
                EventRecordsFilter(event_type=DagsterEventType.ASSET_MATERIALIZATION),
                after_cursor=after_cursor,
            )
        )

        if not records:
            yield SkipReason("No new asset materializations since last tick.")
            return

        for record in records:
            mat = record.asset_materialization
            if mat is not None:
                process_materialization(mat, raw_dir=raw_dir)

        # Advance the cursor to the last record processed so we don't
        # re-process the same events on the next sensor tick.
        context.update_cursor(str(records[-1].storage_id))
        yield RunRequest(run_key=str(records[-1].storage_id))

    return _lineage_sensor
