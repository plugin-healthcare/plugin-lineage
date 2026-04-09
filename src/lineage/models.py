# src/lineage/models.py
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

#: Set of Polars IR node-type names used to detect Polars plan JSON files.
#: Imported by both ``cli.py`` and ``dagster_sensor.py`` to avoid duplication.
POLARS_NODE_TYPES: frozenset[str] = frozenset(
    {
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
)

#: Explicit mapping from common Arrow / Polars type strings to pa.DataType.
#: Used instead of the private ``pa.lib.ensure_type`` API.
#: Covers the types most likely to appear in pipeline schemas; unknown types
#: fall back to ``pa.large_string()`` with a logged warning.
import pyarrow as pa  # noqa: E402 — placed after constants for readability

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    # Integer types
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
    # Floating-point types
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "Float32": pa.float32(),
    "Float64": pa.float64(),
    "half_float": pa.float16(),
    "float": pa.float32(),
    "double": pa.float64(),
    # Boolean
    "bool": pa.bool_(),
    "bool_": pa.bool_(),
    "Boolean": pa.bool_(),
    "boolean": pa.bool_(),
    # String / binary
    "string": pa.string(),
    "utf8": pa.string(),
    "Utf8": pa.string(),
    "large_string": pa.large_string(),
    "large_utf8": pa.large_string(),
    "String": pa.large_string(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    # Temporal
    "date32": pa.date32(),
    "date64": pa.date64(),
    "date": pa.date32(),
    "Date": pa.date32(),
    "time32[s]": pa.time32("s"),
    "time32[ms]": pa.time32("ms"),
    "time64[us]": pa.time64("us"),
    "time64[ns]": pa.time64("ns"),
    "timestamp[s]": pa.timestamp("s"),
    "timestamp[ms]": pa.timestamp("ms"),
    "timestamp[us]": pa.timestamp("us"),
    "timestamp[ns]": pa.timestamp("ns"),
    "duration[s]": pa.duration("s"),
    "duration[ms]": pa.duration("ms"),
    "duration[us]": pa.duration("us"),
    "duration[ns]": pa.duration("ns"),
    # Null
    "null": pa.null(),
    # Decimal (common precision/scale)
    "decimal128(38, 18)": pa.decimal128(38, 18),
}


def arrow_type_from_string(type_str: str) -> pa.DataType:
    """Convert a type string to a ``pa.DataType``.

    Looks up *type_str* in the canonical mapping table.  If not found, falls
    back to ``pa.large_string()`` and logs a warning so callers are aware of
    the coercion rather than silently failing.
    """
    import logging

    result = _ARROW_TYPE_MAP.get(type_str)
    if result is None:
        logging.getLogger(__name__).warning(
            "Unknown Arrow type string %r — falling back to large_string(). "
            "Add it to models._ARROW_TYPE_MAP if needed.",
            type_str,
        )
        return pa.large_string()
    return result


class ColumnLineage(BaseModel):
    """Represents a single column-level lineage edge from source to target."""

    model_config = ConfigDict(frozen=True)

    source_table: str
    source_column: str
    target_table: str  # = Dagster asset name, supplied by walker caller
    target_column: str
    lineage_type: Literal["direct", "derived", "filter", "opaque"] = "direct"
    expression: str | None = None

    @field_validator("source_column", "target_column", mode="before")
    @classmethod
    def strip_and_reject_empty(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("Column name must not be empty or whitespace-only")
        return stripped

    @model_validator(mode="after")
    def derived_requires_expression(self) -> "ColumnLineage":
        if self.lineage_type == "derived" and self.expression is None:
            raise ValueError("lineage_type='derived' requires 'expression' to be set")
        return self


class IdentityMember(BaseModel):
    """A single table.column pair that is a member of an identity group."""

    model_config = ConfigDict(frozen=True)

    table: str
    column: str


class IdentityGroup(BaseModel):
    """A group of columns that refer to the same real-world concept (e.g. join keys)."""

    model_config = ConfigDict(frozen=True)

    key: str  # "::".join(sorted("table.col" for each member))
    members: list[IdentityMember]
    friendly_name: str | None = None
    resolved: bool = False

    @model_validator(mode="after")
    def validate_key_and_resolved(self) -> "IdentityGroup":
        expected_key = "::".join(sorted(f"{m.table}.{m.column}" for m in self.members))
        if self.key != expected_key:
            raise ValueError(
                f"key '{self.key}' does not match computed key '{expected_key}'"
            )
        # Auto-set resolved if friendly_name is provided.
        # Because the model is frozen we cannot mutate after init; instead we
        # raise if the caller explicitly set resolved=False with a friendly_name.
        if self.friendly_name is not None and not self.resolved:
            # Allow: auto-resolution means resolved should be True.
            # We can't mutate a frozen model here, so we validate by accepting
            # the invariant and rely on model_post_init below.
            pass
        return self

    def model_post_init(self, __context: object) -> None:  # noqa: ANN001
        # Pydantic v2: use object.__setattr__ to bypass frozen enforcement
        # during construction only (model_post_init runs before freeze takes effect).
        if self.friendly_name is not None and not self.resolved:
            object.__setattr__(self, "resolved", True)
