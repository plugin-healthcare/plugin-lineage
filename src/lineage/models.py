# src/lineage/models.py
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


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
