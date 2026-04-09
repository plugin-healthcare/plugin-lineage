# src/lineage/persistence/mapping_writer.py
from __future__ import annotations

import pathlib

from linkml_map.datamodel.transformer_model import (
    ClassDerivation,
    SlotDerivation,
    TransformationSpecification,
)
from linkml_runtime.dumpers import YAMLDumper
from linkml_runtime.loaders import YAMLLoader

from lineage.models import ColumnLineage

# ---------------------------------------------------------------------------
# SlotDerivation construction rules
# ---------------------------------------------------------------------------
# lineage_type  | SlotDerivation fields
# --------------|--------------------------------------------------------------
# direct        | populated_from=source_column
# derived       | expr=expression  (populated_from left None)
# filter        | populated_from=source_column, comments=["lineage: filter"]
# opaque        | comments=["lineage: opaque"]  (no populated_from, no expr)
# ---------------------------------------------------------------------------


def _edge_to_slot_derivation(edge: ColumnLineage) -> SlotDerivation:
    """Convert a single :class:`~lineage.models.ColumnLineage` edge to a ``SlotDerivation``."""
    sd = SlotDerivation(name=edge.target_column)

    if edge.lineage_type == "derived":
        sd.expr = edge.expression
    elif edge.lineage_type == "filter":
        sd.populated_from = edge.source_column
        sd.comments.append("lineage: filter")
    elif edge.lineage_type == "opaque":
        sd.comments.append("lineage: opaque")
    else:  # direct (default)
        sd.populated_from = edge.source_column

    return sd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_transformation_spec(
    edges: list[ColumnLineage],
    asset_name: str,
) -> TransformationSpecification:
    """Build a ``TransformationSpecification`` from a list of lineage edges.

    All edges must share the same ``target_table`` (= *asset_name*).
    When multiple edges map to the same ``target_column``, the **last** one wins
    (walkers may emit duplicate edges for derived columns with multiple sources;
    callers should deduplicate before calling if exact semantics matter).

    Parameters
    ----------
    edges:
        The :class:`~lineage.models.ColumnLineage` list produced by a walker.
    asset_name:
        The Dagster asset name; becomes the ``ClassDerivation.name``.
    """
    spec = TransformationSpecification(id=f"http://example.org/mapping/{asset_name}")
    cd = ClassDerivation(name=asset_name)

    for edge in edges:
        sd = _edge_to_slot_derivation(edge)
        cd.slot_derivations[edge.target_column] = sd

    spec.class_derivations.append(cd)
    return spec


def write_mapping(edges: list[ColumnLineage], asset_name: str) -> str:
    """Return a YAML string for the ``TransformationSpecification``."""
    spec = build_transformation_spec(edges, asset_name)
    return YAMLDumper().dumps(spec)


def write_mapping_to_file(
    edges: list[ColumnLineage],
    asset_name: str,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    """Write the mapping YAML to ``output_dir/<asset_name>.mapping.yaml``.

    Creates *output_dir* if it does not exist.

    Returns
    -------
    pathlib.Path
        Path to the written file.
    """
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / f"{asset_name}.mapping.yaml"
    out.write_text(write_mapping(edges, asset_name))
    return out


def upsert_mapping_file(
    edges: list[ColumnLineage],
    asset_name: str,
    mapping_file: pathlib.Path,
) -> None:
    """Merge *edges* into an existing mapping file without overwriting existing entries.

    Pre-existing ``SlotDerivation`` entries are preserved unchanged.
    New target columns from *edges* are added.

    Parameters
    ----------
    edges:
        New lineage edges to merge in.
    asset_name:
        The Dagster asset name (must match the existing file's class derivation).
    mapping_file:
        Path to an existing ``*.mapping.yaml`` file.
    """
    mapping_file = pathlib.Path(mapping_file)
    existing = YAMLLoader().loads(
        mapping_file.read_text(), target_class=TransformationSpecification
    )

    # Find the ClassDerivation for this asset (first match by name)
    cd: ClassDerivation | None = None
    for candidate in existing.class_derivations:
        if candidate.name == asset_name:
            cd = candidate
            break

    if cd is None:
        # Asset class not yet in file — add it
        cd = ClassDerivation(name=asset_name)
        existing.class_derivations.append(cd)

    # Merge: only add slots that are not already present
    for edge in edges:
        if edge.target_column not in cd.slot_derivations:
            cd.slot_derivations[edge.target_column] = _edge_to_slot_derivation(edge)

    mapping_file.write_text(YAMLDumper().dumps(existing))
