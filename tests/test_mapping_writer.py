# tests/test_mapping_writer.py
import pathlib

import pytest
from linkml_map.datamodel.transformer_model import TransformationSpecification
from linkml_runtime.loaders import YAMLLoader

from lineage.models import ColumnLineage
from lineage.persistence.mapping_writer import (
    build_transformation_spec,
    write_mapping,
    write_mapping_to_file,
    upsert_mapping_file,
)


def _make(
    source_column: str = "id",
    target_column: str = "id",
    source_table: str = "orders",
    target_table: str = "my_asset",
    lineage_type: str = "direct",
    expression: str | None = None,
) -> ColumnLineage:
    return ColumnLineage(
        source_table=source_table,
        source_column=source_column,
        target_table=target_table,
        target_column=target_column,
        lineage_type=lineage_type,
        expression=expression,
    )


class TestSlotDerivationRules:
    def test_direct_slot_derivation(self):
        edges = [_make(source_column="id", target_column="id")]
        spec = build_transformation_spec(edges, "my_asset")
        cd = spec.class_derivations[0]
        sd = cd.slot_derivations["id"]
        assert sd.populated_from == "id"
        assert sd.expr is None

    def test_rename_slot_derivation(self):
        edges = [_make(source_column="client_ptr", target_column="customer_id")]
        spec = build_transformation_spec(edges, "my_asset")
        cd = spec.class_derivations[0]
        sd = cd.slot_derivations["customer_id"]
        assert sd.populated_from == "client_ptr"

    def test_derived_slot_derivation(self):
        edges = [
            _make(
                source_column="price",
                target_column="discounted",
                lineage_type="derived",
                expression="price * 0.9",
            )
        ]
        spec = build_transformation_spec(edges, "my_asset")
        cd = spec.class_derivations[0]
        sd = cd.slot_derivations["discounted"]
        assert sd.expr == "price * 0.9"
        assert sd.populated_from is None

    def test_filter_slot_derivation(self):
        edges = [_make(source_column="age", target_column="age", lineage_type="filter")]
        spec = build_transformation_spec(edges, "my_asset")
        cd = spec.class_derivations[0]
        sd = cd.slot_derivations["age"]
        assert sd.populated_from == "age"
        assert any("lineage: filter" in c for c in (sd.comments or []))

    def test_opaque_slot_derivation(self):
        edges = [
            _make(
                source_column="val", target_column="val_mapped", lineage_type="opaque"
            )
        ]
        spec = build_transformation_spec(edges, "my_asset")
        cd = spec.class_derivations[0]
        sd = cd.slot_derivations["val_mapped"]
        assert any("lineage: opaque" in c for c in (sd.comments or []))
        assert sd.expr is None


class TestBuildTransformationSpec:
    def test_full_transformation_spec(self):
        """Multiple ColumnLineage edges → valid TransformationSpecification."""
        edges = [
            _make("id", "id"),
            _make("price", "unit_price"),
            _make(
                "price", "discounted", lineage_type="derived", expression="price * 0.9"
            ),
        ]
        spec = build_transformation_spec(edges, "my_asset")
        assert isinstance(spec, TransformationSpecification)
        assert len(spec.class_derivations) == 1
        cd = spec.class_derivations[0]
        assert cd.name == "my_asset"
        assert "id" in cd.slot_derivations
        assert "unit_price" in cd.slot_derivations
        assert "discounted" in cd.slot_derivations

    def test_roundtrip_yaml(self, tmp_path):
        """write_mapping → YAMLLoader.loads → equal slot derivations."""
        edges = [
            _make("id", "id"),
            _make("price", "unit_price"),
            _make("cost", "total", lineage_type="derived", expression="cost * qty"),
        ]
        yaml_str = write_mapping(edges, "my_asset")
        loaded = YAMLLoader().loads(yaml_str, target_class=TransformationSpecification)
        cd = loaded.class_derivations[0]
        assert cd.slot_derivations["id"].populated_from == "id"
        assert cd.slot_derivations["unit_price"].populated_from == "price"
        assert cd.slot_derivations["total"].expr == "cost * qty"

    def test_upsert_preserves_existing(self, tmp_path):
        """upsert_mapping_file: pre-existing entries not overwritten; new ones merged in."""
        # Write initial mapping
        initial = [_make("id", "id")]
        p = write_mapping_to_file(initial, "my_asset", tmp_path)

        # Upsert with a new column, do NOT include the existing "id"
        new_edges = [_make("price", "unit_price")]
        upsert_mapping_file(new_edges, "my_asset", p)

        loaded = YAMLLoader().loads(
            p.read_text(), target_class=TransformationSpecification
        )
        cd = loaded.class_derivations[0]
        # Both should be present
        assert "id" in cd.slot_derivations
        assert "unit_price" in cd.slot_derivations
