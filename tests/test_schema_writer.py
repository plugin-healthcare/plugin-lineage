# tests/test_schema_writer.py
import pathlib

import pyarrow as pa
import pytest
from linkml_runtime.linkml_model import SchemaDefinition
from linkml_runtime.loaders import YAMLLoader

from lineage.persistence.schema_writer import write_schema, arrow_to_linkml_range


class TestArrowToLinkMLRange:
    def test_integer_types(self):
        for t in [
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint8(),
            pa.uint16(),
            pa.uint32(),
            pa.uint64(),
        ]:
            assert arrow_to_linkml_range(t) == "integer", f"failed for {t}"

    def test_float_types(self):
        for t in [pa.float32(), pa.float64(), pa.float16()]:
            assert arrow_to_linkml_range(t) == "float", f"failed for {t}"

    def test_string_types(self):
        for t in [pa.utf8(), pa.large_utf8()]:
            assert arrow_to_linkml_range(t) == "string", f"failed for {t}"

    def test_boolean_type(self):
        assert arrow_to_linkml_range(pa.bool_()) == "boolean"

    def test_date_types(self):
        assert arrow_to_linkml_range(pa.date32()) == "date"
        assert arrow_to_linkml_range(pa.date64()) == "date"

    def test_complex_type_falls_back_to_string(self):
        assert arrow_to_linkml_range(pa.timestamp("us", tz="UTC")) == "string"
        assert arrow_to_linkml_range(pa.list_(pa.int64())) == "string"


class TestWriteSchema:
    def _schema(self):
        return pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.utf8()),
                pa.field("score", pa.float64()),
                pa.field("active", pa.bool_()),
            ]
        )

    def test_arrow_to_linkml_yaml(self):
        """write_schema returns a YAML string that can be loaded as SchemaDefinition."""
        yaml_str = write_schema("orders", self._schema())
        assert isinstance(yaml_str, str)
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        assert loaded is not None

    def test_type_mapping_integer(self):
        schema = pa.schema([pa.field("id", pa.int64())])
        yaml_str = write_schema("orders", schema)
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        assert loaded.slots["id"].range == "integer"

    def test_type_mapping_float(self):
        # str(pa.float64()) == "double" — must map to range "float"
        schema = pa.schema([pa.field("price", pa.float64())])
        yaml_str = write_schema("orders", schema)
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        assert loaded.slots["price"].range == "float"

    def test_type_mapping_complex(self):
        schema = pa.schema([pa.field("created_at", pa.timestamp("us", tz="UTC"))])
        yaml_str = write_schema("orders", schema)
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        assert loaded.slots["created_at"].range == "string"
        ann = loaded.slots["created_at"].annotations.get("arrow_type")
        assert ann is not None
        assert "timestamp" in ann.value

    def test_annotation_always_present(self):
        """Every slot has an arrow_type annotation."""
        yaml_str = write_schema("orders", self._schema())
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        for slot_name, slot in loaded.slots.items():
            assert "arrow_type" in slot.annotations, (
                f"slot '{slot_name}' missing arrow_type annotation"
            )

    def test_roundtrip_yaml(self):
        """write_schema YAML → YAMLLoader → correct slots and class name."""
        yaml_str = write_schema("orders", self._schema())
        loaded = YAMLLoader().loads(yaml_str, target_class=SchemaDefinition)
        assert set(loaded.slots.keys()) == {"id", "name", "score", "active"}
        assert "orders" in loaded.classes

    def test_write_to_file(self, tmp_path):
        """write_schema_to_file creates a valid YAML file with correct class name."""
        from lineage.persistence.schema_writer import write_schema_to_file

        out = write_schema_to_file("orders", self._schema(), tmp_path)
        assert out.exists()
        loaded = YAMLLoader().loads(out.read_text(), target_class=SchemaDefinition)
        assert "orders" in loaded.classes
        assert set(loaded.slots.keys()) == {"id", "name", "score", "active"}
