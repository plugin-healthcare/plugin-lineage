# src/lineage/persistence/schema_writer.py
from __future__ import annotations

import pathlib

import pyarrow as pa
from linkml.utils.schema_builder import SchemaBuilder
from linkml_runtime.dumpers import YAMLDumper
from linkml_runtime.linkml_model.meta import Annotation

# ---------------------------------------------------------------------------
# Arrow type → LinkML range mapping
# ---------------------------------------------------------------------------

# Canonical str(pa_type) values observed from pyarrow:
#   integers : int8/16/32/64, uint8/16/32/64
#   floats   : halffloat (float16), float (float32), double (float64)
#   strings  : string, large_string
#   bool     : bool
#   dates    : date32[day], date64[ms]
#   all else → "string" with arrow_type annotation preserving the full string

_ARROW_TO_LINKML: dict[str, str] = {
    # integers
    "int8": "integer",
    "int16": "integer",
    "int32": "integer",
    "int64": "integer",
    "uint8": "integer",
    "uint16": "integer",
    "uint32": "integer",
    "uint64": "integer",
    # floats  (note: pa.float16() → "halffloat", pa.float32() → "float", pa.float64() → "double")
    "halffloat": "float",
    "float": "float",
    "double": "float",
    # strings
    "string": "string",
    "large_string": "string",
    # boolean
    "bool": "boolean",
    # dates
    "date32[day]": "date",
    "date64[ms]": "date",
}


def arrow_to_linkml_range(arrow_type: pa.DataType) -> str:
    """Return the LinkML range string for *arrow_type*.

    Falls back to ``"string"`` for any type not in the canonical mapping
    (e.g. timestamps, lists, structs).
    """
    return _ARROW_TO_LINKML.get(str(arrow_type), "string")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_schema(class_name: str, schema: pa.Schema) -> str:
    """Convert an Arrow schema to a LinkML SchemaDefinition YAML string.

    Every slot gets:
    - ``range``: the mapped LinkML type (or ``"string"`` for complex types).
    - ``annotations.arrow_type``: the exact ``str(pa_type)`` string for
      lossless round-tripping and static ``lint`` comparisons.

    Parameters
    ----------
    class_name:
        The name of the LinkML class (= Dagster asset / table name).
    schema:
        The Arrow schema of the asset output.

    Returns
    -------
    str
        YAML string suitable for writing to ``metadata/schemas/<class_name>.yaml``.
    """
    sb = SchemaBuilder(name=class_name)

    slot_names = [schema.field(i).name for i in range(len(schema))]
    sb.add_class(class_name, slots=slot_names)

    for i in range(len(schema)):
        field = schema.field(i)
        col_name = field.name
        arrow_type_str = str(field.type)
        linkml_range = arrow_to_linkml_range(field.type)

        sb.set_slot(col_name, range=linkml_range)
        # Add arrow_type annotation to every slot (always, regardless of range)
        sb.schema.slots[col_name].annotations["arrow_type"] = Annotation(
            tag="arrow_type",
            value=arrow_type_str,
        )

    return YAMLDumper().dumps(sb.schema)


def write_schema_to_file(
    class_name: str,
    schema: pa.Schema,
    output_dir: pathlib.Path,
) -> pathlib.Path:
    """Write the LinkML schema YAML to ``output_dir/<class_name>.yaml``.

    Creates *output_dir* if it does not exist.

    Returns
    -------
    pathlib.Path
        Path to the written file.
    """
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / f"{class_name}.yaml"
    out.write_text(write_schema(class_name, schema))
    return out
