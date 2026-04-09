# IMPLEMENTATION-PLAN.md

## Project PLUGIN-Lineage: TDD Implementation Plan

### Decisions Locked In

| # | Decision |
|---|---|
| 1 | `target_table` in `ColumnLineage` = the Dagster asset name, passed as `asset_name` parameter to `walk()` |
| 2 | `Scan` nodes (file-backed Polars) → walker uses caller-supplied `output_schema`; unmatched columns marked `opaque` |
| 3 | `pydantic>=2.0` added as a core dependency |

---

### Key Ecosystem Facts (Grounding the Design)

These were verified against current library versions before writing this plan:

- **`Polars.to_substrait()` does not exist.** Polars serializes to a proprietary plan format via `LazyFrame.serialize(format="json")`. The Polars plan JSON uses the node type as the **single top-level dict key** (e.g. `{"Select": {...}}`). There is no `"node_type"` field.
- **`node["schema"]["fields"]` is a flat dict** `{"col_name": "TypeStr"}`, not a list.
- **`sqlglot` does not produce Substrait.** It has its own AST and a native `sqlglot.lineage` module for column-level lineage that is used directly.
- **`substrait-python` has no visitor pattern.** Substrait is not used in this project.
- **Polars `AnonymousFunction`** (from `map_elements` / `map_batches`) serializes as `{"AnonymousFunction": {"function": [<int bytes>], ...}}` — the binary blob is the opaque marker.
- **`sqlglot.lineage.lineage()` signature** includes `sources` and `trim_selects` parameters. Leaf nodes satisfy `isinstance(node.source, exp.Table)`. Source table name: `node.source.name`. Source column: `node.name.split(".")[-1]`.
- **linkml-map `TransformationSpecification.class_derivations`** is a list in Python/YAML. `slot_derivations` within a `ClassDerivation` is a `dict[str, SlotDerivation]`.
- **Arrow type strings**: `str(pa.float64())` → `"double"` (not `"float64"`), `str(pa.utf8())` → `"string"`.
- **`Annotation`** is imported from `linkml_runtime.linkml_model.meta`, not `linkml_runtime.linkml_model`.

---

### Directory Layout (End State)

```
plugin-lineage/
├── src/lineage/
│   ├── __init__.py
│   ├── models.py                    # ColumnLineage, IdentityGroup, IdentityMember
│   ├── cli.py                       # Typer app: ingest, lint, diff, name
│   ├── dagster_sensor.py            # AssetMaterialization sensor
│   ├── walkers/
│   │   ├── __init__.py
│   │   ├── base.py                  # PlanWalker ABC
│   │   ├── sql_walker.py            # SQLLineageWalker
│   │   └── polars_walker.py         # PolarsWalker
│   └── persistence/
│       ├── __init__.py
│       ├── schema_writer.py         # Arrow schema → LinkML YAML
│       ├── mapping_writer.py        # list[ColumnLineage] → TransformationSpecification YAML
│       └── identity_registry.py    # identities.yaml read/write/resolve
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # shared fixtures (Arrow schemas, SQL strings, Polars JSON)
│   ├── fixtures/                    # static Polars plan JSON files (hermetic, not generated)
│   │   ├── polars_select.json
│   │   ├── polars_with_columns.json
│   │   ├── polars_filter.json
│   │   ├── polars_join.json
│   │   ├── polars_join_multikey.json
│   │   ├── polars_opaque.json
│   │   ├── polars_chained.json
│   │   └── polars_groupby.json
│   ├── test_models.py
│   ├── test_walker_base.py
│   ├── test_sql_walker.py
│   ├── test_polars_walker.py
│   ├── test_schema_writer.py
│   ├── test_mapping_writer.py
│   ├── test_identity_registry.py
│   ├── test_cli.py
│   └── test_dagster_sensor.py
├── metadata/
│   ├── raw/.gitkeep
│   ├── schemas/.gitkeep
│   ├── mappings/.gitkeep
│   └── identities.yaml
├── AGENTS.md
├── IMPLEMENTATION-PLAN.md           # this file
├── main.py                          # stub (replaced by CLI entry point)
└── pyproject.toml
```

---

## Phase 0 — Scaffold & Configuration

**Goal:** All tooling works. `pytest` collects 0 tests with no errors.

### 0.1 Edit `pyproject.toml`

Add to `dependencies`:
- `"pydantic>=2.0"`
- `"pyyaml>=6.0"`

Add to `[dependency-groups] dev`:
- `"pytest-mock>=3.0"`

Add `[tool.pytest.ini_options]`:
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
```

### 0.2 Create Package Skeleton

Create all `__init__.py` files and empty module stubs:
- `src/lineage/__init__.py`
- `src/lineage/models.py`
- `src/lineage/cli.py`
- `src/lineage/dagster_sensor.py`
- `src/lineage/walkers/__init__.py`
- `src/lineage/walkers/base.py`
- `src/lineage/walkers/sql_walker.py`
- `src/lineage/walkers/polars_walker.py`
- `src/lineage/persistence/__init__.py`
- `src/lineage/persistence/schema_writer.py`
- `src/lineage/persistence/mapping_writer.py`
- `src/lineage/persistence/identity_registry.py`

### 0.3 Create Test Skeleton

- `tests/__init__.py`
- `tests/conftest.py` (empty initially)
- `tests/fixtures/` directory with 8 static Polars JSON fixture files

### 0.4 Create `metadata/` Directories

```
metadata/raw/.gitkeep
metadata/schemas/.gitkeep
metadata/mappings/.gitkeep
metadata/identities.yaml   (empty initially)
```

### 0.5 Verify

```bash
pytest           # 0 collected, no errors
pip install -e . # package installs cleanly
```

---

## Phase 1 — Data Models

**File:** `src/lineage/models.py`
**Tests:** `tests/test_models.py`

### TDD Cycle: write all tests first, then implement.

### 1.1 `ColumnLineage`

```python
class ColumnLineage(BaseModel):
    model_config = ConfigDict(frozen=True)
    source_table: str
    source_column: str
    target_table: str          # = Dagster asset name, supplied by walker caller
    target_column: str
    lineage_type: Literal["direct", "derived", "filter", "opaque"] = "direct"
    expression: str | None = None
```

**Validators:**
- `@field_validator("source_column", "target_column")` — strip whitespace, reject empty string
- `@model_validator(mode="after")` — `lineage_type="derived"` requires `expression` to be set

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_direct_valid` | Constructs without error; `lineage_type="direct"` |
| `test_derived_requires_expression` | `lineage_type="derived"`, `expression=None` → `ValidationError` |
| `test_derived_with_expression_valid` | `lineage_type="derived"`, `expression="price * 0.9"` → valid |
| `test_opaque_no_expression_ok` | `lineage_type="opaque"`, `expression=None` → valid |
| `test_empty_source_column` | `source_column=""` → `ValidationError` |
| `test_whitespace_column_stripped` | `source_column=" id "` → `source_column == "id"` |
| `test_frozen_immutable` | `col.source_column = "x"` raises `TypeError` |
| `test_invalid_lineage_type` | `lineage_type="unknown"` → `ValidationError` |
| `test_yaml_roundtrip` | `model_dump()` → `yaml.dump()` → `yaml.safe_load()` → `model_validate()` → equal |

### 1.2 `IdentityMember`

```python
class IdentityMember(BaseModel):
    model_config = ConfigDict(frozen=True)
    table: str
    column: str
```

### 1.3 `IdentityGroup`

```python
class IdentityGroup(BaseModel):
    model_config = ConfigDict(frozen=True)
    key: str                      # auto-computed: "::".join(sorted("table.col" for each member))
    members: list[IdentityMember]
    friendly_name: str | None = None
    resolved: bool = False
```

**Validators:**
- `@model_validator(mode="after")` — assert `key == "::".join(sorted(f"{m.table}.{m.column}" for m in members))`; set `resolved=True` if `friendly_name` is not None

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_key_is_sorted` | `members=[("orders","item_id"), ("items","id")]` → `key="items.id::orders.item_id"` |
| `test_auto_resolved_when_friendly_name` | `friendly_name="patient_id"` → `resolved=True` |
| `test_unresolved_by_default` | no `friendly_name` → `resolved=False` |
| `test_key_mismatch_raises` | manually passing wrong `key` → `ValidationError` |
| `test_yaml_roundtrip` | dump/load cycle → equal |

---

## Phase 2 — Walker Base Class

**File:** `src/lineage/walkers/base.py`
**Tests:** `tests/test_walker_base.py`

### Interface

```python
from abc import ABC, abstractmethod
import pyarrow as pa
from lineage.models import ColumnLineage, IdentityGroup

class PlanWalker(ABC):
    @abstractmethod
    def walk(
        self,
        plan: str,
        output_schema: pa.Schema,
        asset_name: str,
    ) -> list[ColumnLineage]: ...

    @property
    @abstractmethod
    def identity_groups(self) -> list[IdentityGroup]: ...
```

**Note:** `identity_groups` is accumulated during `walk()` and exposed as a property. This keeps `walk()`'s return type a clean `list[ColumnLineage]`.

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_cannot_instantiate_directly` | `PlanWalker()` → `TypeError` |
| `test_missing_walk_raises` | Subclass missing `walk()` → `TypeError` on instantiation |
| `test_missing_identity_groups_raises` | Subclass missing `identity_groups` → `TypeError` on instantiation |
| `test_valid_subclass_instantiates` | Full minimal subclass → instantiates and `walk()` returns `list` |

---

## Phase 3 — SQL Lineage Walker

**File:** `src/lineage/walkers/sql_walker.py`
**Tests:** `tests/test_sql_walker.py`

### Interface

```python
class SQLLineageWalker(PlanWalker):
    def walk(self, plan: str, output_schema: pa.Schema, asset_name: str) -> list[ColumnLineage]:
        # plan = the SQL query string
        ...
    @property
    def identity_groups(self) -> list[IdentityGroup]: ...
```

### Internal Logic

1. `parsed = sqlglot.parse_one(plan)` → `output_cols = parsed.named_selects`
2. `sqlglot_schema = _arrow_schema_to_sqlglot_dict(output_schema)` — converts Arrow schema to `{"table": {"col": "TYPE"}}` for sqlglot's schema resolver
3. For each `col` in `output_cols`: call `sqlglot.lineage.lineage(col, plan, schema=sqlglot_schema)`
4. Walk `node.walk()`:
   - Leaf test: `isinstance(n.source, exp.Table)` → `source_table = n.source.name`, `source_column = n.name.split(".")[-1]`
   - Expression test: if root `node.expression` is not `exp.Column` → `lineage_type="derived"`, `expression=node.expression.sql()`
   - Otherwise: `lineage_type="direct"`
5. JOIN key extraction: walk root `node.source` AST for `exp.EQ` predicates that compare two `exp.Column` nodes → produce `IdentityGroup` per pair
6. All `ColumnLineage` objects get `target_table=asset_name`

### Helper

```python
def _arrow_schema_to_sqlglot_dict(schema: pa.Schema) -> dict:
    # Returns {"col_name": "TYPE_STR"} flat dict (no table nesting needed for single-table queries)
    # For multi-table, the SQL JOIN syntax provides table context to sqlglot
```

### SQL Test Fixtures (in `tests/conftest.py`)

```python
SIMPLE_SELECT_SQL = "SELECT price, quantity FROM orders"

JOIN_SQL = """
    SELECT o.customer_id, i.price
    FROM orders o
    JOIN items i ON o.item_id = i.id
"""

CTE_SQL = """
    WITH enriched AS (
        SELECT o.id, i.price * o.qty AS total
        FROM orders o JOIN items i ON o.item_id = i.id
    )
    SELECT id, total FROM enriched
"""

DERIVED_SQL = "SELECT price * 0.9 AS discounted_price FROM products"
RENAME_SQL   = "SELECT price AS unit_price FROM products"
STAR_SQL     = "SELECT * FROM orders"
```

### Tests

| Test name | What it asserts |
|---|---|
| `test_simple_direct` | `SELECT price FROM orders` → `ColumnLineage(source_table="orders", source_column="price", target_column="price", lineage_type="direct")` |
| `test_all_output_columns_covered` | `SELECT price, qty FROM orders` → 2 `ColumnLineage` objects |
| `test_join_two_sources` | JOIN SQL → `source_table` values include both `"orders"` and `"items"` |
| `test_derived_expression` | `SELECT price * 0.9 AS disc` → `lineage_type="derived"`, `expression` contains `"price"` and `"0.9"` |
| `test_cte_resolves_to_base_table` | CTE SQL → `source_table` is `"orders"` or `"items"`, never `"enriched"` |
| `test_rename_alias` | `SELECT price AS unit_price` → `source_column="price"`, `target_column="unit_price"` |
| `test_select_star_expands` | `SELECT * FROM orders` + schema → one `ColumnLineage` per column in schema |
| `test_join_registers_identity` | JOIN on `o.item_id = i.id` → `walker.identity_groups` has one entry, `key="items.id::orders.item_id"` |
| `test_target_table_is_asset_name` | `asset_name="my_asset"` → all `ColumnLineage.target_table == "my_asset"` |
| `test_invalid_sql_raises` | Garbled SQL → `ValueError` |

---

## Phase 4 — Polars Walker

**File:** `src/lineage/walkers/polars_walker.py`
**Tests:** `tests/test_polars_walker.py`

### Interface

```python
class PolarsWalker(PlanWalker):
    def walk(self, plan: str, output_schema: pa.Schema, asset_name: str) -> list[ColumnLineage]:
        # plan = JSON string from LazyFrame.serialize(format="json")
        ...
    @property
    def identity_groups(self) -> list[IdentityGroup]: ...
```

### Dispatch Rule

```python
node_type = next(iter(node))   # the single top-level key IS the node type
body = node[node_type]
```

### Node Type Dispatch Table

| Node type | Action |
|---|---|
| `"Select"` | Walk `body["expr"]` list for `Column` refs; recurse into `body["input"]` |
| `"HStack"` | Walk `body["exprs"]` list; recurse into `body["input"]` |
| `"Filter"` | Extract `Column` refs from `body["predicate"]`; mark as `lineage_type="filter"`; recurse into `body["input"]` |
| `"Join"` | Extract `body["left_on"]` / `body["right_on"]` column pairs → `IdentityGroup`; recurse into both `body["input_left"]` and `body["input_right"]` |
| `"DataFrameScan"` | Leaf: schema from `body["schema"]["fields"]` (flat `{name: type_str}` dict) |
| `"Scan"` | Leaf: use `output_schema` from caller |
| `"IR"` | Unwrap: recurse into `body["dsl"]` |
| `"MapFunction"` | If `body["function"]` key is `"Rename"` → direct lineage with remapped names; otherwise mark opaque |
| `"GroupBy"` | Keys → `"direct"`; aggs → `"derived"` |
| `"Sort"` / `"Slice"` | Passthrough: recurse into `body["input"]` |
| Unknown | Skip |

### Opaque Detection

```python
def _is_opaque_expr(expr: dict) -> bool:
    expr_type = next(iter(expr))
    if expr_type == "AnonymousFunction":
        fn = expr["AnonymousFunction"].get("function", [])
        return isinstance(fn, list) and bool(fn) and isinstance(fn[0], int)
    return False
```

### `Alias` Unwrapping

Expressions in `HStack.exprs` are typically `{"Alias": [<inner_expr>, "alias_name"]}`. Unwrap: `inner = expr["Alias"][0]`, `alias = expr["Alias"][1]`.

### Static JSON Fixtures (`tests/fixtures/`)

All 8 files use the confirmed JSON structure from Polars 1.x plan serialization:

**`polars_select.json`**
```json
{
  "Select": {
    "expr": [{"Column": "a"}, {"Column": "b"}],
    "input": {
      "DataFrameScan": {
        "schema": {"fields": {"a": "Int64", "b": "String"}, "metadata": null}
      }
    },
    "options": {}
  }
}
```

**`polars_with_columns.json`**
```json
{
  "HStack": {
    "input": {
      "DataFrameScan": {
        "schema": {"fields": {"a": "Int64", "b": "Int64"}, "metadata": null}
      }
    },
    "exprs": [
      {"Alias": [{"BinaryExpr": {"left": {"Column": "a"}, "op": "Plus", "right": {"Column": "b"}}}, "sum_ab"]}
    ],
    "options": {}
  }
}
```

**`polars_filter.json`**
```json
{
  "Filter": {
    "input": {
      "DataFrameScan": {
        "schema": {"fields": {"age": "Int64", "name": "String"}, "metadata": null}
      }
    },
    "predicate": {
      "BinaryExpr": {
        "left": {"Column": "age"},
        "op": "Gt",
        "right": {"Literal": {"Dyn": {"Int": 30}}}
      }
    }
  }
}
```

**`polars_join.json`**
```json
{
  "Join": {
    "input_left": {
      "DataFrameScan": {
        "schema": {"fields": {"id": "Int64", "name": "String"}, "metadata": null}
      }
    },
    "input_right": {
      "DataFrameScan": {
        "schema": {"fields": {"user_id": "Int64", "score": "Float64"}, "metadata": null}
      }
    },
    "left_on":  [{"Column": "id"}],
    "right_on": [{"Column": "user_id"}],
    "predicates": [],
    "options": {"allow_parallel": true, "force_parallel": false, "args": {"how": "Left", "suffix": "_right", "coalesce": "JoinSpecific", "nulls_equal": false}}
  }
}
```

**`polars_join_multikey.json`**
```json
{
  "Join": {
    "input_left": {
      "DataFrameScan": {
        "schema": {"fields": {"org_id": "Int64", "user_id": "Int64", "val": "Int64"}, "metadata": null}
      }
    },
    "input_right": {
      "DataFrameScan": {
        "schema": {"fields": {"org_id": "Int64", "user_id": "Int64", "score": "Float64"}, "metadata": null}
      }
    },
    "left_on":  [{"Column": "org_id"}, {"Column": "user_id"}],
    "right_on": [{"Column": "org_id"}, {"Column": "user_id"}],
    "predicates": [],
    "options": {"args": {"how": "Inner"}}
  }
}
```

**`polars_opaque.json`**
```json
{
  "HStack": {
    "input": {
      "DataFrameScan": {
        "schema": {"fields": {"b": "String"}, "metadata": null}
      }
    },
    "exprs": [
      {"Alias": [
        {"AnonymousFunction": {
          "input": [{"Column": "b"}],
          "function": [80, 76, 80, 89],
          "options": {"check_lengths": true, "flags": "ROW_SEPARABLE"}
        }},
        "b_mapped"
      ]}
    ],
    "options": {}
  }
}
```

**`polars_chained.json`**
```json
{
  "Select": {
    "expr": [{"Column": "patient_id"}, {"Column": "diagnosis"}, {"Column": "age_doubled"}],
    "input": {
      "HStack": {
        "exprs": [
          {"Alias": [
            {"BinaryExpr": {"left": {"Column": "age"}, "op": "Multiply", "right": {"Literal": {"Dyn": {"Int": 2}}}}},
            "age_doubled"
          ]}
        ],
        "input": {
          "Filter": {
            "predicate": {
              "BinaryExpr": {"left": {"Column": "age"}, "op": "GtEq", "right": {"Literal": {"Dyn": {"Int": 30}}}}
            },
            "input": {
              "DataFrameScan": {
                "schema": {"fields": {"patient_id": "Int64", "age": "Int64", "diagnosis": "String"}, "metadata": null}
              }
            }
          }
        },
        "options": {}
      }
    },
    "options": {}
  }
}
```

**`polars_groupby.json`**
```json
{
  "GroupBy": {
    "input": {
      "DataFrameScan": {
        "schema": {"fields": {"dept": "String", "salary": "Float64"}, "metadata": null}
      }
    },
    "keys": [{"Column": "dept"}],
    "aggs": [
      {"Alias": [{"Agg": {"Sum": {"Column": "salary"}}}, "total_salary"]}
    ],
    "predicates": [],
    "maintain_order": false,
    "options": {},
    "apply": null
  }
}
```

### Tests

| Test name | What it asserts |
|---|---|
| `test_simple_select` | `Select` with 2 `Column` exprs → 2 direct `ColumnLineage` objects |
| `test_with_columns_direct` | `HStack` with `Column` expr → `lineage_type="direct"` |
| `test_with_columns_derived` | `HStack` with `BinaryExpr` → `lineage_type="derived"`, `expression` set |
| `test_filter_predicate_columns` | `Filter` → `lineage_type="filter"` for `age` column in predicate |
| `test_join_registers_identity` | `left_on=[Column "id"]`, `right_on=[Column "user_id"]` → `identity_groups` has key `"...id::...user_id"` (sorted) |
| `test_join_multikey_two_identities` | Two key pairs → two `IdentityGroup` entries |
| `test_opaque_anonymous_function` | `AnonymousFunction` with int-array blob → `lineage_type="opaque"` |
| `test_rename_via_map_function` | `MapFunction(Rename)` → `lineage_type="direct"`, remapped column names |
| `test_chained_pipeline` | filter→hstack→select → combined lineage includes direct, derived, and filter entries |
| `test_schema_extraction` | `DataFrameScan` fields parsed correctly to `{name: type_str}` |
| `test_groupby_agg_is_derived` | GroupBy agg output → `lineage_type="derived"` |
| `test_groupby_key_is_direct` | GroupBy key column → `lineage_type="direct"` |
| `test_scan_node_uses_output_schema` | `Scan` node → column names from `output_schema` arg |
| `test_target_table_is_asset_name` | All `ColumnLineage.target_table == asset_name` |

---

## Phase 5 — Persistence

### 5a — Schema Writer

**File:** `src/lineage/persistence/schema_writer.py`
**Tests:** `tests/test_schema_writer.py`

**Arrow → LinkML type mapping** (canonical `str(pa_type)` strings from pyarrow):

| `str(pa_type)` | LinkML `range` |
|---|---|
| `int8`, `int16`, `int32`, `int64`, `uint8`…`uint64` | `integer` |
| `float`, `double`, `float16` | `float` |
| `string`, `large_string` | `string` |
| `bool` | `boolean` |
| `date32[day]`, `date64[ms]` | `date` |
| All other types (timestamps, lists, structs, etc.) | `string` + full `arrow_type` annotation |

The `arrow_type` annotation (tag=`"arrow_type"`, value=`str(pa_type)`) is written on **every** slot regardless of range.

**Imports:**
```python
from linkml_runtime.linkml_model import ClassDefinition, SlotDefinition
from linkml_runtime.linkml_model.meta import Annotation   # note: .meta submodule
from linkml_runtime.dumpers import YAMLDumper
from linkml_runtime.loaders import YAMLLoader
from linkml.utils.schema_builder import SchemaBuilder
```

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_arrow_to_linkml_yaml` | `pa.schema([field("id", int64()), field("name", string())])` → parseable YAML |
| `test_type_mapping_integer` | `int64` → `range: integer` |
| `test_type_mapping_float` | `double` → `range: float` (note: `str(pa.float64()) == "double"`) |
| `test_type_mapping_complex` | `timestamp("us", tz="UTC")` → `range: string`, annotation value `"timestamp[us, tz=UTC]"` |
| `test_annotation_always_present` | Every slot has `arrow_type` annotation |
| `test_roundtrip_yaml(tmp_path)` | Write → `YAMLLoader.load_any(..., SchemaDefinition)` → correct slots |
| `test_write_to_file(tmp_path)` | File created, valid YAML, correct class name |

### 5b — Mapping Writer

**File:** `src/lineage/persistence/mapping_writer.py`
**Tests:** `tests/test_mapping_writer.py`

**`ColumnLineage` → `SlotDerivation` mapping rules:**

| `lineage_type` | `SlotDerivation` fields set |
|---|---|
| `direct`, same name | `name=target_column`, `populated_from=source_column` |
| `direct`, different name (rename) | `name=target_column`, `populated_from=source_column` |
| `derived` | `name=target_column`, `expr=expression` |
| `filter` | `name=target_column`, `populated_from=source_column`, `comments=["lineage: filter"]` |
| `opaque` | `name=target_column`, `comments=["lineage: opaque"]` |

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_direct_slot_derivation` | Same-name direct → `SlotDerivation(populated_from="id")` |
| `test_rename_slot_derivation` | `source_column="client_ptr"`, `target_column="customer_id"` → `populated_from="client_ptr"` |
| `test_derived_slot_derivation` | `lineage_type="derived"`, `expression="price * 0.9"` → `expr="price * 0.9"` |
| `test_opaque_slot_derivation` | `lineage_type="opaque"` → `comments` contains `"lineage: opaque"` |
| `test_full_transformation_spec` | `list[ColumnLineage]` + `asset_name` → valid `TransformationSpecification` with one `ClassDerivation` |
| `test_roundtrip_yaml(tmp_path)` | Write → `YAMLLoader.load_any(..., TransformationSpecification)` → equal |
| `test_upsert_preserves_existing(tmp_path)` | Pre-existing entries not overwritten; new ones merged in |

### 5c — Identity Registry

**File:** `src/lineage/persistence/identity_registry.py`
**Tests:** `tests/test_identity_registry.py`

**`identities.yaml` format:**

```yaml
# sorted by key — append-only, merge-conflict safe
items.id::orders.item_id:
  key: items.id::orders.item_id
  members:
    - table: items
      column: id
    - table: orders
      column: item_id
  friendly_name: null
  resolved: false

users.id::users.user_ptr:
  key: users.id::users.user_ptr
  members:
    - table: users
      column: id
    - table: users
      column: user_ptr
  friendly_name: user_identity
  resolved: true
```

**Tests:**

| Test name | What it asserts |
|---|---|
| `test_add_pending` | `add(group)` → `list_all()` contains it |
| `test_key_is_sorted` | `("orders.item_id", "items.id")` input → key `"items.id::orders.item_id"` |
| `test_auto_resolve_same_name` | Both members have `column="id"` → loaded entry has `resolved=True` |
| `test_idempotent_add` | Same key added twice → one entry |
| `test_append_only` | Resolved entry re-added → original `friendly_name` preserved |
| `test_list_pending` | Returns only `resolved=False` entries |
| `test_resolve(tmp_path)` | `resolve(key, "patient_id")` → `resolved=True`, `friendly_name="patient_id"` |
| `test_yaml_roundtrip(tmp_path)` | Dump → load → equal `list[IdentityGroup]` |
| `test_sorted_keys_in_yaml(tmp_path)` | YAML file has keys in alphabetical order |

---

## Phase 6 — CLI

**File:** `src/lineage/cli.py`
**Tests:** `tests/test_cli.py`

All CLI tests use `typer.testing.CliRunner`. Filesystem tests use `tmp_path`.

### Commands

#### `plugin-lineage ingest`

Reads all files from `metadata/raw/`. Detects plan type:
- File ending in `.sql` or content is not valid JSON → `SQLLineageWalker`
- File containing valid JSON with a known Polars node type key → `PolarsWalker`

Writes to `metadata/schemas/` and `metadata/mappings/`.

| Test name | What it asserts |
|---|---|
| `test_ingest_sql_plan(tmp_path)` | `.sql` + `.schema.json` in `raw/` → schema and mapping files written |
| `test_ingest_polars_plan(tmp_path)` | Polars JSON in `raw/` → correct mapping written |
| `test_ingest_empty_raw(tmp_path)` | Empty dir → exit 0, informational message |
| `test_ingest_invalid_plan(tmp_path)` | Garbled content → exit 1, error message |

#### `plugin-lineage lint`

Validates all schemas and mappings in `metadata/`. Fully static — no DB or runtime.

Checks:
1. All `SlotDerivation.populated_from` columns exist in referenced schema
2. Identity groups with type-incompatible members (e.g. `string::int64`) flagged
3. `opaque` columns without a manual `expr` override flagged

| Test name | What it asserts |
|---|---|
| `test_lint_all_valid(tmp_path)` | Valid input → exit 0, "All checks passed" |
| `test_lint_missing_source_column(tmp_path)` | `populated_from` references unknown column → exit 1, column name in output |
| `test_lint_type_mismatch(tmp_path)` | Identity group with `string::int64` members → exit 1 |
| `test_lint_opaque_unresolved(tmp_path)` | Opaque column without override → exit 1 |

#### `plugin-lineage diff [--base <git-ref>]`

Compares current metadata against a git ref (default: `main`). Uses `rich.tree.Tree` for impact tree.

Detects:
- **Breaking Changes**: deleted columns, type changes
- **Logic Drift**: changed `expr` field in a `SlotDerivation`
- **Impact Tree**: blast radius for a changed schema class

| Test name | What it asserts |
|---|---|
| `test_diff_no_changes(tmp_path)` | No change from base → "No changes detected" |
| `test_diff_breaking_deleted_column(tmp_path)` | Column removed → "Breaking Change" in output |
| `test_diff_logic_drift(tmp_path)` | `expr` changed → "Logic Drift" in output |
| `test_diff_impact_tree(tmp_path)` | Impact tree rendered to stdout |

#### `plugin-lineage name`

Interactive prompt for resolving pending identities using `questionary`.

| Test name | What it asserts |
|---|---|
| `test_name_no_pending(tmp_path)` | No unresolved identities → "Nothing to name" |
| `test_name_prompt_accepted(tmp_path, mocker)` | `questionary.select` mocked → `identities.yaml` updated with `friendly_name` |

---

## Phase 7 — Dagster Sensor

**File:** `src/lineage/dagster_sensor.py`
**Tests:** `tests/test_dagster_sensor.py`

### Sensor Logic

Monitors `AssetMaterialization` events. Two required metadata keys per asset:

| Metadata Key | Content |
|---|---|
| `lineage_plan` | SQL string **or** Polars plan JSON string from `LazyFrame.serialize(format="json")` |
| `lineage_schema` | Output Arrow schema, serialized via `schema.serialize().to_pybytes()` encoded as base64, **or** the JSON string from `schema.to_string()` |

The sensor writes to `metadata/raw/<asset_key>.<ext>` where `ext` is `.sql` for SQL plans and `.json` for Polars plans.

| Test name | What it asserts |
|---|---|
| `test_sensor_extracts_sql_plan(tmp_path, mocker)` | Mock `AssetMaterialization` with SQL `lineage_plan` → `.sql` file written to `metadata/raw/` |
| `test_sensor_extracts_polars_plan(tmp_path, mocker)` | Mock with JSON `lineage_plan` → `.json` file written |
| `test_sensor_missing_key_warns(mocker)` | No `lineage_plan` key → logs warning, no crash, exit clean |
| `test_sensor_schema_deserialization(mocker)` | Arrow schema string → reconstructed as `pa.Schema` correctly |

---

## Execution Order

```
Phase 0  →  Phase 1  →  Phase 2  →  Phase 3  →  Phase 4
                                                    ↓
Phase 7  ←  Phase 6  ←  Phase 5c ←  Phase 5b ←  Phase 5a
```

**Rule for every phase:** Write all tests first (they must fail). Implement until all pass. Do not proceed to the next phase until the current phase is fully green.

---

## Running Tests

```bash
# All tests
pytest

# Single phase
pytest tests/test_models.py
pytest tests/test_sql_walker.py -v

# With coverage
pytest --cov=lineage --cov-report=term-missing
```
