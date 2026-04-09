This `AGENTS.md` serves as the high-level technical manifesto and architectural blueprint for **Project PLUGIN-Lineage**. It is designed for contributors and AI agents to understand the "how" and "why" of our approach to column-level data lineage.

---

# Project PLUGIN-Lineage: Architectural Manifesto

**Project PLUGIN-Lineage** is an open-source framework designed to infer column-level data lineage from Python-based data pipelines. By treating metadata as a first-class citizen and leveraging **Apache Arrow**, **sqlglot**, and **LinkML**, we turn opaque compute logic into transparent, version-controlled YAML artifacts.

## The Core Philosophy

1. **Metadata-First, Data-Second:** We don't need to see your data to understand it. We intercept compute plans (Polars' serialized plan JSON) and schemas (Arrow) to infer lineage without execution overhead.
2. **Static YAML (GitOps):** Lineage should live where the code lives. All output is persisted as human-readable YAML files, making lineage changes auditable via standard Git Pull Requests.
3. **The Terminal is the Source of Truth:** No heavy Web UIs. Developers use a high-fidelity CLI to audit, name, and validate the lineage graph.
4. **Two Explicit Paths, Not One Universal IR:** SQL pipelines use `sqlglot.lineage` directly; Polars DataFrame pipelines use Polars' native plan serialization. We do not require a common intermediate representation — correctness and simplicity over theoretical elegance.

---

## The Technical Stack

| Layer | Technology |
| :--- | :--- |
| **Orchestration** | Dagster (via Sensors on `AssetMaterialization` events) |
| **SQL Lineage** | `sqlglot` (`sqlglot.lineage` module) |
| **DataFrame Lineage** | Polars (`LazyFrame.serialize(format="json")`) |
| **Type System** | Apache Arrow (for schema derivation and type safety) |
| **Persistence** | LinkML (Schemas) and LinkML-Map (Column Lineage) |
| **CLI Framework** | Python `Typer`, `Rich`, and `Questionary` |

---

## The Two Inference Paths

### Path A: SQL Pipelines

SQL transformations are parsed and analyzed using **`sqlglot.lineage`**. This module traces a named output column back through JOINs, CTEs, subqueries, and `SELECT *` expansions, returning a linked `Node` graph. Each `Node` holds the `sqlglot` expression that produced the column and a `downstream` list for further traversal.

No database engine is required. Lineage is inferred statically from the SQL text and an optional schema dictionary (column name → Arrow type string).

```python
from sqlglot.lineage import lineage

node = lineage(
    column="price",
    sql="SELECT items.price FROM orders JOIN items ON orders.item_id = items.id",
    schema={"items": {"price": "FLOAT", "id": "INT"}, "orders": {"item_id": "INT"}}
)
# node.walk() -> recursive iterator over all upstream lineage nodes
```

The `SQLLineageWalker` converts the `Node` graph into the project's internal `ColumnLineage` data model.

### Path B: Polars DataFrame Pipelines

Polars `LazyFrame` objects can serialize their logical plan to JSON via `LazyFrame.serialize(format="json")`. This JSON encodes the full query plan including projections, joins, filters, and aggregations using Polars' own plan representation.

The `PolarsWalker` parses this JSON recursively. Key dispatch points:

- **`Projection`**: Maps output column expressions back to input column references.
- **`Join`**: Identifies left/right key columns, registering a **Pending Identity** for each join key pair.
- **`Filter`**: Records which columns participate in filter predicates (filter-level lineage).
- **`HStack` / `Select`**: Handles `with_columns` patterns.

Column names are resolved from the plan node's `schema` field (a `{name: ArrowType}` mapping present on each node), not from expression nodes, which may be anonymous.

**Opaque expressions:** If a plan node contains a `MapFunction` (e.g., a `.map_elements(lambda ...)` call), the output column is marked as `lineage: opaque`. Developers may provide a manual override via Dagster asset metadata.

---

## The Lifecycle of a Metadata Trace

### 1. Capture (Dagster Sensor)

A Dagster sensor monitors `AssetMaterialization` events. Assets must attach two metadata keys:

| Metadata Key | Content |
| :--- | :--- |
| `lineage_plan` | For SQL assets: the SQL query string. For Polars assets: the JSON string from `LazyFrame.serialize(format="json")`. |
| `lineage_schema` | The output Arrow schema, serialized as a JSON string via `schema.to_string()`. |

The sensor writes these to the `metadata/` directory for offline processing.

Local plan extraction (without a running Dagster instance) is available via:

```
plugin-lineage trace <asset_module_path>
```

### 2. The Plan Walker

A `PlanWalker` abstract base class defines the shared interface. Two concrete implementations:

- **`SQLLineageWalker`**: Wraps `sqlglot.lineage`, iterating over all output columns.
- **`PolarsWalker`**: Recursively descends the Polars plan JSON using `node["node_type"]` to dispatch.

Both walkers produce a list of `ColumnLineage` objects (the internal data model), each containing:

- `source_table`, `source_column`
- `target_table`, `target_column`
- `expression` (the sqlglot expression string or Polars expression repr)
- `lineage_type`: one of `direct`, `derived`, `filter`, `opaque`

### 3. Identity Consensus (The "Frequency Winner")

When a join is detected (e.g., `users.id == orders.user_ptr`), the engine creates a **Pending Identity** — a named group of columns that refer to the same real-world concept.

- The identity key is the **sorted, normalized join-key pair** (e.g., `orders.user_ptr::users.id`) to ensure deterministic, merge-conflict-free entries in `identities.yaml`.
- Auto-resolve: if all columns in the group share the same name, the identity is registered automatically.
- Prompt-only: when names differ, `plugin-lineage name` surfaces the pending identity for interactive naming. The CLI suggests the **most frequent column name** within the group as a default.

### 4. Persistence (LinkML)

The engine emits two types of YAML files into the `/metadata` directory:

- **Schemas** (`/metadata/schemas/`): Standard LinkML `ClassDefinitions` representing tables. The `slot` definitions carry the Arrow type as a `range` annotation.
- **Mappings** (`/metadata/mappings/`): LinkML-Map `TransformationSpecification` files. Each output column is a `SlotDerivation` with either `populated_from` (direct/rename) or `expr` (derived) set.

Arrow schema serialization: an Arrow `Schema` object is converted to a dict of `{column_name: arrow_type_string}` and embedded in the LinkML YAML under a `source_schema` annotation. This keeps `lint` and `diff` fully static — no database or runtime needed.

---

## The Developer Utility (CLI)

The CLI (`plugin-lineage`) is the developer's primary interface for managing the lineage ledger.

### `plugin-lineage trace <asset_module>`

Extracts the lineage plan from a Python module (Dagster asset definitions) without requiring a running Dagster instance. Writes raw plan JSON/SQL and schema to `metadata/raw/`.

### `plugin-lineage ingest`

Processes all files in `metadata/raw/`, runs the appropriate walker, and writes/updates schema and mapping YAMLs in `metadata/schemas/` and `metadata/mappings/`.

### `plugin-lineage lint`

- Validates that all `SlotDerivation.populated_from` references point to existing columns in the schemas.
- Checks type-compatibility in identity groups (e.g., ensures you are not joining a `string` to an `int64`).
- Flags any `lineage: opaque` columns that lack a manual override.
- Fully static: no database or runtime required.

### `plugin-lineage diff [--base <git-ref>]`

A terminal-only semantic diff tool. Compares current metadata against a Git reference (default: `main`).

- Highlights **Breaking Changes** (deleted columns, type changes).
- Highlights **Logic Drift** (changes in the `expr` field of a `SlotDerivation`).
- Visualizes the **Impact Tree** (the blast radius of a schema change) using `rich.tree.Tree`.

### `plugin-lineage name`

Interactive prompt for resolving Pending Identities.

- Lists all unresolved entries in `identities.yaml`.
- Suggests the most frequent column name as the default.
- On confirmation, updates `identities.yaml` and propagates the friendly name across all affected LinkML mapping files.

---

## Directory Structure

```text
/project_root
  ├── /metadata
  │   ├── /raw          # Raw plan JSON/SQL from capture step
  │   ├── /schemas      # LinkML ClassDefinitions (one file per table)
  │   ├── /mappings     # LinkML-Map TransformationSpecification files
  │   └── identities.yaml  # Semantic identity registry (sorted keys, append-only)
  ├── /src
  │   └── /lineage
  │       ├── /walkers       # SQLLineageWalker, PolarsWalker, base class
  │       ├── /persistence   # LinkML schema + mapping writers/readers
  │       ├── /models.py     # ColumnLineage dataclass, IdentityGroup dataclass
  │       └── /cli.py        # Typer app entry point
  └── AGENTS.md
```

---

## Implementation Guardrails

- **No runtime required for lint/diff:** Always use the Arrow schemas embedded in the LinkML YAMLs. Never require a running database or Polars session for static analysis.
- **Opaque functions:** Mark as `lineage: opaque`. Do not silently drop lineage. Surface opaque columns in `lint` output.
- **Joins always register identities:** If a join is detected, a Pending Identity MUST be created. Skipping identity registration is not permitted.
- **`identities.yaml` is append-only with sorted keys:** Use the normalized `table_a.col::table_b.col` (alphabetically sorted) format as the primary key. This prevents merge conflicts in team workflows.
- **Type annotations everywhere:** All internal data models (`ColumnLineage`, `IdentityGroup`) must use Python dataclasses with full type annotations. Use `pyarrow` types as the canonical type representation.

> **Note to Agents:**
> - For the `SQLLineageWalker`, start from `sqlglot.lineage.lineage()` and iterate over all output columns of the final `SELECT`. Use `node.walk()` to traverse the full upstream chain.
> - For the `PolarsWalker`, dispatch on `node["node_type"]` in the plan JSON. Key node types are `"Projection"`, `"Join"`, `"Filter"`, `"HStack"`, `"DataFrameScan"`, and `"Scan"`. Column names are resolved from `node["schema"]["fields"]`, not from expression ASTs.
> - For the CLI, use `rich.table.Table` for tabular output and `rich.tree.Tree` for the Impact Tree in `diff`. Use `questionary.select` and `questionary.text` for interactive prompts in `name`.
> - The `PlanWalker` base class must define `walk(plan: str, output_schema: pa.Schema) -> list[ColumnLineage]` as its single public method.
