# src/lineage/walkers/polars_walker.py
from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

from lineage.models import ColumnLineage, IdentityGroup, IdentityMember
from lineage.walkers.base import PlanWalker

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

PlanNode = dict[str, Any]
Schema = dict[str, str]  # {col_name: type_str}


# ---------------------------------------------------------------------------
# Opaque detection
# ---------------------------------------------------------------------------


def _is_opaque_expr(expr: PlanNode) -> bool:
    """Return True if *expr* is an AnonymousFunction with a binary int-array blob."""
    if not isinstance(expr, dict):
        return False
    expr_type = next(iter(expr))
    if expr_type != "AnonymousFunction":
        return False
    fn = expr["AnonymousFunction"].get("function", [])
    return isinstance(fn, list) and bool(fn) and isinstance(fn[0], int)


# ---------------------------------------------------------------------------
# Expression helpers
# ---------------------------------------------------------------------------


def _collect_column_refs(expr: Any) -> list[str]:
    """Recursively collect all Column name strings referenced in *expr*."""
    if not isinstance(expr, dict):
        return []
    cols: list[str] = []
    key = next(iter(expr))
    body = expr[key]
    if key == "Column":
        cols.append(body)
    elif key == "Alias":
        # body = [inner_expr, alias_name]
        cols.extend(_collect_column_refs(body[0]))
    elif key == "BinaryExpr":
        cols.extend(_collect_column_refs(body["left"]))
        cols.extend(_collect_column_refs(body["right"]))
    elif key == "Agg":
        inner_key = next(iter(body))
        cols.extend(_collect_column_refs(body[inner_key]))
    elif key == "AnonymousFunction":
        for inp in body.get("input", []):
            cols.extend(_collect_column_refs(inp))
    elif key == "Cast":
        cols.extend(_collect_column_refs(body.get("expr", {})))
    elif isinstance(body, dict):
        for v in body.values():
            if isinstance(v, dict):
                cols.extend(_collect_column_refs(v))
    return cols


def _expr_repr(expr: Any) -> str:
    """Return a human-readable string for a plan expression (best-effort)."""
    if not isinstance(expr, dict):
        return str(expr)
    key = next(iter(expr))
    body = expr[key]
    if key == "Column":
        return body
    if key == "BinaryExpr":
        left = _expr_repr(body["left"])
        right = _expr_repr(body["right"])
        return f"{left} {body.get('op', '?')} {right}"
    if key == "Agg":
        inner_key = next(iter(body))
        inner = _expr_repr(body[inner_key])
        return f"{inner_key}({inner})"
    if key == "Literal":
        dyn = body.get("Dyn", {})
        if dyn:
            val_key = next(iter(dyn))
            return str(dyn[val_key])
        return str(body)
    if key == "Cast":
        return _expr_repr(body.get("expr", {}))
    if key == "AnonymousFunction":
        inputs = [_expr_repr(i) for i in body.get("input", [])]
        return f"AnonymousFunction({', '.join(inputs)})"
    return key


def _classify_expr(inner: Any) -> tuple[str, str | None]:
    """Classify an expression (after Alias unwrap) as direct or derived.

    Returns (lineage_type, expression_repr | None).
    """
    if not isinstance(inner, dict):
        return "derived", _expr_repr(inner)
    key = next(iter(inner))
    if key == "Column":
        return "direct", None
    return "derived", _expr_repr(inner)


# ---------------------------------------------------------------------------
# Schema extraction
# ---------------------------------------------------------------------------


def _schema_from_node(node: PlanNode) -> Schema:
    """Extract {col: type_str} from a DataFrameScan node body."""
    return dict(node.get("schema", {}).get("fields", {}))


def _schema_from_output(output_schema: pa.Schema) -> Schema:
    """Convert pyarrow.Schema to {col: type_str} dict."""
    return {
        output_schema.field(i).name: str(output_schema.field(i).type)
        for i in range(len(output_schema))
    }


# ---------------------------------------------------------------------------
# Walker state
# ---------------------------------------------------------------------------


class _WalkState:
    """Mutable state threaded through the recursive walk."""

    def __init__(self, asset_name: str, output_schema: pa.Schema) -> None:
        self.asset_name = asset_name
        self.output_schema = output_schema
        self.edges: list[ColumnLineage] = []
        self.identity_groups: list[IdentityGroup] = []
        self._seen_identity_keys: set[str] = set()

    def add_edge(
        self,
        source_table: str,
        source_column: str,
        target_column: str,
        lineage_type: str,
        expression: str | None = None,
    ) -> None:
        self.edges.append(
            ColumnLineage(
                source_table=source_table,
                source_column=source_column,
                target_table=self.asset_name,
                target_column=target_column,
                lineage_type=lineage_type,  # type: ignore[arg-type]
                expression=expression,
            )
        )

    def add_identity(
        self, left_table: str, left_col: str, right_table: str, right_col: str
    ) -> None:
        key = "::".join(
            sorted([f"{left_table}.{left_col}", f"{right_table}.{right_col}"])
        )
        if key in self._seen_identity_keys:
            return
        self._seen_identity_keys.add(key)
        parts = sorted([f"{left_table}.{left_col}", f"{right_table}.{right_col}"])
        members = []
        for part in parts:
            t, c = part.rsplit(".", 1)
            members.append(IdentityMember(table=t, column=c))
        self.identity_groups.append(IdentityGroup(key=key, members=members))


# ---------------------------------------------------------------------------
# Recursive node dispatcher
# ---------------------------------------------------------------------------


def _walk_node(node: PlanNode, state: _WalkState) -> Schema:
    """Recursively walk *node* and return the output schema of this node.

    The returned schema is used by parent nodes to know which columns flow
    through from this node.
    """
    if not isinstance(node, dict) or not node:
        return {}

    node_type = next(iter(node))
    body = node[node_type]

    if node_type == "Select":
        return _handle_select(body, state)
    elif node_type == "HStack":
        return _handle_hstack(body, state)
    elif node_type == "Filter":
        return _handle_filter(body, state)
    elif node_type == "Join":
        return _handle_join(body, state)
    elif node_type == "DataFrameScan":
        return _schema_from_node(body)
    elif node_type == "Scan":
        return _schema_from_output(state.output_schema)
    elif node_type == "IR":
        return _walk_node(body.get("dsl", {}), state)
    elif node_type == "MapFunction":
        return _handle_map_function(body, state)
    elif node_type == "GroupBy":
        return _handle_groupby(body, state)
    elif node_type in ("Sort", "Slice"):
        return _walk_node(body.get("input", {}), state)
    else:
        # Unknown node — try to recurse into "input" if present
        if isinstance(body, dict) and "input" in body:
            return _walk_node(body["input"], state)
        return {}


# ---------------------------------------------------------------------------
# Node handlers
# ---------------------------------------------------------------------------


def _handle_select(body: PlanNode, state: _WalkState) -> Schema:
    """Handle a Select node.

    Recurse into the input to get its schema, then for each Column expr
    in body["expr"] emit a direct edge.
    """
    input_schema = _walk_node(body.get("input", {}), state)
    # Use DataFrameScan node as the "source_table" label
    source_table = _derive_source_label(body.get("input", {}), input_schema)

    output_schema: Schema = {}
    for expr in body.get("expr", []):
        col_name, lineage_type, expression, src_col = _resolve_expr(expr, input_schema)
        if col_name is None:
            continue
        effective_src = src_col or col_name
        state.add_edge(source_table, effective_src, col_name, lineage_type, expression)
        out_type = input_schema.get(effective_src, input_schema.get(col_name, ""))
        output_schema[col_name] = out_type

    return output_schema


def _handle_hstack(body: PlanNode, state: _WalkState) -> Schema:
    """Handle an HStack (with_columns) node.

    The input columns all pass through unchanged; the exprs list adds new
    (or replaces) columns.
    """
    input_schema = _walk_node(body.get("input", {}), state)
    source_table = _derive_source_label(body.get("input", {}), input_schema)

    output_schema: Schema = dict(input_schema)  # passthrough

    for expr in body.get("exprs", []):
        col_name, lineage_type, expression, src_col = _resolve_expr(expr, input_schema)
        if col_name is None:
            continue
        effective_src = src_col or col_name
        state.add_edge(source_table, effective_src, col_name, lineage_type, expression)
        output_schema[col_name] = input_schema.get(effective_src, "")

    return output_schema


def _handle_filter(body: PlanNode, state: _WalkState) -> Schema:
    """Handle a Filter node.

    Records filter-level lineage for columns referenced in the predicate,
    then passes through all input columns as direct.
    """
    input_schema = _walk_node(body.get("input", {}), state)
    source_table = _derive_source_label(body.get("input", {}), input_schema)

    # Emit filter edges for predicate column references
    predicate = body.get("predicate", {})
    pred_cols = _collect_column_refs(predicate)
    for col in pred_cols:
        if col in input_schema:
            state.add_edge(source_table, col, col, "filter")

    # Passthrough: all input columns flow through as direct
    for col in input_schema:
        state.add_edge(source_table, col, col, "direct")

    return dict(input_schema)


def _handle_join(body: PlanNode, state: _WalkState) -> Schema:
    """Handle a Join node.

    - Recurse into both inputs.
    - Register IdentityGroups for each (left_col, right_col) key pair.
    - Emit direct edges for all columns from both sides.
    """
    left_schema = _walk_node(body.get("input_left", {}), state)
    right_schema = _walk_node(body.get("input_right", {}), state)

    # Derive stable source labels for left / right
    left_label = _derive_source_label(body.get("input_left", {}), left_schema)
    right_label = _derive_source_label(body.get("input_right", {}), right_schema)

    # Register identity groups for each key pair
    for l_expr, r_expr in zip(body.get("left_on", []), body.get("right_on", [])):
        lc = l_expr.get("Column") if isinstance(l_expr, dict) else None
        rc = r_expr.get("Column") if isinstance(r_expr, dict) else None
        if lc and rc:
            state.add_identity(left_label, lc, right_label, rc)

    # Emit direct edges for all columns from both inputs
    output_schema: Schema = {}
    for col, typ in left_schema.items():
        state.add_edge(left_label, col, col, "direct")
        output_schema[col] = typ
    for col, typ in right_schema.items():
        # Suffix handling: if same name exists from left, use right as-is
        # (Polars adds a suffix, but we don't know it statically; emit both)
        state.add_edge(right_label, col, col, "direct")
        if col not in output_schema:
            output_schema[col] = typ

    return output_schema


def _handle_map_function(body: PlanNode, state: _WalkState) -> Schema:
    """Handle a MapFunction node.

    If function is "Rename" → emit direct edges with remapped names.
    Otherwise treat all output columns as opaque.
    """
    input_schema = _walk_node(body.get("input", {}), state)
    source_table = _derive_source_label(body.get("input", {}), input_schema)
    function = body.get("function", {})

    output_schema: Schema = dict(input_schema)

    if isinstance(function, dict) and "Rename" in function:
        rename_body = function["Rename"]
        existing = rename_body.get("existing", [])
        new_names = rename_body.get("new", [])
        rename_map = dict(zip(existing, new_names))
        for old, new in rename_map.items():
            state.add_edge(source_table, old, new, "direct")
            if old in output_schema:
                output_schema[new] = output_schema.pop(old)
    else:
        # Opaque map: emit an opaque edge for every column
        for col in input_schema:
            state.add_edge(source_table, col, col, "opaque")

    return output_schema


def _handle_groupby(body: PlanNode, state: _WalkState) -> Schema:
    """Handle a GroupBy node.

    Keys → direct lineage.
    Aggs → derived lineage (Alias wraps an Agg expression).
    """
    input_schema = _walk_node(body.get("input", {}), state)
    source_table = _derive_source_label(body.get("input", {}), input_schema)
    output_schema: Schema = {}

    # Key columns — direct
    for key_expr in body.get("keys", []):
        col_name = key_expr.get("Column") if isinstance(key_expr, dict) else None
        if col_name:
            state.add_edge(source_table, col_name, col_name, "direct")
            output_schema[col_name] = input_schema.get(col_name, "")

    # Aggregation expressions — derived
    for agg_expr in body.get("aggs", []):
        col_name, lineage_type, expression, src_col = _resolve_expr(
            agg_expr, input_schema
        )
        if col_name is None:
            continue
        effective_src = src_col or col_name
        state.add_edge(source_table, effective_src, col_name, lineage_type, expression)
        output_schema[col_name] = ""

    return output_schema


# ---------------------------------------------------------------------------
# Expression resolver
# ---------------------------------------------------------------------------


def _resolve_expr(
    expr: Any,
    input_schema: Schema,
) -> tuple[str | None, str, str | None, str | None]:
    """Resolve an expression to (target_col, lineage_type, expression, source_col).

    source_col is set only for direct/filter edges where source != target.
    Returns (None, ...) if the expression cannot be resolved.
    """
    if not isinstance(expr, dict):
        return None, "direct", None, None

    key = next(iter(expr))
    body = expr[key]

    if key == "Column":
        # Plain column reference — direct passthrough
        return body, "direct", None, body

    if key == "Alias":
        # body = [inner_expr, alias_name]
        inner_expr, alias_name = body[0], body[1]
        return _resolve_alias(inner_expr, alias_name, input_schema)

    if key == "Agg":
        # Bare aggregation without alias — use first referenced column as name
        inner_key = next(iter(body))
        src_cols = _collect_column_refs(body[inner_key])
        src = src_cols[0] if src_cols else "unknown"
        return src, "derived", _expr_repr(expr), src

    # Fallback for other expression types
    src_cols = _collect_column_refs(expr)
    src = src_cols[0] if src_cols else "unknown"
    return src, "derived", _expr_repr(expr), src


def _resolve_alias(
    inner_expr: Any,
    alias_name: str,
    input_schema: Schema,
) -> tuple[str | None, str, str | None, str | None]:
    """Resolve an Alias(inner_expr, alias_name) expression."""
    if _is_opaque_expr(inner_expr):
        # Opaque: use alias_name as both source and target column
        return alias_name, "opaque", None, alias_name

    if not isinstance(inner_expr, dict):
        return alias_name, "derived", str(inner_expr), None

    inner_key = next(iter(inner_expr))

    if inner_key == "Column":
        # Simple rename / identity: direct
        src_col = inner_expr["Column"]
        return alias_name, "direct", None, src_col

    # Everything else is derived
    expression = _expr_repr(inner_expr)
    src_cols = _collect_column_refs(inner_expr)
    src = src_cols[0] if src_cols else alias_name
    return alias_name, "derived", expression, src


# ---------------------------------------------------------------------------
# Source label derivation
# ---------------------------------------------------------------------------


def _derive_source_label(input_node: PlanNode, input_schema: Schema) -> str:
    """Derive a stable string label for the source of a node.

    - DataFrameScan → use first column name as label (stable, schema-derived)
    - Scan → use file path stem if available, else "scan"
    - Join → "join"
    - Anything else → "source"
    """
    if not isinstance(input_node, dict) or not input_node:
        return "source"
    node_type = next(iter(input_node))
    body = input_node[node_type]

    if node_type == "DataFrameScan":
        fields = body.get("schema", {}).get("fields", {})
        if fields:
            # Use a label derived from sorted column names for stability
            return "_".join(sorted(fields.keys())[:2])
        return "datasource"

    if node_type == "Scan":
        try:
            paths = body.get("sources", {}).get("Paths", [])
            if paths:
                import pathlib

                return pathlib.Path(paths[0]["inner"]).stem
        except Exception:
            pass
        return "scan"

    if node_type == "Join":
        return "join"

    # For compound nodes (HStack, Filter, Select wrapping something) recurse
    if isinstance(body, dict) and "input" in body:
        return _derive_source_label(body["input"], input_schema)

    return "source"


# ---------------------------------------------------------------------------
# Public walker class
# ---------------------------------------------------------------------------


class PolarsWalker(PlanWalker):
    """Walker for Polars LazyFrame-based Dagster assets.

    Takes a JSON string from ``LazyFrame.serialize(format="json")`` as *plan*
    and recursively dispatches on node type to infer column-level lineage.
    """

    def __init__(self) -> None:
        self._state: _WalkState | None = None

    # ------------------------------------------------------------------
    # PlanWalker interface
    # ------------------------------------------------------------------

    def walk(
        self,
        plan: str,
        output_schema: pa.Schema,
        asset_name: str,
    ) -> list[ColumnLineage]:
        """Parse the Polars plan JSON and return column-level lineage edges."""
        try:
            root = json.loads(plan)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid Polars plan JSON: {exc}") from exc

        self._state = _WalkState(asset_name, output_schema)
        _walk_node(root, self._state)
        return list(self._state.edges)

    @property
    def identity_groups(self) -> list[IdentityGroup]:
        if self._state is None:
            return []
        return list(self._state.identity_groups)
