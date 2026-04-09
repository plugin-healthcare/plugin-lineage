# src/lineage/walkers/sql_walker.py
from __future__ import annotations

import pyarrow as pa
import sqlglot
import sqlglot.expressions as exp
from sqlglot.lineage import lineage as sqlglot_lineage

from lineage.models import ColumnLineage, IdentityGroup, IdentityMember
from lineage.walkers.base import PlanWalker


_ARROW_TO_SQL: dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "TINYINT",
    "uint16": "SMALLINT",
    "uint32": "INT",
    "uint64": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "float16": "FLOAT",
    "string": "VARCHAR",
    "large_string": "VARCHAR",
    "bool": "BOOLEAN",
    "date32[day]": "DATE",
    "date64[ms]": "DATE",
}


def _arrow_schema_to_flat_dict(schema: pa.Schema) -> dict[str, str]:
    """Return a flat ``{col_name: SQL_TYPE}`` dict from an Arrow schema."""
    result: dict[str, str] = {}
    for i in range(len(schema)):
        field = schema.field(i)
        sql_type = _ARROW_TO_SQL.get(str(field.type), "VARCHAR")
        result[field.name] = sql_type
    return result


def _build_sqlglot_schema(
    flat: dict[str, str],
    table_names: set[str],
) -> dict[str, dict[str, str]] | None:
    """Nest the flat column dict under every table name found in the SQL.

    sqlglot requires ``{"table": {"col": "TYPE"}}``; distributing all columns
    under every table is safe — sqlglot's qualifier resolves ambiguity via
    the SQL context.  Returns ``None`` when the schema is empty so that
    callers can pass ``schema=None`` to ``sqlglot.lineage.lineage()``.
    """
    if not flat or not table_names:
        return None
    return {t: flat for t in table_names}


def _build_alias_map(parsed: exp.Expression) -> dict[str, str]:
    """Return a mapping of table-alias -> real table name from a parsed SQL expression."""
    alias_map: dict[str, str] = {}
    for table in parsed.find_all(exp.Table):
        if table.alias:
            alias_map[table.alias] = table.name
    return alias_map


def _resolve_table(name: str, alias_map: dict[str, str]) -> str:
    return alias_map.get(name, name)


def _classify_and_collect(
    col: str,
    sql: str,
    sqlglot_schema: dict[str, dict[str, str]] | None,
    asset_name: str,
    alias_map: dict[str, str],
) -> list[ColumnLineage]:
    """Run sqlglot lineage for *col* and return ``ColumnLineage`` objects.

    Classification algorithm
    ~~~~~~~~~~~~~~~~~~~~~~~~
    Walk all nodes:
    - Collect leaf nodes (``isinstance(n.source, exp.Table)``).
    - Track the deepest Select-source node's expression (the one closest to
      the base tables, i.e. last encountered while walking).

    If there are **multiple leaves** → ``derived`` (expression aggregates
    multiple source columns, e.g. ``price * qty``).

    If there is exactly **one leaf** → inspect the deepest Select-source
    node's inner expression (unwrap ``Alias`` → ``.this``):
    - ``isinstance(inner, exp.Column)`` → ``direct``
    - otherwise → ``derived``
    """
    node = sqlglot_lineage(col, sql, schema=sqlglot_schema)

    leaves: list = []
    deepest_select_expr: exp.Expression | None = None

    for n in node.walk():
        if isinstance(n.source, exp.Table):
            leaves.append(n)
        else:
            deepest_select_expr = n.expression

    if not leaves:
        return []

    # Determine lineage_type and expression string
    if len(leaves) > 1:
        lineage_type = "derived"
        expression = deepest_select_expr.sql() if deepest_select_expr else None
    else:
        inner = (
            deepest_select_expr.this
            if isinstance(deepest_select_expr, exp.Alias)
            else deepest_select_expr
        )
        if isinstance(inner, exp.Column):
            lineage_type = "direct"
            expression = None
        else:
            lineage_type = "derived"
            expression = deepest_select_expr.sql() if deepest_select_expr else None

    edges: list[ColumnLineage] = []
    for leaf in leaves:
        source_table = _resolve_table(leaf.source.name, alias_map)
        source_col = leaf.name.split(".")[-1]
        edges.append(
            ColumnLineage(
                source_table=source_table,
                source_column=source_col,
                target_table=asset_name,
                target_column=col,
                lineage_type=lineage_type,
                expression=expression,
            )
        )
    return edges


class SQLLineageWalker(PlanWalker):
    """Walker for SQL-based Dagster assets.

    Takes a SQL query string as *plan* and uses ``sqlglot.lineage`` to infer
    column-level lineage without requiring a live database.
    """

    def __init__(self) -> None:
        self._identity_groups: list[IdentityGroup] = []

    # ------------------------------------------------------------------
    # PlanWalker interface
    # ------------------------------------------------------------------

    def walk(
        self,
        plan: str,
        output_schema: pa.Schema,
        asset_name: str,
    ) -> list[ColumnLineage]:
        """Parse *plan* (a SQL string) and return column-level lineage edges.

        Parameters
        ----------
        plan:
            The SQL query string.
        output_schema:
            Arrow schema of the asset output. Used for ``SELECT *`` expansion
            when the columns cannot be inferred from the SQL alone.
        asset_name:
            Dagster asset name; becomes ``target_table`` on every edge.
        """
        self._identity_groups = []

        try:
            parsed = sqlglot.parse_one(plan)
        except sqlglot.errors.ParseError as exc:
            raise ValueError(f"Invalid SQL: {exc}") from exc

        alias_map = _build_alias_map(parsed)
        # Real (non-alias) table names present in the SQL.
        real_tables: set[str] = {t.name for t in parsed.find_all(exp.Table) if t.name}

        flat_schema = _arrow_schema_to_flat_dict(output_schema)
        sqlglot_schema = _build_sqlglot_schema(flat_schema, real_tables)

        # Determine output column names.
        # SELECT * → use output_schema column names.
        named_selects: list[str] = parsed.named_selects
        if named_selects == ["*"]:
            named_selects = [
                output_schema.field(i).name for i in range(len(output_schema))
            ]

        # Extract JOIN identity groups from the parsed AST.
        self._identity_groups = _extract_identity_groups(parsed, alias_map)

        # Collect lineage edges for every output column.
        edges: list[ColumnLineage] = []
        for col in named_selects:
            edges.extend(
                _classify_and_collect(col, plan, sqlglot_schema, asset_name, alias_map)
            )
        return edges

    @property
    def identity_groups(self) -> list[IdentityGroup]:
        return list(self._identity_groups)


# ---------------------------------------------------------------------------
# Identity group extraction
# ---------------------------------------------------------------------------


def _extract_identity_groups(
    parsed: exp.Expression,
    alias_map: dict[str, str],
) -> list[IdentityGroup]:
    """Walk the parsed AST for JOIN … ON predicates and build IdentityGroups."""
    groups: list[IdentityGroup] = []
    seen_keys: set[str] = set()

    for join in parsed.find_all(exp.Join):
        on_clause = join.args.get("on")
        if on_clause is None:
            continue
        for eq in on_clause.find_all(exp.EQ):
            left, right = eq.left, eq.right
            if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
                continue
            lt = _resolve_table(left.table or "", alias_map)
            lc = left.name
            rt = _resolve_table(right.table or "", alias_map)
            rc = right.name
            key = "::".join(sorted([f"{lt}.{lc}", f"{rt}.{rc}"]))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            members = [
                IdentityMember(table=lt, column=lc),
                IdentityMember(table=rt, column=rc),
            ]
            groups.append(IdentityGroup(key=key, members=members))
    return groups
