# src/lineage/cli.py
"""plugin-lineage CLI — Typer application.

Commands
--------
ingest   Process raw plan files → write/update schema and mapping YAMLs.
lint     Validate all schemas and mappings (fully static).
diff     Semantic diff of metadata against a git ref.
name     Interactive resolver for pending identity groups.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
from typing import Optional

import pyarrow as pa
import questionary
import typer
from linkml_map.datamodel.transformer_model import TransformationSpecification
from linkml_runtime.dumpers import YAMLDumper
from linkml_runtime.linkml_model import SchemaDefinition
from linkml_runtime.loaders import YAMLLoader
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from lineage.models import ColumnLineage
from lineage.persistence.identity_registry import IdentityRegistry
from lineage.persistence.mapping_writer import (
    write_mapping_to_file,
    upsert_mapping_file,
)
from lineage.persistence.schema_writer import write_schema_to_file
from lineage.walkers.polars_walker import PolarsWalker
from lineage.walkers.sql_walker import SQLLineageWalker

app = typer.Typer(
    name="plugin-lineage",
    help="Column-level data lineage inference for Python-based data pipelines.",
    add_completion=False,
)

console = Console()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_POLARS_NODE_TYPES = {
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

_DEFAULT_RAW = pathlib.Path("metadata/raw")
_DEFAULT_SCHEMAS = pathlib.Path("metadata/schemas")
_DEFAULT_MAPPINGS = pathlib.Path("metadata/mappings")
_DEFAULT_IDENTITIES = pathlib.Path("metadata/identities.yaml")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _detect_plan_type(content: str, filename: str) -> str:
    """Return ``'sql'`` or ``'polars'`` based on file extension and content."""
    if filename.endswith(".sql"):
        return "sql"
    try:
        data = json.loads(content)
        if isinstance(data, dict) and next(iter(data)) in _POLARS_NODE_TYPES:
            return "polars"
    except (json.JSONDecodeError, StopIteration, ValueError):
        pass
    return "sql"


def _load_arrow_schema(schema_path: pathlib.Path) -> pa.Schema:
    """Load an Arrow schema from a ``{col: type_str}`` JSON file."""
    schema_dict: dict[str, str] = json.loads(schema_path.read_text())
    fields = [
        pa.field(name, pa.lib.ensure_type(type_str))
        for name, type_str in schema_dict.items()
    ]
    return pa.schema(fields)


def _git_show(ref: str, rel_path: str, cwd: str) -> str | None:
    """Return the content of *rel_path* at git *ref*, or ``None`` if absent."""
    result = subprocess.run(
        ["git", "show", f"{ref}:{rel_path}"],
        capture_output=True,
        text=True,
        cwd=cwd,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def _repo_root() -> str:
    """Return the absolute path to the git repository root."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "."


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command()
def ingest(
    raw_dir: pathlib.Path = typer.Option(
        _DEFAULT_RAW, "--raw-dir", help="Directory containing raw plan files."
    ),
    schemas_dir: pathlib.Path = typer.Option(
        _DEFAULT_SCHEMAS, "--schemas-dir", help="Output directory for schema YAMLs."
    ),
    mappings_dir: pathlib.Path = typer.Option(
        _DEFAULT_MAPPINGS, "--mappings-dir", help="Output directory for mapping YAMLs."
    ),
    identities_file: pathlib.Path = typer.Option(
        _DEFAULT_IDENTITIES, "--identities-file", help="Path to identities.yaml."
    ),
) -> None:
    """Process all plan files in RAW_DIR and write schema + mapping YAMLs."""
    raw_dir = pathlib.Path(raw_dir)
    if not raw_dir.exists():
        console.print(f"[yellow]Raw directory does not exist: {raw_dir}[/yellow]")
        typer.echo("No plans ingested (raw dir missing).")
        return

    # Collect plan files (*.sql, *.json — but not *.schema.json)
    plan_files = [
        f
        for f in sorted(raw_dir.iterdir())
        if f.is_file()
        and not f.name.endswith(".schema.json")
        and f.suffix in (".sql", ".json")
    ]

    if not plan_files:
        typer.echo("No plan files found — 0 assets ingested.")
        return

    reg = IdentityRegistry(identities_file)
    errors: list[str] = []

    for plan_file in plan_files:
        asset_name = plan_file.stem
        schema_path = raw_dir / f"{asset_name}.schema.json"
        if not schema_path.exists():
            console.print(
                f"[yellow]Missing schema file for {asset_name}, skipping.[/yellow]"
            )
            continue

        plan_content = plan_file.read_text()
        plan_type = _detect_plan_type(plan_content, plan_file.name)

        try:
            output_schema = _load_arrow_schema(schema_path)
        except Exception as exc:
            errors.append(f"{asset_name}: failed to load schema — {exc}")
            continue

        try:
            if plan_type == "sql":
                walker = SQLLineageWalker()
            else:
                walker = PolarsWalker()

            edges: list[ColumnLineage] = walker.walk(
                plan_content, output_schema, asset_name
            )
        except Exception as exc:
            errors.append(f"{asset_name}: failed to walk plan — {exc}")
            continue

        # Write / update schema
        write_schema_to_file(asset_name, output_schema, schemas_dir)

        # Write / upsert mapping
        mapping_path = mappings_dir / f"{asset_name}.mapping.yaml"
        if mapping_path.exists():
            upsert_mapping_file(edges, asset_name, mapping_path)
        else:
            write_mapping_to_file(edges, asset_name, mappings_dir)

        # Register identity groups
        for group in walker.identity_groups:
            reg.add(group)

        console.print(
            f"[green]✓[/green] Ingested [bold]{asset_name}[/bold] "
            f"({len(edges)} edges, {plan_type})"
        )

    if errors:
        for err in errors:
            console.print(f"[red]✗ {err}[/red]")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# lint
# ---------------------------------------------------------------------------


@app.command()
def lint(
    schemas_dir: pathlib.Path = typer.Option(_DEFAULT_SCHEMAS, "--schemas-dir"),
    mappings_dir: pathlib.Path = typer.Option(_DEFAULT_MAPPINGS, "--mappings-dir"),
    identities_file: pathlib.Path = typer.Option(
        _DEFAULT_IDENTITIES, "--identities-file"
    ),
) -> None:
    """Validate all schemas and mappings (fully static — no DB or runtime required)."""
    schemas_dir = pathlib.Path(schemas_dir)
    mappings_dir = pathlib.Path(mappings_dir)
    issues: list[str] = []

    # Load all schemas into a lookup: class_name → {slot_name}
    schema_slots: dict[str, set[str]] = {}
    if schemas_dir.exists():
        for schema_file in sorted(schemas_dir.glob("*.yaml")):
            try:
                sd: SchemaDefinition = YAMLLoader().loads(
                    schema_file.read_text(), target_class=SchemaDefinition
                )
                schema_slots[schema_file.stem] = set(sd.slots.keys())
            except Exception as exc:
                issues.append(f"Schema load error [{schema_file.name}]: {exc}")

    # Check each mapping file
    if mappings_dir.exists():
        for mapping_file in sorted(mappings_dir.glob("*.mapping.yaml")):
            try:
                ts: TransformationSpecification = YAMLLoader().loads(
                    mapping_file.read_text(), target_class=TransformationSpecification
                )
            except Exception as exc:
                issues.append(f"Mapping load error [{mapping_file.name}]: {exc}")
                continue

            for cd in ts.class_derivations:
                known_slots = schema_slots.get(cd.name, set())
                for slot_name, sd in cd.slot_derivations.items():
                    # Check 1: populated_from references must exist in schema
                    if sd.populated_from and known_slots:
                        if sd.populated_from not in known_slots:
                            issues.append(
                                f"[{cd.name}.{slot_name}] populated_from='{sd.populated_from}' "
                                f"not found in schema"
                            )
                    # Check 2: opaque columns without expr override
                    is_opaque = any("lineage: opaque" in c for c in (sd.comments or []))
                    if is_opaque and not sd.expr:
                        issues.append(
                            f"[{cd.name}.{slot_name}] opaque column has no expr override"
                        )

    if issues:
        table = Table(title="Lint Issues", show_header=True, header_style="bold red")
        table.add_column("Issue")
        for issue in issues:
            table.add_row(issue)
        console.print(table)
        raise typer.Exit(code=1)
    else:
        console.print("[green]✓ All checks passed.[/green]")


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


@app.command()
def diff(
    schemas_dir: pathlib.Path = typer.Option(_DEFAULT_SCHEMAS, "--schemas-dir"),
    mappings_dir: pathlib.Path = typer.Option(_DEFAULT_MAPPINGS, "--mappings-dir"),
    base: str = typer.Option("main", "--base", help="Git ref to diff against."),
) -> None:
    """Semantic diff of current metadata against a git ref (default: main).

    Reports:
    - Breaking Changes: deleted columns, type changes
    - Logic Drift: changed expr field in a SlotDerivation
    """
    schemas_dir = pathlib.Path(schemas_dir)
    mappings_dir = pathlib.Path(mappings_dir)
    cwd = _repo_root()
    breaking: list[str] = []
    drift: list[str] = []

    def _rel(file: pathlib.Path) -> str:
        """Return the repo-relative path for git show, falling back to the filename."""
        try:
            return str(file.relative_to(pathlib.Path(cwd)))
        except ValueError:
            return file.name

    # ── Schema diff ──────────────────────────────────────────────────────────
    if schemas_dir.exists():
        for schema_file in sorted(schemas_dir.glob("*.yaml")):
            base_content = _git_show(base, _rel(schema_file), cwd)
            if base_content is None:
                continue  # New file at HEAD — not a breaking change

            try:
                current_sd: SchemaDefinition = YAMLLoader().loads(
                    schema_file.read_text(), target_class=SchemaDefinition
                )
                base_sd: SchemaDefinition = YAMLLoader().loads(
                    base_content, target_class=SchemaDefinition
                )
            except Exception:
                continue

            current_slots = set(current_sd.slots.keys())
            base_slots = set(base_sd.slots.keys())

            # Deleted columns
            for deleted in base_slots - current_slots:
                breaking.append(f"[{schema_file.stem}] column deleted: '{deleted}'")

            # Type changes
            for col in base_slots & current_slots:
                base_range = base_sd.slots[col].range
                cur_range = current_sd.slots[col].range
                if base_range != cur_range:
                    breaking.append(
                        f"[{schema_file.stem}] type changed: '{col}' "
                        f"{base_range!r} → {cur_range!r}"
                    )

    # ── Mapping diff ─────────────────────────────────────────────────────────
    if mappings_dir.exists():
        for mapping_file in sorted(mappings_dir.glob("*.mapping.yaml")):
            base_content = _git_show(base, _rel(mapping_file), cwd)
            if base_content is None:
                continue

            try:
                current_ts: TransformationSpecification = YAMLLoader().loads(
                    mapping_file.read_text(), target_class=TransformationSpecification
                )
                base_ts: TransformationSpecification = YAMLLoader().loads(
                    base_content, target_class=TransformationSpecification
                )
            except Exception:
                continue

            # Index by class name
            base_cds = {cd.name: cd for cd in base_ts.class_derivations}
            cur_cds = {cd.name: cd for cd in current_ts.class_derivations}

            for class_name, cur_cd in cur_cds.items():
                base_cd = base_cds.get(class_name)
                if base_cd is None:
                    continue
                for slot_name, cur_sd in cur_cd.slot_derivations.items():
                    base_sd_slot = base_cd.slot_derivations.get(slot_name)
                    if base_sd_slot is None:
                        continue
                    if cur_sd.expr != base_sd_slot.expr:
                        drift.append(
                            f"[{class_name}.{slot_name}] logic drift: "
                            f"expr {base_sd_slot.expr!r} → {cur_sd.expr!r}"
                        )

    if not breaking and not drift:
        typer.echo("No changes detected.")
        return

    if breaking:
        tree = Tree("[bold red]Breaking Changes[/bold red]")
        for item in breaking:
            tree.add(item)
        console.print(tree)

    if drift:
        tree = Tree("[bold yellow]Logic Drift[/bold yellow]")
        for item in drift:
            tree.add(item)
        console.print(tree)

    raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# name
# ---------------------------------------------------------------------------


@app.command()
def name(
    identities_file: pathlib.Path = typer.Option(
        _DEFAULT_IDENTITIES, "--identities-file"
    ),
) -> None:
    """Interactive prompt to resolve pending identity groups."""
    identities_file = pathlib.Path(identities_file)
    reg = IdentityRegistry(identities_file)
    pending = reg.list_pending()

    if not pending:
        typer.echo("Nothing to name — no pending identities.")
        return

    console.print(f"[bold]{len(pending)} pending identity group(s) to name.[/bold]")

    for group in pending:
        console.print(f"\nIdentity key: [cyan]{group.key}[/cyan]")
        console.print(
            "Members: " + ", ".join(f"{m.table}.{m.column}" for m in group.members)
        )

        # Suggest the most frequent column name among members
        from collections import Counter

        col_counts = Counter(m.column for m in group.members)
        suggested = col_counts.most_common(1)[0][0]

        friendly = questionary.text(
            f"Friendly name for this identity [default: {suggested}]:",
        ).ask()

        if friendly is None:  # user cancelled (Ctrl-C)
            typer.echo("Aborted.")
            raise typer.Exit(code=1)

        friendly = friendly.strip() or suggested
        reg.resolve(group.key, friendly)
        console.print(f"[green]✓[/green] Resolved as [bold]{friendly!r}[/bold]")
