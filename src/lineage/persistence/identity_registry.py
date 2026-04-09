# src/lineage/persistence/identity_registry.py
from __future__ import annotations

import pathlib
from typing import Any

import yaml

from lineage.models import IdentityGroup, IdentityMember

# ---------------------------------------------------------------------------
# identities.yaml format
# ---------------------------------------------------------------------------
# The file is a YAML mapping keyed by the sorted identity key string.
# Keys are always kept in alphabetical order (append-only, merge-conflict safe).
#
# items.id::orders.item_id:
#   key: items.id::orders.item_id
#   members:
#     - table: items
#       column: id
#     - table: orders
#       column: item_id
#   friendly_name: null
#   resolved: false
# ---------------------------------------------------------------------------


def _group_to_dict(group: IdentityGroup) -> dict[str, Any]:
    return {
        "key": group.key,
        "members": [{"table": m.table, "column": m.column} for m in group.members],
        "friendly_name": group.friendly_name,
        "resolved": group.resolved,
    }


def _dict_to_group(d: dict[str, Any]) -> IdentityGroup:
    members = [
        IdentityMember(table=m["table"], column=m["column"]) for m in d["members"]
    ]
    return IdentityGroup(
        key=d["key"],
        members=members,
        friendly_name=d.get("friendly_name"),
        resolved=d.get("resolved", False),
    )


def _auto_resolved(group: IdentityGroup) -> bool:
    """Return True if all members share the same column name (auto-resolve rule)."""
    col_names = {m.column for m in group.members}
    return len(col_names) == 1


class IdentityRegistry:
    """Read/write interface for ``identities.yaml``.

    The file is loaded on first access and written back on every mutating
    operation.  The in-memory state is always a ``dict[key, dict]`` sorted
    alphabetically by key.

    Parameters
    ----------
    path:
        Path to the ``identities.yaml`` file.  The file is created (empty)
        if it does not yet exist.
    """

    def __init__(self, path: pathlib.Path) -> None:
        self._path = pathlib.Path(path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_raw(self) -> dict[str, Any]:
        """Load the raw YAML dict (key → entry dict). Returns {} if file absent/empty."""
        if not self._path.exists():
            return {}
        content = self._path.read_text().strip()
        if not content:
            return {}
        data = yaml.safe_load(content)
        return data if isinstance(data, dict) else {}

    def _save_raw(self, data: dict[str, Any]) -> None:
        """Write *data* to the YAML file with keys sorted alphabetically."""
        sorted_data = dict(sorted(data.items()))
        self._path.write_text(
            yaml.dump(sorted_data, default_flow_style=False, sort_keys=True)
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, group: IdentityGroup) -> None:
        """Add *group* to the registry (append-only: existing entries are preserved).

        If the key already exists:
        - The entry is not overwritten.
        - If the existing entry has a ``friendly_name`` it is preserved.

        Auto-resolution: if all members share the same column name, the entry
        is stored with ``resolved=True``.
        """
        data = self._load_raw()

        if group.key in data:
            # Append-only: do not overwrite.  Only auto-resolve if not yet resolved.
            existing = data[group.key]
            if not existing.get("resolved", False) and _auto_resolved(group):
                existing["resolved"] = True
            # Preserve existing friendly_name if present
            self._save_raw(data)
            return

        # New entry
        entry = _group_to_dict(group)
        # Apply auto-resolve rule
        if _auto_resolved(group) and not entry["resolved"]:
            entry["resolved"] = True

        data[group.key] = entry
        self._save_raw(data)

    def resolve(self, key: str, friendly_name: str) -> None:
        """Mark the identity *key* as resolved with *friendly_name*.

        Raises
        ------
        KeyError
            If *key* is not in the registry.
        """
        data = self._load_raw()
        if key not in data:
            raise KeyError(f"Identity key not found: {key!r}")
        data[key]["friendly_name"] = friendly_name
        data[key]["resolved"] = True
        self._save_raw(data)

    def list_all(self) -> list[IdentityGroup]:
        """Return all identity groups, sorted by key."""
        data = self._load_raw()
        return [_dict_to_group(v) for v in data.values()]

    def list_pending(self) -> list[IdentityGroup]:
        """Return only unresolved identity groups."""
        return [g for g in self.list_all() if not g.resolved]
