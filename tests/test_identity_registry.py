# tests/test_identity_registry.py
import pathlib

import pytest
import yaml

from lineage.models import IdentityGroup, IdentityMember
from lineage.persistence.identity_registry import IdentityRegistry


def _make_group(
    left_table: str,
    left_col: str,
    right_table: str,
    right_col: str,
    friendly_name: str | None = None,
) -> IdentityGroup:
    key = "::".join(sorted([f"{left_table}.{left_col}", f"{right_table}.{right_col}"]))
    members = []
    for part in sorted([f"{left_table}.{left_col}", f"{right_table}.{right_col}"]):
        t, c = part.rsplit(".", 1)
        members.append(IdentityMember(table=t, column=c))
    return IdentityGroup(key=key, members=members, friendly_name=friendly_name)


class TestIdentityRegistry:
    def test_add_pending(self, tmp_path):
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g = _make_group("orders", "item_id", "items", "id")
        reg.add(g)
        groups = reg.list_all()
        assert any(gr.key == g.key for gr in groups)

    def test_key_is_sorted(self, tmp_path):
        """Members given in any order → key is always alphabetically sorted."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        # Deliberately pass right before left
        g = _make_group("orders", "item_id", "items", "id")
        assert g.key == "items.id::orders.item_id"
        reg.add(g)
        loaded = reg.list_all()
        assert loaded[0].key == "items.id::orders.item_id"

    def test_auto_resolve_same_name(self, tmp_path):
        """Both members share the same column name → resolved=True on load."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g = _make_group("left", "id", "right", "id")
        reg.add(g)
        loaded = reg.list_all()
        entry = next(gr for gr in loaded if gr.key == g.key)
        assert entry.resolved is True

    def test_idempotent_add(self, tmp_path):
        """Same key added twice → only one entry in the registry."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g = _make_group("orders", "item_id", "items", "id")
        reg.add(g)
        reg.add(g)
        assert len(reg.list_all()) == 1

    def test_append_only(self, tmp_path):
        """Re-adding a resolved entry does NOT overwrite the friendly_name."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g_resolved = _make_group(
            "orders", "item_id", "items", "id", friendly_name="order_item_id"
        )
        reg.add(g_resolved)
        # Add bare version (no friendly_name)
        g_bare = _make_group("orders", "item_id", "items", "id")
        reg.add(g_bare)
        loaded = reg.list_all()
        entry = next(gr for gr in loaded if gr.key == g_resolved.key)
        assert entry.friendly_name == "order_item_id"
        assert entry.resolved is True

    def test_list_pending(self, tmp_path):
        """list_pending returns only unresolved entries."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g_pending = _make_group("orders", "item_id", "items", "id")
        g_resolved = _make_group(
            "users", "id", "orders", "user_id", friendly_name="user_identity"
        )
        reg.add(g_pending)
        reg.add(g_resolved)
        pending = reg.list_pending()
        assert all(not gr.resolved for gr in pending)
        resolved_keys = {gr.key for gr in reg.list_all() if gr.resolved}
        assert g_resolved.key in resolved_keys

    def test_resolve(self, tmp_path):
        """resolve(key, friendly_name) → entry becomes resolved=True with name set."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g = _make_group("orders", "item_id", "items", "id")
        reg.add(g)
        reg.resolve(g.key, "order_item_link")
        loaded = reg.list_all()
        entry = next(gr for gr in loaded if gr.key == g.key)
        assert entry.resolved is True
        assert entry.friendly_name == "order_item_link"

    def test_yaml_roundtrip(self, tmp_path):
        """Dump → reload → equal list[IdentityGroup]."""
        reg = IdentityRegistry(tmp_path / "identities.yaml")
        g1 = _make_group("orders", "item_id", "items", "id")
        g2 = _make_group(
            "users", "id", "orders", "user_id", friendly_name="user_identity"
        )
        reg.add(g1)
        reg.add(g2)
        # Create a fresh registry pointing at the same file
        reg2 = IdentityRegistry(tmp_path / "identities.yaml")
        loaded = reg2.list_all()
        assert {gr.key for gr in loaded} == {g1.key, g2.key}

    def test_sorted_keys_in_yaml(self, tmp_path):
        """The YAML file has keys in alphabetical order."""
        path = tmp_path / "identities.yaml"
        reg = IdentityRegistry(path)
        # Add in reverse alphabetical order
        reg.add(_make_group("z_table", "col", "a_table", "col"))
        reg.add(_make_group("m_table", "col", "b_table", "col"))
        raw = yaml.safe_load(path.read_text())
        keys = list(raw.keys())
        assert keys == sorted(keys)
