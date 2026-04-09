# tests/test_models.py
import pytest
import yaml
from pydantic import ValidationError

from lineage.models import ColumnLineage, IdentityGroup, IdentityMember


# ---------------------------------------------------------------------------
# ColumnLineage tests
# ---------------------------------------------------------------------------


class TestColumnLineage:
    def test_direct_valid(self):
        col = ColumnLineage(
            source_table="orders",
            source_column="price",
            target_table="my_asset",
            target_column="price",
        )
        assert col.lineage_type == "direct"
        assert col.expression is None

    def test_derived_requires_expression(self):
        with pytest.raises(ValidationError):
            ColumnLineage(
                source_table="orders",
                source_column="price",
                target_table="my_asset",
                target_column="discounted_price",
                lineage_type="derived",
                expression=None,
            )

    def test_derived_with_expression_valid(self):
        col = ColumnLineage(
            source_table="products",
            source_column="price",
            target_table="my_asset",
            target_column="discounted_price",
            lineage_type="derived",
            expression="price * 0.9",
        )
        assert col.lineage_type == "derived"
        assert col.expression == "price * 0.9"

    def test_opaque_no_expression_ok(self):
        col = ColumnLineage(
            source_table="orders",
            source_column="value",
            target_table="my_asset",
            target_column="value_mapped",
            lineage_type="opaque",
        )
        assert col.lineage_type == "opaque"
        assert col.expression is None

    def test_empty_source_column(self):
        with pytest.raises(ValidationError):
            ColumnLineage(
                source_table="orders",
                source_column="",
                target_table="my_asset",
                target_column="price",
            )

    def test_whitespace_column_stripped(self):
        col = ColumnLineage(
            source_table="orders",
            source_column=" id ",
            target_table="my_asset",
            target_column=" id ",
        )
        assert col.source_column == "id"
        assert col.target_column == "id"

    def test_frozen_immutable(self):
        col = ColumnLineage(
            source_table="orders",
            source_column="id",
            target_table="my_asset",
            target_column="id",
        )
        with pytest.raises(Exception):  # TypeError (Pydantic frozen) or AttributeError
            col.source_column = "x"

    def test_invalid_lineage_type(self):
        with pytest.raises(ValidationError):
            ColumnLineage(
                source_table="orders",
                source_column="id",
                target_table="my_asset",
                target_column="id",
                lineage_type="unknown",
            )

    def test_yaml_roundtrip(self):
        col = ColumnLineage(
            source_table="orders",
            source_column="price",
            target_table="my_asset",
            target_column="discounted_price",
            lineage_type="derived",
            expression="price * 0.9",
        )
        dumped = yaml.dump(col.model_dump())
        loaded_dict = yaml.safe_load(dumped)
        col2 = ColumnLineage.model_validate(loaded_dict)
        assert col == col2


# ---------------------------------------------------------------------------
# IdentityMember tests
# ---------------------------------------------------------------------------


class TestIdentityMember:
    def test_basic(self):
        m = IdentityMember(table="orders", column="item_id")
        assert m.table == "orders"
        assert m.column == "item_id"

    def test_frozen(self):
        m = IdentityMember(table="orders", column="item_id")
        with pytest.raises(Exception):
            m.table = "other"


# ---------------------------------------------------------------------------
# IdentityGroup tests
# ---------------------------------------------------------------------------


class TestIdentityGroup:
    def _make_group(self, *, friendly_name=None, resolved=False, key=None):
        members = [
            IdentityMember(table="orders", column="item_id"),
            IdentityMember(table="items", column="id"),
        ]
        computed_key = "items.id::orders.item_id"
        kwargs = dict(
            key=key if key is not None else computed_key,
            members=members,
        )
        if friendly_name is not None:
            kwargs["friendly_name"] = friendly_name
        if resolved:
            kwargs["resolved"] = resolved
        return IdentityGroup(**kwargs)

    def test_key_is_sorted(self):
        group = self._make_group()
        assert group.key == "items.id::orders.item_id"

    def test_auto_resolved_when_friendly_name(self):
        group = self._make_group(friendly_name="patient_id")
        assert group.resolved is True

    def test_unresolved_by_default(self):
        group = self._make_group()
        assert group.resolved is False

    def test_key_mismatch_raises(self):
        with pytest.raises(ValidationError):
            IdentityGroup(
                key="wrong.key::another.key",
                members=[
                    IdentityMember(table="orders", column="item_id"),
                    IdentityMember(table="items", column="id"),
                ],
            )

    def test_yaml_roundtrip(self):
        group = self._make_group(friendly_name="order_item_id")
        dumped = yaml.dump(group.model_dump())
        loaded_dict = yaml.safe_load(dumped)
        group2 = IdentityGroup.model_validate(loaded_dict)
        assert group == group2
