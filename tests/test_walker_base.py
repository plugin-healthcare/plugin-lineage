# tests/test_walker_base.py
import pyarrow as pa
import pytest

from lineage.models import ColumnLineage, IdentityGroup
from lineage.walkers.base import PlanWalker


class TestPlanWalkerABC:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            PlanWalker()

    def test_missing_walk_raises(self):
        class NoWalk(PlanWalker):
            @property
            def identity_groups(self) -> list[IdentityGroup]:
                return []

        with pytest.raises(TypeError):
            NoWalk()

    def test_missing_identity_groups_raises(self):
        class NoIdentityGroups(PlanWalker):
            def walk(
                self,
                plan: str,
                output_schema: pa.Schema,
                asset_name: str,
            ) -> list[ColumnLineage]:
                return []

        with pytest.raises(TypeError):
            NoIdentityGroups()

    def test_valid_subclass_instantiates(self):
        class MinimalWalker(PlanWalker):
            def walk(
                self,
                plan: str,
                output_schema: pa.Schema,
                asset_name: str,
            ) -> list[ColumnLineage]:
                return []

            @property
            def identity_groups(self) -> list[IdentityGroup]:
                return []

        walker = MinimalWalker()
        schema = pa.schema([pa.field("id", pa.int64())])
        result = walker.walk("SELECT 1", schema, "my_asset")
        assert isinstance(result, list)
        assert walker.identity_groups == []
