# src/lineage/walkers/base.py
from abc import ABC, abstractmethod

import pyarrow as pa

from lineage.models import ColumnLineage, IdentityGroup


class PlanWalker(ABC):
    """Abstract base class for all plan walkers.

    Concrete implementations must provide:
    - ``walk()``: parse a plan string and return column-level lineage edges.
    - ``identity_groups``: the identity groups accumulated during ``walk()``.
    """

    @abstractmethod
    def walk(
        self,
        plan: str,
        output_schema: pa.Schema,
        asset_name: str,
    ) -> list[ColumnLineage]:
        """Parse *plan* and return a list of :class:`~lineage.models.ColumnLineage` objects.

        Parameters
        ----------
        plan:
            For SQL walkers: the SQL query string.
            For Polars walkers: the JSON string from ``LazyFrame.serialize(format="json")``.
        output_schema:
            The Arrow schema of the asset's output table.  Used by ``Scan``
            nodes (file-backed sources) where schema cannot be inferred from
            the plan alone.
        asset_name:
            The Dagster asset name.  All returned ``ColumnLineage`` objects
            will have ``target_table`` set to this value.
        """

    @property
    @abstractmethod
    def identity_groups(self) -> list[IdentityGroup]:
        """Identity groups accumulated during the most recent ``walk()`` call."""
