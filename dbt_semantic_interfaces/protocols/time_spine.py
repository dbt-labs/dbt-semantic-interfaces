from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from dbt_semantic_interfaces.implementations.node_relation import NodeRelation
from dbt_semantic_interfaces.type_enums import TimeGranularity


class TimeSpine(Protocol):
    """Describes a table that contains dates at a specific time grain.

    One column must map to a standard granularity (one of the TimeGranularity enum members). Others might represent
    custom granularity columns. Custom granularity columns are not yet implemented.
    """

    @property
    @abstractmethod
    def node_relation(self) -> NodeRelation:
        """dbt model where this time spine lives."""  # noqa: D403
        pass

    @property
    @abstractmethod
    def primary_column(self) -> TimeSpinePrimaryColumn:
        """The column in the time spine that maps to one of our standard granularities."""
        pass


class TimeSpinePrimaryColumn(Protocol):
    """The column in the time spine that maps to one of our standard granularities."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The column name."""
        pass

    @property
    @abstractmethod
    def time_granularity(self) -> TimeGranularity:
        """The column name."""
        pass
