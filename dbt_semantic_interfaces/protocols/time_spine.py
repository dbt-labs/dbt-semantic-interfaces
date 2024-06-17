from abc import abstractmethod
from typing import Protocol

from dbt_semantic_interfaces.protocols.semantic_model import NodeRelation
from dbt_semantic_interfaces.type_enums import TimeGranularity


class TimeSpine(Protocol):
    """Describes the configuration for a time spine table.

    A time spine table is a table with at least one column containing dates at a specific grain.

    e.g. with day granularity:
    ...
    2020-01-01
    2020-01-02
    2020-01-03
    ...
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name used to reference this time spine."""
        pass

    @property
    @abstractmethod
    def node_relation(self) -> NodeRelation:
        """dbt model that represents the time spine."""  # noqa: D403
        pass

    @property
    @abstractmethod
    def base_column(self) -> str:
        """The name of the column in the time spine table that has the values at the base granularity."""
        pass

    @property
    @abstractmethod
    def base_granularity(self) -> TimeGranularity:
        """The grain of the dates in the base_column.

        Must map to one of the default TimeGranularity values, not a custom granularity.
        """
        pass
