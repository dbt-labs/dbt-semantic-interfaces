from abc import abstractmethod
from typing import List, Optional, Protocol, TypeVar

from dbt_semantic_interfaces.type_enums import TimeGranularity


class DimensionInput(Protocol):
    """Dimension input information necessary for the where filter."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the dimension."""
        pass

    @property
    @abstractmethod
    def entity_path(self) -> Optional[List[str]]:
        """An optional path of entities to get to the required dimension."""
        pass


class TimeDimensionInput(Protocol):
    """Time dimension input information necessary for the where filter."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the time dimension."""
        pass

    @property
    @abstractmethod
    def granularity(self) -> TimeGranularity:
        """Time granularity to be used in the filter for the time dimension."""
        pass

    @property
    @abstractmethod
    def entity_path(self) -> Optional[List[str]]:
        """An optional path of entities to get to the required time dimension."""
        pass


class EntityInput(Protocol):
    """Entity input information necessary for the where filter."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the entity."""
        pass

    @property
    @abstractmethod
    def entity_path(self) -> Optional[List[str]]:
        """An optional path of entities to get to the required entity."""
        pass


class WhereFilter(Protocol):
    """A filter that is applied using a WHERE filter in the generated SQL."""

    @property
    @abstractmethod
    def where_sql_template(self) -> str:
        """A template that describes how to render the SQL for a WHERE clause.

        Example: "{{ country }} = 'US' AND {{ ds }} >= '2023-0701' AND {{ user }} == 'SOME_USER_ID'"
        """
        pass

    @property
    @abstractmethod
    def input_dimensions(self) -> List[DimensionInput]:
        """The dimension inputs in the where_sql_template.

        Example corresponding to the example `where_sql_template`
        [DimensionInput(name='country', entity_path=['user']),]
        """
        pass

    @property
    @abstractmethod
    def input_time_dimensions(self) -> List[TimeDimensionInput]:
        """The time dimension inputs in the where_sql_template.

        Example corresponding to the example `where_sql_template`
        [TimeDimensionInput(name='ds', granularity=TimeGranularity.MONTH, entity_path=['transaction']),]
        """
        pass

    @property
    @abstractmethod
    def input_entities(self) -> List[EntityInput]:
        """The entity inputs in the where_sql_template.

        Example corresponding to the example `where_sql_template`
        [EntityInput(name='user'),]
        """
        pass


DimensionInputT = TypeVar("DimensionInputT", bound=DimensionInput)
TimeDimensionInputT = TypeVar("TimeDimensionInputT", bound=TimeDimensionInput)
EntityInputT = TypeVar("EntityInputT", bound=EntityInput)
WhereFilterT = TypeVar("WhereFilterT", bound=WhereFilter)
