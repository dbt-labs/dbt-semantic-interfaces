from __future__ import annotations
from abc import abstractmethod

from typing import Protocol, Sequence


class QueryInterfaceDimension(Protocol):
    """Represents the interface for Dimension in the query interface."""

    @abstractmethod
    def grain(self, _grain: str) -> QueryInterfaceDimension:
        """The time granularity."""
        pass

    @abstractmethod
    def alias(self, _alias: str) -> QueryInterfaceDimension:
        """Renaming the column."""
        pass


class QueryInterfaceDimensionFactory(Protocol):
    """Creates a Dimension for the query interface.

    Represented as the Dimension constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(self, name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceDimension:
        """Create a QueryInterfaceDimension."""
        pass


class QueryInterfaceTimeDimension(Protocol):
    """Represents the interface for TimeDimension in the query interface."""

    pass


class QueryInterfaceTimeDimensionFactory(Protocol):
    """Creates a TimeDimension for the query interface.

    Represented as the TimeDimension constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: str,
        entity_path: Sequence[str] = (),
    ) -> QueryInterfaceTimeDimension:
        """Create a TimeDimension."""
        pass


class QueryInterfaceEntity(Protocol):
    """Represents the interface for Entity in the query interface."""

    pass


class QueryInterfaceEntityFactory(Protocol):
    """Creates an Entity for the query interface.

    Represented as the Entity constructor in the Jinja sandbox.
    """

    @abstractmethod
    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> QueryInterfaceEntity:
        """Create an Entity."""
        pass
