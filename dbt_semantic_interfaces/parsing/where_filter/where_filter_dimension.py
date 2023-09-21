from __future__ import annotations

from typing import List, Optional, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(  # noqa
        self,
        name: str,
        entity_path: Sequence[str],
    ) -> None:
        self.name = name
        self.entity_path = entity_path
        self.time_granularity_name: Optional[str] = None

    def grain(self, time_granularity: str) -> QueryInterfaceDimension:
        """The time granularity."""
        self.time_granularity_name = time_granularity
        return self

    def descending(self, _is_descending: bool) -> QueryInterfaceDimension:
        """Set the sort order for order-by."""
        raise InvalidQuerySyntax("descending is invalid in the where parameter and filter spec")

    def date_part(self, _date_part: str) -> QueryInterfaceDimension:
        """Date part to extract from the dimension."""
        raise InvalidQuerySyntax("date_part isn't currently supported in the where parameter and filter spec")


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to `created`.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def __init__(self) -> None:  # noqa
        self.created: List[WhereFilterDimension] = []

    def create(self, dimension_name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        dimension = WhereFilterDimension(dimension_name, entity_path)
        self.created.append(dimension)
        return dimension
