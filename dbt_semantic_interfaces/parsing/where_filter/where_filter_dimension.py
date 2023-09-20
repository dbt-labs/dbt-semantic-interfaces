from __future__ import annotations

from typing import List, Optional, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceDimension,
    QueryInterfaceDimensionFactory,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class WhereFilterDimension(ProtocolHint[QueryInterfaceDimension]):
    """A dimension that is passed in through the where filter parameter."""

    @override
    def _implements_protocol(self) -> QueryInterfaceDimension:
        return self

    def __init__(  # noqa
        self,
        name: str,
        entity_path: Sequence[str],
    ):
        self.name = name
        self.entity_path = entity_path
        self.time_granularity: Optional[TimeGranularity] = None

    def grain(self, time_granularity: str) -> QueryInterfaceDimension:
        """The time granularity."""
        self.time_granularity = TimeGranularity(time_granularity)
        return self


class WhereFilterDimensionFactory(ProtocolHint[QueryInterfaceDimensionFactory]):
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to `created`.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceDimensionFactory:
        return self

    def __init__(self):  # noqa
        self.created: List[WhereFilterDimension] = []

    def create(self, dimension_name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        dimension = WhereFilterDimension(dimension_name, entity_path)
        self.created.append(dimension)
        return dimension
