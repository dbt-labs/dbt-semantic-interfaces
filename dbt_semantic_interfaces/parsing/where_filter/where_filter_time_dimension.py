from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import ParameterSetFactory
from dbt_semantic_interfaces.parsing.where_filter.query_interface import (
    QueryInterfaceTimeDimension,
    QueryInterfaceTimeDimensionFactory,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from typing_extensions import override


class TimeDimensionStub(ProtocolHint[QueryInterfaceTimeDimension]):
    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimension:
        return self


class WhereFilterTimeDimensionFactory(ProtocolHint[QueryInterfaceTimeDimensionFactory]):
    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimensionFactory:
        return self

    def __init__(self):  # noqa
        self.time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []

    def create(
        self, time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
    ) -> TimeDimensionStub:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}."""
        self.time_dimension_call_parameter_sets.append(
            ParameterSetFactory.create_time_dimension(time_dimension_name, time_granularity_name, entity_path)
        )
        return TimeDimensionStub()
