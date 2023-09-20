from __future__ import annotations

from typing import List, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.call_parameter_sets import TimeDimensionCallParameterSet
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceTimeDimension,
    QueryInterfaceTimeDimensionFactory,
)


class TimeDimensionStub(ProtocolHint[QueryInterfaceTimeDimension]):
    """A TimeDimension implementation that just satisfies the protocol.

    QueryInterfaceTimeDimension currently has no methods and the parameter set is created in the factory.
    So, there is nothing to do here.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimension:
        return self


class WhereFilterTimeDimensionFactory(ProtocolHint[QueryInterfaceTimeDimensionFactory]):
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimensionFactory:
        return self

    def __init__(self) -> None:  # noqa
        self.time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []

    def create(
        self, time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
    ) -> TimeDimensionStub:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}."""
        self.time_dimension_call_parameter_sets.append(
            ParameterSetFactory.create_time_dimension(time_dimension_name, time_granularity_name, entity_path)
        )
        return TimeDimensionStub()
