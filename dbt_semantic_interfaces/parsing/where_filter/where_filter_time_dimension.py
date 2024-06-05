from __future__ import annotations

from typing import List, Optional, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.call_parameter_sets import TimeDimensionCallParameterSet
from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_stubs import (
    TimeDimensionStub,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceTimeDimensionFactory,
)
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity


class WhereFilterTimeDimensionFactory(ProtocolHint[QueryInterfaceTimeDimensionFactory]):
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimensionFactory:
        return self

    def __init__(self) -> None:  # noqa
        self.time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        descending: Optional[bool] = None,
        date_part_name: Optional[str] = None,
    ) -> TimeDimensionStub:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}."""
        if descending is not None:
            raise InvalidQuerySyntax("descending is invalid in the where parameter and filter spec")
        self.time_dimension_call_parameter_sets.append(
            ParameterSetFactory.create_time_dimension(
                time_dimension_name=time_dimension_name,
                time_granularity_name=time_granularity_name,
                entity_path=entity_path,
                date_part_name=date_part_name,
            )
        )
        return TimeDimensionStub(
            element_name=time_dimension_name,
            time_granularity=TimeGranularity(time_granularity_name.lower()) if time_granularity_name else None,
            entity_path=entity_path,
            date_part=DatePart(date_part_name.lower()) if date_part_name else None,
        )
