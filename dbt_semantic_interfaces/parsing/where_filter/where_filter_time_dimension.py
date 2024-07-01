from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import TimeDimensionCallParameterSet
from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_dimension import (
    WhereFilterDimension,
)


class WhereFilterTimeDimensionFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa
        self.time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        date_part_name: Optional[str] = None,
    ) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}."""
        self.time_dimension_call_parameter_sets.append(
            ParameterSetFactory.create_time_dimension(
                time_dimension_name=time_dimension_name,
                time_granularity_name=time_granularity_name,
                entity_path=entity_path,
                date_part_name=date_part_name,
            )
        )
        return WhereFilterDimension(
            name=time_dimension_name,
            entity_path=entity_path,
            time_granularity_name=time_granularity_name,
            date_part_name=date_part_name,
        )

        # TODO: should we add grain() and date_part() methods here for consistency with WhereFilterDimensionFactory?
