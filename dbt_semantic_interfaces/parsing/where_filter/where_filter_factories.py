from __future__ import annotations

from typing import List, Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
    MetricCallParameterSet,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_objects import (
    WhereFilterDimension,
    WhereFilterEntity,
    WhereFilterMetric,
    WhereFilterTimeDimension,
)
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity


class WhereFilterEntityFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa
        self.entity_call_parameter_sets: List[EntityCallParameterSet] = []

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> WhereFilterEntity:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        self.entity_call_parameter_sets.append(ParameterSetFactory.create_entity(entity_name, entity_path))
        return WhereFilterEntity(element_name=entity_name, entity_links=entity_path)


class WhereFilterMetricFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa: D
        self.metric_call_parameter_sets: List[MetricCallParameterSet] = []

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> WhereFilterMetric:
        """Create a WhereFilterMetric.

        Note that group_by is required, but uses a default arg here so that we can return a readable error to the user
        if they leave it out.
        """
        self.metric_call_parameter_sets.append(
            ParameterSetFactory.create_metric(metric_name=metric_name, group_by=group_by)
        )
        return WhereFilterMetric(element_name=metric_name, group_by=group_by)


class WhereFilterDimensionFactory:
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to `created`.
    """

    def __init__(self) -> None:  # noqa
        self.created: List[WhereFilterDimension] = []

    def create(self, dimension_name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        dimension = WhereFilterDimension(dimension_name, entity_path)
        self.created.append(dimension)
        return dimension


class WhereFilterTimeDimensionFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa
        self.time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []

    def create(
        self,
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        descending: Optional[bool] = None,
        date_part_name: Optional[str] = None,
    ) -> WhereFilterTimeDimension:
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
        return WhereFilterTimeDimension(
            element_name=time_dimension_name,
            time_granularity=TimeGranularity(time_granularity_name.lower()) if time_granularity_name else None,
            entity_path=entity_path,
            date_part_name=DatePart(date_part_name.lower()) if date_part_name else None,
        )
