from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
    MetricCallParameterSet,
)
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)


class WhereFilterEntityFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa: D
        self.entity_call_parameter_sets: List[EntityCallParameterSet] = []

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> None:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        self.entity_call_parameter_sets.append(ParameterSetFactory.create_entity(entity_name, entity_path))


# TODO: move to its own file!
class WhereFilterMetricFactory:
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    def __init__(self) -> None:  # noqa: D
        self.metric_call_parameter_sets: List[MetricCallParameterSet] = []

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> None:
        """Build call_parameter_sets and store on factory object."""
        self.metric_call_parameter_sets.append(
            ParameterSetFactory.create_metric(metric_name=metric_name, group_by=group_by)
        )
