from __future__ import annotations

from typing import List, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.call_parameter_sets import (
    EntityCallParameterSet,
    MetricCallParameterSet,
)
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_stubs import (
    EntityStub,
    MetricStub,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceEntityFactory,
    QueryInterfaceMetricFactory,
)


class WhereFilterEntityFactory(ProtocolHint[QueryInterfaceEntityFactory]):
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    @override
    def _implements_protocol(self) -> QueryInterfaceEntityFactory:
        return self

    def __init__(self) -> None:  # noqa
        self.entity_call_parameter_sets: List[EntityCallParameterSet] = []

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> EntityStub:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        self.entity_call_parameter_sets.append(ParameterSetFactory.create_entity(entity_name, entity_path))
        return EntityStub(element_name=entity_name, entity_links=entity_path)


class WhereFilterMetricFactory(ProtocolHint[QueryInterfaceMetricFactory]):
    """Executes in the Jinja sandbox to produce parameter sets and append them to a list."""

    @override
    def _implements_protocol(self) -> QueryInterfaceMetricFactory:
        return self

    def __init__(self) -> None:  # noqa: D
        self.metric_call_parameter_sets: List[MetricCallParameterSet] = []

    def create(self, metric_name: str, group_by: Sequence[str] = ()) -> MetricStub:  # noqa: D
        self.metric_call_parameter_sets.append(
            ParameterSetFactory.create_metric(metric_name=metric_name, group_by=group_by)
        )
        return MetricStub()
