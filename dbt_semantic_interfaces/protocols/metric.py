from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol

from dbt_semantic_interfaces.objects.filters.where_filter import WhereFilter
from dbt_semantic_interfaces.references import MeasureReference

# from dbt_semantic_interfaces.type_enums.metric_type import MetricType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class MetricInputMeasure(Protocol):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    filter: Optional[WhereFilter]
    alias: Optional[str]

    @property
    @abstractmethod
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure."""
        ...

    @property
    @abstractmethod
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate."""
        ...


class MetricTimeWindow(Protocol):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc."""

    count: int
    granularity: TimeGranularity
