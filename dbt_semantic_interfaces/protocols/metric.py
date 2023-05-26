from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Protocol, Sequence

from dbt_semantic_interfaces.protocols.where_filter import WhereFilter
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums.metric_type import MetricType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class MetricInputMeasure(Protocol):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:  # noqa: D
        pass

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

    @property
    @abstractmethod
    def count(self) -> int:  # noqa: D
        pass

    @property
    @abstractmethod
    def granularity(self) -> TimeGranularity:  # noqa: D
        pass


class MetricInput(Protocol):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def alias(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def offset_window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        pass

    @property
    @abstractmethod
    def offset_to_grain(self) -> Optional[TimeGranularity]:  # noqa: D
        pass

    @property
    @abstractmethod
    def as_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference associated with this metric input."""
        ...


class MetricTypeParams(Protocol):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    @property
    @abstractmethod
    def measure(self) -> Optional[MetricInputMeasure]:  # noqa: D
        pass

    @property
    @abstractmethod
    def measures(self) -> Optional[Sequence[MetricInputMeasure]]:  # noqa: D
        pass

    @property
    @abstractmethod
    def numerator(self) -> Optional[MetricInputMeasure]:  # noqa: D
        pass

    @property
    @abstractmethod
    def denominator(self) -> Optional[MetricInputMeasure]:  # noqa: D
        pass

    @property
    @abstractmethod
    def expr(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def window(self) -> Optional[MetricTimeWindow]:  # noqa: D
        pass

    @property
    @abstractmethod
    def grain_to_date(self) -> Optional[TimeGranularity]:  # noqa: D
        pass

    @property
    @abstractmethod
    def metrics(self) -> Optional[Sequence[MetricInput]]:  # noqa: D
        pass

    @property
    @abstractmethod
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator."""
        ...

    @property
    @abstractmethod
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator."""
        ...


class Metric(Protocol):
    """Describes a metric."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def type(self) -> MetricType:  # noqa: D
        pass

    @property
    @abstractmethod
    def type_params(self) -> MetricTypeParams:  # noqa: D
        pass

    @property
    @abstractmethod
    def filter(self) -> Optional[WhereFilter]:  # noqa: D
        pass

    @property
    @abstractmethod
    def input_measures(self: Metric) -> List[MetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        ...

    @property
    @abstractmethod
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric."""
        ...

    @property
    @abstractmethod
    def input_metrics(self) -> List[MetricInput]:
        """Return the associated input metrics for this metric."""
        ...
