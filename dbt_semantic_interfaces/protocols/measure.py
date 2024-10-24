from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from dbt_semantic_interfaces.protocols.meta import ConfigMeta
from dbt_semantic_interfaces.references import MeasureReference
from dbt_semantic_interfaces.type_enums import AggregationType


class NonAdditiveDimensionParameters(Protocol):
    """Describes the params for specifying non-additive dimensions in a measure."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def window_choice(self) -> AggregationType:  # noqa: D
        pass

    @property
    @abstractmethod
    def window_groupings(self) -> Sequence[str]:  # noqa: D
        pass


class MeasureAggregationParameters(Protocol):
    """Describes parameters for aggregations."""

    @property
    @abstractmethod
    def percentile(self) -> Optional[float]:  # noqa: D
        pass

    @property
    @abstractmethod
    def use_discrete_percentile(self) -> bool:  # noqa: D
        pass

    @property
    @abstractmethod
    def use_approximate_percentile(self) -> bool:  # noqa: D
        pass


class Measure(Protocol):
    """Describes a measure.

    Measure is a field in the underlying semantic model that can be aggregated
    in a specific way.
    """

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def agg(self) -> AggregationType:  # noqa: D
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def expr(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def agg_params(self) -> Optional[MeasureAggregationParameters]:  # noqa: D
        pass

    @property
    @abstractmethod
    def non_additive_dimension(self) -> Optional[NonAdditiveDimensionParameters]:  # noqa: D
        pass

    @property
    @abstractmethod
    def agg_time_dimension(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def reference(self) -> MeasureReference:
        """Returns a reference to this measure."""
        ...

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """Returns a string representing a human readable label for the measure."""
        pass

    @property
    @abstractmethod
    def config(self) -> Optional[ConfigMeta]:  # noqa: D
        pass
