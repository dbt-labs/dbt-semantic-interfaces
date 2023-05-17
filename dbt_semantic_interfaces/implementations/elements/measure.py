from __future__ import annotations

from typing import List, Optional

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.metadata import Metadata
from dbt_semantic_interfaces.references import MeasureReference, TimeDimensionReference
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType


class NonAdditiveDimensionParameters(HashableBaseModel):
    """Describes the params for specifying non-additive dimensions in a measure.

    NOTE: Currently, only TimeDimensions are supported for this filter
    """

    name: str

    # Optional Fields
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = []


class MeasureAggregationParameters(HashableBaseModel):
    """Describes parameters for aggregations."""

    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


class Measure(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a measure."""

    name: str
    agg: AggregationType
    description: Optional[str]
    create_metric: Optional[bool]
    expr: Optional[str] = None
    agg_params: Optional[MeasureAggregationParameters]
    metadata: Optional[Metadata]
    non_additive_dimension: Optional[NonAdditiveDimensionParameters] = None
    agg_time_dimension: Optional[str] = None

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:  # noqa: D
        assert self.agg_time_dimension, (
            f"Aggregation time dimension for measure {self.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the default_agg_time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(element_name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.name)
