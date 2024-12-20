from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence, Set

from typing_extensions import override

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.protocols import Metric, ProtocolHint
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import (
    ConversionCalculationType,
    MetricType,
    PeriodAggregation,
    TimeGranularity,
)
from dsi_pydantic_shim import Field


class PydanticMetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    filter: Optional[PydanticWhereFilterIntersection]
    alias: Optional[str]
    join_to_timespine: bool = False
    fill_nulls_with: Optional[int] = None

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticMetricInputMeasure:
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input.

        For user input cases, the original YAML spec for a PydanticMetric included measure(s) specified as string names
        or lists of string names. As such, configs pre-dating the addition of this model type will only provide the
        base name for this object.
        """
        if isinstance(input, str):
            return PydanticMetricInputMeasure(name=input)
        else:
            raise ValueError(
                f"MetricInputMeasure inputs from model configs are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )

    @property
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure."""
        return MeasureReference(element_name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate."""
        return MeasureReference(element_name=self.alias or self.name)


class PydanticMetricTimeWindow(PydanticCustomInputParser, HashableBaseModel):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc."""

    count: int
    granularity: str

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticMetricTimeWindow:
        """Parses a MetricTimeWindow from a string input found in a user provided model specification.

        The MetricTimeWindow is always expected to be provided as a string in user-defined YAML configs.
        """
        if isinstance(input, str):
            return PydanticMetricTimeWindow.parse(window=input.lower())
        else:
            raise ValueError(
                f"MetricTimeWindow inputs from model configs are expected to always be of type string, but got "
                f"type {type(input)} with value: {input}"
            )

    @property
    def is_standard_granularity(self) -> bool:
        """Returns whether the window uses standard TimeGranularity."""
        return self.granularity.casefold() in {item.value.casefold() for item in TimeGranularity}

    @property
    def window_string(self) -> str:
        """Returns the string value of the time window."""
        return f"{self.count} {self.granularity}"

    @staticmethod
    def parse(window: str) -> PydanticMetricTimeWindow:
        """Returns window values if parsing succeeds, None otherwise."""
        parts = window.lower().split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, "
                "e.g., `28 days`",
            )

        granularity = parts[1]
        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return PydanticMetricTimeWindow(
            count=int(count),
            granularity=granularity,
        )


class PydanticConstantPropertyInput(HashableBaseModel):
    """Input of a constant property used in conversion metrics."""

    base_property: str
    conversion_property: str


class PydanticMetricInput(HashableBaseModel):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    name: str
    filter: Optional[PydanticWhereFilterIntersection]
    alias: Optional[str]
    offset_window: Optional[PydanticMetricTimeWindow]
    offset_to_grain: Optional[str]

    @property
    def as_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference associated with this metric input."""
        return MetricReference(element_name=self.name)

    @property
    def post_aggregation_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference with the aliased name, if appropriate."""
        return MetricReference(element_name=self.alias or self.name)


class PydanticConversionTypeParams(HashableBaseModel):
    """Type params to provide context for conversion metrics properties."""

    base_measure: PydanticMetricInputMeasure
    conversion_measure: PydanticMetricInputMeasure
    entity: str
    calculation: ConversionCalculationType = ConversionCalculationType.CONVERSION_RATE
    window: Optional[PydanticMetricTimeWindow]
    constant_properties: Optional[List[PydanticConstantPropertyInput]]


class PydanticCumulativeTypeParams(HashableBaseModel):
    """Type params to provide context for cumulative metrics properties."""

    window: Optional[PydanticMetricTimeWindow]
    grain_to_date: Optional[str]
    period_agg: PeriodAggregation = PeriodAggregation.FIRST


class PydanticMetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    measure: Optional[PydanticMetricInputMeasure]
    numerator: Optional[PydanticMetricInput]
    denominator: Optional[PydanticMetricInput]
    expr: Optional[str]
    # Legacy, supports custom grain through PydanticMetricTimeWindow changes (should deprecate though)
    window: Optional[PydanticMetricTimeWindow]
    # Legacy, will not support custom granularity
    grain_to_date: Optional[TimeGranularity]
    metrics: Optional[List[PydanticMetricInput]]
    conversion_type_params: Optional[PydanticConversionTypeParams]
    cumulative_type_params: Optional[PydanticCumulativeTypeParams]

    input_measures: List[PydanticMetricInputMeasure] = Field(default_factory=list)


class PydanticMetric(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[Metric]):
    """Describes a metric."""

    @override
    def _implements_protocol(self) -> Metric:  # noqa: D
        return self

    name: str
    description: Optional[str]
    type: MetricType
    type_params: PydanticMetricTypeParams
    filter: Optional[PydanticWhereFilterIntersection]
    metadata: Optional[PydanticMetadata]
    label: Optional[str] = None
    config: Optional[PydanticSemanticLayerElementConfig]
    time_granularity: Optional[str] = None

    @classmethod
    def parse_obj(cls, input: Any) -> PydanticMetric:
        """Adds custom parsing to the default method."""
        data = deepcopy(input)

        # Ensure grain_to_date is lowercased
        type_params = data.get("type_params") or {}
        grain_to_date = (type_params.get("cumulative_type_params") or {}).get("grain_to_date")
        if isinstance(grain_to_date, str):
            data["type_params"]["cumulative_type_params"]["grain_to_date"] = grain_to_date.lower()

        # Ensure offset_to_grain is lowercased
        input_metrics = type_params.get("metrics", [])
        if input_metrics:
            for input_metric in input_metrics:
                offset_to_grain = input_metric.get("offset_to_grain")
                if offset_to_grain and isinstance(offset_to_grain, str):
                    input_metric["offset_to_grain"] = offset_to_grain.lower()

        return super(HashableBaseModel, cls).parse_obj(data)

    @property
    def input_measures(self) -> Sequence[PydanticMetricInputMeasure]:
        """Return the complete list of input measure configurations for this metric."""
        return self.type_params.input_measures

    @property
    def measure_references(self) -> List[MeasureReference]:
        """Return the measure references associated with all input measure configurations for this metric."""
        return [x.measure_reference for x in self.input_measures]

    @property
    def input_metrics(self) -> Sequence[PydanticMetricInput]:
        """Return the associated input metrics for this metric."""
        if self.type is MetricType.SIMPLE or self.type is MetricType.CUMULATIVE or self.type is MetricType.CONVERSION:
            return ()
        elif self.type is MetricType.DERIVED:
            assert self.type_params.metrics is not None, f"{MetricType.DERIVED} should have type_params.metrics set"
            return self.type_params.metrics
        elif self.type is MetricType.RATIO:
            assert (
                self.type_params.numerator is not None and self.type_params.denominator is not None
            ), f"{self} is metric type {MetricType.RATIO}, so neither the numerator and denominator should not be None"
            return (self.type_params.numerator, self.type_params.denominator)
        else:
            assert_values_exhausted(self.type)

    @staticmethod
    def all_input_measures_for_metric(
        metric: Metric, metric_index: Dict[MetricReference, Metric]
    ) -> Set[MeasureReference]:
        """Gets all input measures for the metric, including those defined on input metrics (recursively)."""
        measures: Set[MeasureReference] = set()
        if metric.type is MetricType.SIMPLE or metric.type is MetricType.CUMULATIVE:
            assert (
                metric.type_params.measure is not None
            ), f"Metric {metric.name} should have a measure defined, but it does not."
            measures.add(metric.type_params.measure.measure_reference)
        elif metric.type is MetricType.DERIVED or metric.type is MetricType.RATIO:
            for input_metric in metric.input_metrics:
                nested_metric = metric_index.get(input_metric.as_reference)
                assert nested_metric, f"Could not find metric {input_metric.name} in semantic manifest."
                measures.update(
                    PydanticMetric.all_input_measures_for_metric(metric=nested_metric, metric_index=metric_index)
                )
        elif metric.type is MetricType.CONVERSION:
            conversion_type_params = metric.type_params.conversion_type_params
            assert conversion_type_params, "Conversion metric should have conversion_type_params."
            measures.add(conversion_type_params.base_measure.measure_reference)
            measures.add(conversion_type_params.conversion_measure.measure_reference)
        else:
            assert_values_exhausted(metric.type)

        return measures
