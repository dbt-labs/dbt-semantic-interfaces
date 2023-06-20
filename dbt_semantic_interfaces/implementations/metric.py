from __future__ import annotations

from typing import List, Optional, Sequence

from pydantic import Field, PrivateAttr
from typing_extensions import override

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.errors import ParsingException
from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.protocols import Metric, ProtocolHint
from dbt_semantic_interfaces.protocols.metric import DerivedMetricParameters
from dbt_semantic_interfaces.references import MeasureReference, MetricReference
from dbt_semantic_interfaces.type_enums import MetricType, TimeGranularity


class PydanticMetricInputMeasure(PydanticCustomInputParser, HashableBaseModel):
    """Provides a pointer to a measure along with metric-specific processing directives.

    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    filter: Optional[PydanticWhereFilter]
    alias: Optional[str]

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
    granularity: TimeGranularity

    @classmethod
    def _from_yaml_value(cls, input: PydanticParseableValueType) -> PydanticMetricTimeWindow:
        """Parses a MetricTimeWindow from a string input found in a user provided model specification.

        The MetricTimeWindow is always expected to be provided as a string in user-defined YAML configs.
        """
        if isinstance(input, str):
            return PydanticMetricTimeWindow.parse(input)
        else:
            raise ValueError(
                f"MetricTimeWindow inputs from model configs are expected to always be of type string, but got "
                f"type {type(input)} with value: {input}"
            )

    @staticmethod
    def parse(window: str) -> PydanticMetricTimeWindow:
        """Returns window values if parsing succeeds, None otherwise.

        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window
        is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingException(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, "
                "e.g., `28 days`",
            )

        granularity = parts[1]
        # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
        if granularity.endswith("s"):
            # months -> month
            granularity = granularity[:-1]
        if granularity not in [item.value for item in TimeGranularity]:
            raise ParsingException(
                f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
            )

        count = parts[0]
        if not count.isdigit():
            raise ParsingException(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return PydanticMetricTimeWindow(
            count=int(count),
            granularity=TimeGranularity(granularity),
        )


class PydanticMetricInput(HashableBaseModel):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    name: str
    filter: Optional[PydanticWhereFilter]
    alias: Optional[str]
    offset_window: Optional[PydanticMetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]

    @property
    def as_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference associated with this metric input."""
        return MetricReference(element_name=self.name)

    @property
    def post_aggregation_reference(self) -> MetricReference:
        """Property accessor to get the MetricReference with the aliased name, if appropriate."""
        return MetricReference(element_name=self.alias or self.name)


class PydanticMetricTypeParams(HashableBaseModel):
    """Type params add additional context to certain metric types (the context depends on the metric type)."""

    measure: Optional[PydanticMetricInputMeasure]
    numerator: Optional[PydanticMetricInput]
    denominator: Optional[PydanticMetricInput]
    expr: Optional[str]
    window: Optional[PydanticMetricTimeWindow]
    grain_to_date: Optional[TimeGranularity]
    metrics: Optional[List[PydanticMetricInput]]

    input_measures: List[PydanticMetricInputMeasure] = Field(default_factory=list)


class PydanticSimpleMetricParameters(HashableBaseModel):
    """Pydantic implementation of SimpleMetricParameters."""

    measure: PydanticMetricInputMeasure


class PydanticRatioMetricParameters(HashableBaseModel):
    """Pydantic implementation of RatioMetricParameters."""

    numerator: PydanticMetricInput
    denominator: PydanticMetricInput


class PydanticCumulativeMetricParameters(HashableBaseModel):
    """Pydantic implementation of CumulativeMetricParameters."""

    measure: PydanticMetricInputMeasure
    window: Optional[PydanticMetricTimeWindow]
    grain_to_date: Optional[TimeGranularity]


class PydanticDerivedMetricParameters(HashableBaseModel, ProtocolHint[DerivedMetricParameters]):
    """Pydantic implementation of DerivedMetricParameters."""

    def _implements_protocol(self) -> DerivedMetricParameters:
        return self

    expr: str
    metrics: List[PydanticMetricInput]


class PydanticMetric(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[Metric]):
    """Describes a metric."""

    def _implements_protocol(self) -> Metric:
        return self

    name: str
    description: Optional[str]
    type: MetricType
    type_params: PydanticMetricTypeParams
    filter: Optional[PydanticWhereFilter]
    metadata: Optional[PydanticMetadata]

    _simple_metric_parameters: Optional[PydanticSimpleMetricParameters] = PrivateAttr(default_factory=lambda: None)
    _ratio_metric_parameters: Optional[PydanticRatioMetricParameters] = PrivateAttr(default_factory=lambda: None)
    _cumulative_metric_parameters: Optional[PydanticCumulativeMetricParameters] = PrivateAttr(
        default_factory=lambda: None
    )
    _derived_metric_parameters: Optional[PydanticDerivedMetricParameters] = PrivateAttr(default_factory=lambda: None)

    @property
    @override
    def simple_metric_parameters(self) -> Optional[PydanticSimpleMetricParameters]:
        if self.type is not MetricType.SIMPLE:
            return None
        if self._simple_metric_parameters is None:
            assert self.type_params.measure is not None
            self._simple_metric_parameters = PydanticSimpleMetricParameters(measure=self.type_params.measure)
        return self._simple_metric_parameters

    @property
    @override
    def ratio_metric_parameters(self) -> Optional[PydanticRatioMetricParameters]:
        assert self.type is MetricType.RATIO
        if self._ratio_metric_parameters is None:
            assert self.type_params.numerator is not None
            assert self.type_params.denominator is not None
            self._ratio_metric_parameters = PydanticRatioMetricParameters(
                numerator=self.type_params.numerator,
                denominator=self.type_params.denominator,
            )
        return self._ratio_metric_parameters

    @property
    @override
    def cumulative_metric_parameters(self) -> Optional[PydanticCumulativeMetricParameters]:
        """If this describes a cumulative metric, then return the associated parameters (exception otherwise)."""
        if self.type is not MetricType.CUMULATIVE:
            return None
        if self._cumulative_metric_parameters is None:
            assert self.type_params.measure is not None
            self._cumulative_metric_parameters = PydanticCumulativeMetricParameters(
                measure=self.type_params.measure,
                window=self.type_params.window,
                grain_to_date=self.type_params.grain_to_date,
            )

        return self._cumulative_metric_parameters

    @property
    @override
    def derived_metric_parameters(self) -> Optional[PydanticDerivedMetricParameters]:
        """If this describes a derived metric, then return the associated parameters (exception otherwise)."""
        if self.type is not MetricType.DERIVED:
            return None
        if self._derived_metric_parameters is None:
            assert self.type_params.expr is not None
            assert self.type_params.metrics is not None
            self._derived_metric_parameters = PydanticDerivedMetricParameters(
                expr=self.type_params.expr,
                metrics=self.type_params.metrics,
            )
        return self._derived_metric_parameters

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
        if self.type is MetricType.SIMPLE or self.type is MetricType.CUMULATIVE:
            return ()
        elif self.type is MetricType.DERIVED:
            assert self.type_params.metrics is not None, f"{MetricType.DERIVED} should have type_params.metrics set"
            return self.type_params.metrics
        elif self.type is MetricType.RATIO:
            ratio_metric_parameters = self.ratio_metric_parameters
            assert ratio_metric_parameters is not None
            return (
                ratio_metric_parameters.numerator,
                ratio_metric_parameters.denominator,
            )
        else:
            assert_values_exhausted(self.type)
