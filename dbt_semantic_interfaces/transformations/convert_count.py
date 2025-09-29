from typing_extensions import override

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import AggregationType
from dbt_semantic_interfaces.type_enums.metric_type import MetricType

ONE = "1"


class ConvertCountToSumRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts any COUNT measures to SUM equivalent.

    This is a *legacy* behavior that will be irrelevant once measures are no longer supported.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for semantic_model in semantic_manifest.semantic_models:
            for measure in semantic_model.measures:
                if measure.agg == AggregationType.COUNT:
                    if measure.expr is None:
                        raise ModelTransformError(
                            f"Measure '{measure.name}' uses a COUNT aggregation, which requires an expr to be "
                            f"provided. Provide 'expr: 1' if a count of all rows is desired."
                        )
                    if measure.expr != ONE:
                        # Just leave it as SUM(1) if we want to count all
                        measure.expr = f"CASE WHEN {measure.expr} IS NOT NULL THEN 1 ELSE 0 END"
                    measure.agg = AggregationType.SUM
        return semantic_manifest


class ConvertCountMetricToSumRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Converts any COUNT metrics to SUM equivalent.

    This only applies to SIMPLE metrics.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for metric in semantic_manifest.metrics:
            if (
                metric.type == MetricType.SIMPLE
                and metric.type_params.metric_aggregation_params is not None
                and metric.type_params.metric_aggregation_params.agg == AggregationType.COUNT
            ):
                if metric.type_params.expr is None:
                    raise ModelTransformError(
                        f"Metric '{metric.name}' uses a COUNT aggregation, which requires an expr to be "
                        f"provided. Provide 'expr: 1' if a count of all rows is desired."
                    )
                if metric.type_params.expr != ONE:
                    metric.type_params.expr = f"CASE WHEN {metric.type_params.expr} IS NOT NULL THEN 1 ELSE 0 END"
                metric.type_params.metric_aggregation_params.agg = AggregationType.SUM
        return semantic_manifest
