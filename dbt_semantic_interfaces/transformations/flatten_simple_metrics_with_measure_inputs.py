import logging
from typing import Optional, Tuple

from typing_extensions import override

from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class FlattenSimpleMetricsWithMeasureInputsRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Flattens simple metrics with measure inputs into a single metric with a measure input."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def _get_semantic_model_and_measure_for_input_measure(
        input_measure: PydanticMetricInputMeasure,
        semantic_manifest: PydanticSemanticManifest,
    ) -> Optional[Tuple[PydanticSemanticModel, PydanticMeasure]]:
        for model in semantic_manifest.semantic_models:
            for model_measure in model.measures:
                if model_measure.name == input_measure.name:
                    return model, model_measure
        return None

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for metric in semantic_manifest.metrics:
            if metric.type == MetricType.SIMPLE:
                # If this is a simple metric with a measure input that does NOT already have some
                # sort of metric input information overriding that measure input
                input_measure = metric.type_params.measure
                if input_measure is None or metric.type_params.metric_aggregation_params is not None:
                    continue

                model_and_measure = (
                    FlattenSimpleMetricsWithMeasureInputsRule._get_semantic_model_and_measure_for_input_measure(
                        input_measure=input_measure,
                        semantic_manifest=semantic_manifest,
                    )
                )
                if model_and_measure is None:
                    # Should be validated; see test_metric_missing_measure for tests that show that this
                    # is the case.
                    logger.warning(
                        f"Measure {input_measure.name} not found in any semantic model; skipping flattening of metric. "
                        "(This should also be caught by validations.)"
                    )
                    continue
                semantic_model, measure = model_and_measure

                metric.type_params.metric_aggregation_params = PydanticMetric.get_metric_aggregation_params(
                    measure=measure,
                    semantic_model_name=semantic_model.name,
                )
                metric.type_params.expr = measure.expr

                # MetricAggregationParamsInForSimpleMetricsRule enforces that fill_nulls_with
                # and join_to_timespine are not allowed if a measure input is present, so this overwrite
                # is safe.
                metric.type_params.fill_nulls_with = input_measure.fill_nulls_with
                metric.type_params.join_to_timespine = True

        return semantic_manifest
