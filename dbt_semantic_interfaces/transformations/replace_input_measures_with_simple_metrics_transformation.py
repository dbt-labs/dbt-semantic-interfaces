import logging
from typing import Optional, Tuple

from typing_extensions import override

from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.measure_to_metric_transformation_pieces.measure_features_to_metric_name import (  # noqa: E501
    MeasureFeaturesToMetricNameMapper,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class ReplaceInputMeasuresWithSimpleMetricsTransformationRule(
    ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]
):
    """Replaces measure inputs on cumulative and conversion metrics with metric inputs.

    These metric inputs are simple metrics that perfectly match the referenced measure;
    if there is no such metric, one will be created.

    - For cumulative metrics: replace `type_params.measure` with
      `type_params.cumulative_type_params.metric` pointing to a simple metric.
    - For conversion metrics: replace `base_measure`/`conversion_measure` with
      `base_metric`/`conversion_metric` respectively.

    The simple metrics are looked up (or created) using MeasureFeaturesToMetricNameMapper
    based on the referenced measure and its join_to_timespine/fill_nulls_with settings.
    """

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
    def _maybe_get_or_create_metric_and_retrieve_name(
        mapper: MeasureFeaturesToMetricNameMapper,
        input_measure: Optional[PydanticMetricInputMeasure],
        input_metric: Optional[PydanticMetricInput],
        semantic_manifest: PydanticSemanticManifest,
    ) -> Optional[str]:
        if input_measure is None or input_metric is not None:
            return None
        model_and_measure = (
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._get_semantic_model_and_measure_for_input_measure(
                input_measure=input_measure,
                semantic_manifest=semantic_manifest,
            )
        )
        if model_and_measure is None:
            logger.warning(
                (
                    f"Measure {input_measure.name} not found in any semantic model; "
                    "skipping replacement on cumulative metric. "
                    "(This should also be caught by validations.)"
                )
            )
            return None
        semantic_model, measure = model_and_measure
        return mapper.get_or_create_metric_for_measure(
            manifest=semantic_manifest,
            model_name=semantic_model.name,
            measure=measure,
            fill_nulls_with=input_measure.fill_nulls_with,
            join_to_timespine=input_measure.join_to_timespine,
        )

    @staticmethod
    def _maybe_handle_cumulative_metric(
        metric: PydanticMetric,
        semantic_manifest: PydanticSemanticManifest,
        mapper: MeasureFeaturesToMetricNameMapper,
    ) -> None:
        if metric.type != MetricType.CUMULATIVE:
            return
        if metric.type_params.measure is None:
            return
        if metric.type_params.cumulative_type_params is None:
            logger.warning(
                (
                    f"Cumulative metric {metric.name} has no cumulative type params; "
                    "skipping replacement on cumulative metric. "
                    "(This should also be caught by validations.)"
                )
            )
            return
        new_metric_name = (
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_get_or_create_metric_and_retrieve_name(
                mapper=mapper,
                input_measure=metric.type_params.measure,
                input_metric=metric.type_params.cumulative_type_params.metric,
                semantic_manifest=semantic_manifest,
            )
        )
        if new_metric_name is not None:
            metric.type_params.cumulative_type_params.metric = PydanticMetricInput(
                name=new_metric_name,
                filter=metric.type_params.measure.filter,
                alias=metric.type_params.measure.alias,
            )
            # Note: we leave the old measure reference in place for backward compatibility.

    @staticmethod
    def _maybe_handle_conversion_metric(
        metric: PydanticMetric,
        semantic_manifest: PydanticSemanticManifest,
        mapper: MeasureFeaturesToMetricNameMapper,
    ) -> None:
        if metric.type != MetricType.CONVERSION:
            return
        if metric.type_params.conversion_type_params is None:
            logger.warning(
                (
                    f"Conversion metric {metric.name} has no conversion type params; "
                    "skipping replacement on conversion metric. "
                    "(This should also be caught by validations.)"
                )
            )
            return
        conversion_type_params = metric.type_params.conversion_type_params
        new_conversion_metric_base_metric_name = (
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_get_or_create_metric_and_retrieve_name(
                mapper=mapper,
                input_measure=conversion_type_params.base_measure,
                input_metric=conversion_type_params.base_metric,
                semantic_manifest=semantic_manifest,
            )
        )
        if new_conversion_metric_base_metric_name is not None:
            metric.type_params.conversion_type_params.base_metric = PydanticMetricInput(
                name=new_conversion_metric_base_metric_name,
                filter=conversion_type_params.base_measure.filter
                if conversion_type_params.base_measure is not None
                else None,
                alias=conversion_type_params.base_measure.alias
                if conversion_type_params.base_measure is not None
                else None,
            )
            # Note: we leave the old measure reference in place for backward compatibility.

        new_conversion_metric_conversion_metric_name = (
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_get_or_create_metric_and_retrieve_name(
                mapper=mapper,
                input_measure=conversion_type_params.conversion_measure,
                input_metric=conversion_type_params.conversion_metric,
                semantic_manifest=semantic_manifest,
            )
        )
        if new_conversion_metric_conversion_metric_name is not None:
            metric.type_params.conversion_type_params.conversion_metric = PydanticMetricInput(
                name=new_conversion_metric_conversion_metric_name,
                filter=conversion_type_params.conversion_measure.filter
                if conversion_type_params.conversion_measure is not None
                else None,
                alias=conversion_type_params.conversion_measure.alias
                if conversion_type_params.conversion_measure is not None
                else None,
            )
            # Note: we leave the old measure reference in place for backward compatibility.

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        mapper = MeasureFeaturesToMetricNameMapper()

        for metric in semantic_manifest.metrics:
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_handle_cumulative_metric(
                metric, semantic_manifest, mapper
            )
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_handle_conversion_metric(
                metric, semantic_manifest, mapper
            )

        return semantic_manifest
