import logging
from typing import Dict, Optional, Set, Tuple

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
    def _maybe_get_or_create_metric_and_retrieve_name(
        mapper: MeasureFeaturesToMetricNameMapper,
        input_measure: Optional[PydanticMetricInputMeasure],
        input_metric: Optional[PydanticMetricInput],
        semantic_manifest: PydanticSemanticManifest,
        existing_metric_names: Set[str],
        measure_name_to_model_and_measure_map: Dict[str, Tuple[PydanticSemanticModel, PydanticMeasure]],
    ) -> Optional[str]:
        if input_measure is None or input_metric is not None:
            return None
        model_and_measure = measure_name_to_model_and_measure_map.get(
            input_measure.name,
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
            existing_metric_names=existing_metric_names,
        )

    @staticmethod
    def _maybe_handle_cumulative_metric(
        metric: PydanticMetric,
        semantic_manifest: PydanticSemanticManifest,
        mapper: MeasureFeaturesToMetricNameMapper,
        existing_metric_names: Set[str],
        measure_name_to_model_and_measure_map: Dict[str, Tuple[PydanticSemanticModel, PydanticMeasure]],
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
                existing_metric_names=existing_metric_names,
                measure_name_to_model_and_measure_map=measure_name_to_model_and_measure_map,
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
        existing_metric_names: Set[str],
        measure_name_to_model_and_measure_map: Dict[str, Tuple[PydanticSemanticModel, PydanticMeasure]],
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
                existing_metric_names=existing_metric_names,
                measure_name_to_model_and_measure_map=measure_name_to_model_and_measure_map,
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
                existing_metric_names=existing_metric_names,
                measure_name_to_model_and_measure_map=measure_name_to_model_and_measure_map,
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
        existing_metric_names = set([metric.name for metric in semantic_manifest.metrics])
        measure_name_to_model_and_measure_map = semantic_manifest.build_measure_name_to_model_and_measure_map()

        for metric in semantic_manifest.metrics:
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_handle_cumulative_metric(
                metric,
                semantic_manifest,
                mapper,
                existing_metric_names,
                measure_name_to_model_and_measure_map,
            )
            ReplaceInputMeasuresWithSimpleMetricsTransformationRule._maybe_handle_conversion_metric(
                metric,
                semantic_manifest,
                mapper,
                existing_metric_names,
                measure_name_to_model_and_measure_map,
            )

        return semantic_manifest
