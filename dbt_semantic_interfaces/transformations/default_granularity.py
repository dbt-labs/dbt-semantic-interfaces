import logging
from typing import Dict, Set, Tuple

from typing_extensions import override

from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.metric import Metric
from dbt_semantic_interfaces.references import (
    DimensionReference,
    MetricReference,
    SemanticModelReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity

logger = logging.getLogger(__name__)


class SetDefaultGranularityRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """If default_granularity is not set for a metric, set it to DAY if available, else the smallest available grain."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """For each metric, set default_granularity to DAY or smallest granularity supported by all agg_time_dims."""
        for metric in semantic_manifest.metrics:
            if metric.default_granularity:
                continue

            default_granularity = TimeGranularity.DAY
            seen_agg_time_dimensions: Set[Tuple[SemanticModelReference, TimeDimensionReference]] = set()

            metric_index: Dict[MetricReference, Metric] = {
                MetricReference(metric.name): metric for metric in semantic_manifest.metrics
            }

            for semantic_model in semantic_manifest.semantic_models:
                for measure_ref in set(
                    PydanticMetric.all_input_measures_for_metric(metric=metric, metric_index=metric_index)
                ).intersection(semantic_model.measure_references):
                    try:
                        agg_time_dimension_ref = semantic_model.checked_agg_time_dimension_for_measure(measure_ref)
                    except AssertionError:
                        # This indicates the agg_time_dimension is misconfigured, which will fail elsewhere.
                        # Do nothing here to avoid disrupting the validation process.
                        logger.warning(
                            f"Agg time dimension '{agg_time_dimension_ref.element_name}' not found in model."
                            "This should raise a validation error elsewhere."
                        )
                        continue
                    # Time dims might have the same names across semantic models, so check model/name combo.
                    semantic_model_with_agg_time_dimension = (semantic_model.reference, agg_time_dimension_ref)
                    if semantic_model_with_agg_time_dimension in seen_agg_time_dimensions:
                        continue
                    seen_agg_time_dimensions.add(semantic_model_with_agg_time_dimension)
                    dimension = semantic_model.get_dimension(DimensionReference(agg_time_dimension_ref.element_name))
                    if (
                        dimension.type_params
                        and dimension.type_params.time_granularity.to_int() > default_granularity.to_int()
                    ):
                        default_granularity = dimension.type_params.time_granularity

            metric.default_granularity = default_granularity

        return semantic_manifest
