from typing import Set

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.references import (
    DimensionReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class SetDefaultGrainRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
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
            seen_agg_time_dimensions: Set[TimeDimensionReference] = set()
            for semantic_model in semantic_manifest.semantic_models:
                for measure_ref in set(metric.measure_references).intersection(semantic_model.measure_references):
                    agg_time_dimension_ref = semantic_model.checked_agg_time_dimension_for_measure(measure_ref)
                    if agg_time_dimension_ref in seen_agg_time_dimensions:
                        continue
                    seen_agg_time_dimensions.add(agg_time_dimension_ref)
                    dimension = semantic_model.get_dimension(DimensionReference(agg_time_dimension_ref.element_name))
                    if (
                        dimension.type_params
                        and dimension.type_params.time_granularity.to_int() > default_granularity.to_int()
                    ):
                        default_granularity = dimension.type_params.time_granularity

        return semantic_manifest
