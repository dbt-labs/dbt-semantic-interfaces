import logging
from typing import Optional

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.references import TimeDimensionReference
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def _find_primary_time_dimension(semantic_model: PydanticSemanticModel) -> Optional[TimeDimensionReference]:
        for dimension in semantic_model.dimensions:
            if dimension.type == DimensionType.TIME and dimension.type_params and dimension.type_params.is_primary:
                return dimension.time_dimension_reference
        return None

    @staticmethod
    def transform_model(model: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            primary_time_dimension_reference = SetMeasureAggregationTimeDimensionRule._find_primary_time_dimension(
                semantic_model
            )

            if not primary_time_dimension_reference:
                # PydanticDimension semantic models won't have a primary time dimension.
                continue

            for measure in semantic_model.measures:
                if not measure.agg_time_dimension:
                    measure.agg_time_dimension = primary_time_dimension_reference.element_name

        return model
