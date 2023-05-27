import logging

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class SetMeasureAggregationTimeDimensionRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Sets the aggregation time dimension for measures to the one specified as default."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(model: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for semantic_model in model.semantic_models:
            if semantic_model.defaults is None:
                continue

            if semantic_model.defaults.agg_time_dimension is None:
                continue

            for measure in semantic_model.measures:
                if not measure.agg_time_dimension:
                    measure.agg_time_dimension = semantic_model.defaults.agg_time_dimension

        return model
