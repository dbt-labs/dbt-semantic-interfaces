import logging

from typing_extensions import override

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType

logger = logging.getLogger(__name__)


class FixProxyMetricsRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Fixes proxy metrics that were created from the create_metric flag set.

    This is a temporary fix for a bug in dbt core where proxy metrics have the expr always set
    to the measure name instead of the measure expr if it is provided. This may continue to exist
    in the future in legacy manifests, so this may need to exist until all those are cleaned up.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Fixes proxy metrics that were created from the create_metric flag set."""
        for semantic_model in semantic_manifest.semantic_models:
            measures_in_model = {measure.name: measure for measure in semantic_model.measures}
            for metric in semantic_manifest.metrics:
                proxied_measure = measures_in_model.get(metric.name)
                # If the metric is a simple metric and has the same name as a measure,
                # then it is likely a metric created from the create_metric flag set.
                if metric.type == MetricType.SIMPLE and proxied_measure is not None:
                    if metric.type_params.expr == proxied_measure.name:
                        # The faulty expr occurs when the expr is always set to the measure name.
                        # We are effectively overriding this to ensure the expr is instantiated
                        # in the correct order where the measure expr is first if it is provided.
                        metric.type_params.expr = proxied_measure.expr or proxied_measure.name
                        # Adding logging to help track down the issue in the future.
                        logger.info("Potential error with proxy metric expr, overriding.")
        return semantic_manifest
