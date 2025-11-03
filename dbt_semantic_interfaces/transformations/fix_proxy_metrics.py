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
    """Fixes proxy metrics that were created from the create_metric flag set."""

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
                if metric.type == MetricType.SIMPLE and proxied_measure is not None:
                    # This is likely a proxy metric that was created from the create_metric flag set.
                    if metric.type_params.expr == metric.name:
                        # The faulty expr is when the expr is set to the measure name, which in this case
                        # when being created from create_metric, the metric name would be the same as the measure name.
                        metric.type_params.expr = proxied_measure.expr or metric.name
        return semantic_manifest
