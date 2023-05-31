from typing import Set

from typing_extensions import override

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.metric import (
    MetricType,
    PydanticMetricInputMeasure,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)


class AddInputMetricMeasuresRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def _get_measures_for_metric(
        semantic_manifest: PydanticSemanticManifest, metric_name: str
    ) -> Set[PydanticMetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures = set()
        matched_metric = next(
            iter((metric for metric in semantic_manifest.metrics if metric.name == metric_name)), None
        )
        if matched_metric:
            if matched_metric.type == MetricType.DERIVED:
                for input_metric in matched_metric.input_metrics:
                    measures.update(
                        AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, input_metric.name)
                    )
            else:
                measures.update(set(matched_metric.input_measures))
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        for metric in semantic_manifest.metrics:
            if metric.type == MetricType.DERIVED:
                measures = AddInputMetricMeasuresRule._get_measures_for_metric(semantic_manifest, metric.name)
                assert (
                    metric.type_params.measures is None
                ), "Derived metric should have no measures predefined in the config"
                metric.type_params.measures = list(measures)
        return semantic_manifest
