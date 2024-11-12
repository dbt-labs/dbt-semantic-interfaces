from typing import Sequence

from typing_extensions import override

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.metric import PydanticMetricTimeWindow
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)
from dbt_semantic_interfaces.type_enums import MetricType


class RemovePluralFromWindowGranularityRule(ProtocolHint[SemanticManifestTransformRule[PydanticSemanticManifest]]):
    """Remove trailing s from granularity in MetricTimeWindow.

    During parsing, MetricTimeWindow.granularity can still contain he trailing 's' (ie., 3 days).
    This is because with the introduction of custom granularities, we don't have access to the valid
    custom grains during parsing. This transformation rule is introduced to remove the trailing 's'
    from `MetricTimeWindow.granularity` if necessary.
    """

    @override
    def _implements_protocol(self) -> SemanticManifestTransformRule[PydanticSemanticManifest]:  # noqa: D
        return self

    @staticmethod
    def _update_metric(
        semantic_manifest: PydanticSemanticManifest, metric_name: str, custom_granularity_names: Sequence[str]
    ) -> None:
        """Mutates all the MetricTimeWindow by reparsing to remove the trailing 's'."""

        def reparse_window(window: PydanticMetricTimeWindow) -> PydanticMetricTimeWindow:
            """Reparse the window to remove the trailing 's'."""
            return PydanticMetricTimeWindow.parse(
                window=window.window_string, custom_granularity_names=custom_granularity_names
            )

        matched_metric = next(
            iter((metric for metric in semantic_manifest.metrics if metric.name == metric_name)), None
        )
        if matched_metric:
            if matched_metric.type is MetricType.CUMULATIVE:
                if (
                    matched_metric.type_params.cumulative_type_params
                    and matched_metric.type_params.cumulative_type_params.window
                ):
                    matched_metric.type_params.cumulative_type_params.window = reparse_window(
                        matched_metric.type_params.cumulative_type_params.window
                    )
            elif matched_metric.type is MetricType.CONVERSION:
                if (
                    matched_metric.type_params.conversion_type_params
                    and matched_metric.type_params.conversion_type_params.window
                ):
                    matched_metric.type_params.conversion_type_params.window = reparse_window(
                        matched_metric.type_params.conversion_type_params.window
                    )

            elif matched_metric.type is MetricType.DERIVED or matched_metric.type is MetricType.RATIO:
                for input_metric in matched_metric.input_metrics:
                    if input_metric.offset_window:
                        input_metric.offset_window = reparse_window(input_metric.offset_window)
            elif matched_metric.type is MetricType.SIMPLE:
                pass
            else:
                assert_values_exhausted(matched_metric.type)
        else:
            raise ModelTransformError(f"Metric '{metric_name}' is not configured as a metric in the model.")

    @staticmethod
    def transform_model(semantic_manifest: PydanticSemanticManifest) -> PydanticSemanticManifest:  # noqa: D
        custom_granularity_names = [
            granularity.name
            for time_spine in semantic_manifest.project_configuration.time_spines
            for granularity in time_spine.custom_granularities
        ]

        for metric in semantic_manifest.metrics:
            RemovePluralFromWindowGranularityRule._update_metric(
                semantic_manifest=semantic_manifest,
                metric_name=metric.name,
                custom_granularity_names=custom_granularity_names,
            )
        return semantic_manifest
