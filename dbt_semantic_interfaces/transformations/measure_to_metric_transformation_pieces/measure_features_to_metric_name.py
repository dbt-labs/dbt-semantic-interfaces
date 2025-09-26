from typing import Dict, Optional, Tuple

from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.type_enums import MetricType


class MeasureFeaturesToMetricNameMapper:
    """Maps measure configurations to metric names, and helps add new metrics to the manifest."""

    # Until we're at minimum Python version 3.12, we can't use "type" statements, so
    # we use this for backward compatibility.
    _MetricNameKey = Tuple[str, Optional[int], bool]
    _metric_name_dict: Dict[_MetricNameKey, str]

    def __init__(self):  # noqa: D
        self._metric_name_dict = {}

    def _get_stored_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
    ) -> Optional[str]:
        """Get the name of the metric that is stored for the tuple(measure, <settings>).

        where settings is a set of features that are moved from a measure_input
        object to the metric object.  It contains:
        - fill_nulls_with
        - join_to_timespine

        returns the name of the metric that is stored for this measure configuration, or
                None if no metric is stored for the measure configuration
        """
        key = (measure_name, fill_nulls_with, join_to_timespine)
        return self._metric_name_dict.get(key)

    def _store_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
        metric_name: str,
    ) -> None:
        """Store the name of the metric that is stored for the tuple(measure, <settings>)."""
        key = (measure_name, fill_nulls_with, join_to_timespine)
        self._metric_name_dict[key] = metric_name

    def _find_metric_clone_in_manifest(
        self,
        metric: PydanticMetric,
        manifest: PydanticSemanticManifest,
    ) -> Optional[PydanticMetric]:
        """Check if a metric exists in the manifest that matches the metric (except for name).

        returns the metric if it exists, otherwise None

        Note: this can be further optimized by pre-caching metrics based on features,
        but let's not prematurely optimize.
        """
        search_metric = metric.copy(deep=True)
        for existing_metric in manifest.metrics:
            # this allows us to a straight equality comparison, which is safer in the future
            # than implementing a custom comparison function.
            search_metric.name = existing_metric.name
            search_metric.metadata = existing_metric.metadata
            search_metric.type_params.is_private = existing_metric.type_params.is_private
            if search_metric == existing_metric:
                return existing_metric
            print("provided metric", search_metric)
            print("existing metric", existing_metric)
        return None

    @staticmethod
    def build_metric_from_measure_configuration(
        measure: PydanticMeasure,
        semantic_model_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: Optional[bool],
        is_private: bool = True,
    ) -> PydanticMetric:
        """Build a metric from the measure configuration.

        Name defaults to the measure name, which will require overriding in many cases
        (Name override is handled automatically if you are using
        get_or_create_metric_for_measure instead of this method).
        """
        type_params = PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetric.build_metric_aggregation_params(
                measure=measure,
                semantic_model_name=semantic_model_name,
            ),
            expr=measure.expr,
            is_private=is_private,
        )
        # This allows us to avoid re-implementing the defaults in a second place.
        if fill_nulls_with is not None:
            type_params.fill_nulls_with = fill_nulls_with
        if join_to_timespine is not None:
            type_params.join_to_timespine = join_to_timespine

        return PydanticMetric(
            name=measure.name,
            type=MetricType.SIMPLE,
            type_params=type_params,
            description=measure.description,
            label=measure.label,
            config=measure.config,
            metadata=measure.metadata,
        )

    def _generate_new_metric_name(
        self,
        measure_name: str,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
        manifest: PydanticSemanticManifest,
    ) -> str:
        """Generate a new metric name for the measure configuration."""
        # we could cache the names, too, but I'm not sure it's worth the trouble
        # of keeping the names-list up to date.
        existing_names = set([metric.name for metric in manifest.metrics])

        name_parts = [measure_name]
        if fill_nulls_with is not None:
            name_parts.append(f"fill_nulls_with_{fill_nulls_with}")
        if join_to_timespine:
            name_parts.append("join_to_timespine_true")

        base_name = "_".join(name_parts)
        new_name = base_name
        count = 1
        while new_name in existing_names:
            # one hopes people are not naming their metrics like this, but we'll just assume
            # someone has and avoid collisions.
            new_name = f"{base_name}_{count}"
            count += 1

        return new_name

    def get_or_create_metric_for_measure(
        self,
        manifest: PydanticSemanticManifest,
        model_name: str,
        measure: PydanticMeasure,
        fill_nulls_with: Optional[int],
        join_to_timespine: bool,
    ) -> str:
        """Find the existing metric for a measure configuration, or create it if it doesn't exist.

        returns the name of the metric
        """
        # Check: do we already have this in the dict?  Let's skip searching for it then!
        stored_metric_name = self._get_stored_metric_name(
            measure_name=measure.name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
        )
        if stored_metric_name is not None:
            return stored_metric_name

        # if no, does a metric exist in the manifest that matches all required features?
        built_metric = self.build_metric_from_measure_configuration(
            measure=measure,
            semantic_model_name=model_name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
        )
        metric = self._find_metric_clone_in_manifest(
            metric=built_metric,
            manifest=manifest,
        )

        if metric is None:
            # if we didn't find it, let's make a new name and add it to the manifest
            metric_name = self._generate_new_metric_name(
                measure_name=measure.name,
                fill_nulls_with=fill_nulls_with,
                join_to_timespine=join_to_timespine,
                manifest=manifest,
            )
            metric = built_metric
            metric.name = metric_name
            manifest.metrics.append(metric)

        self._store_metric_name(
            measure_name=measure.name,
            fill_nulls_with=fill_nulls_with,
            join_to_timespine=join_to_timespine,
            metric_name=metric.name,
        )
        return metric.name
