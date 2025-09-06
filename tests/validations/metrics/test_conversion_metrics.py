from typing import List, Optional

import pytest

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticConstantPropertyInput,
    PydanticConversionTypeParams,
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.metrics import ConversionMetricRule
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION
from tests.validations.validation_test_utils import check_error_in_issues

BASE_MEASURE_NAME = "base_measure"
CONVERSION_MEASURE_NAME = "conversion_measure"
ENTITY_NAME = "entity"
INVALID_ENTITY_NAME = "bad_entity"
INVALID_MEASURE_NAME = "invalid_measure"
DEFAULT_WINDOW = PydanticMetricTimeWindow.parse("7 days")

VALIDATOR = SemanticManifestValidator[PydanticSemanticManifest]([ConversionMetricRule()])
SEMANTIC_MODELS = [
    semantic_model_with_guaranteed_meta(
        name="base",
        measures=[
            PydanticMeasure(name=BASE_MEASURE_NAME, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1"),
            PydanticMeasure(name=INVALID_MEASURE_NAME, agg=AggregationType.MAX, agg_time_dimension="ds"),
        ],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        ],
        entities=[
            PydanticEntity(name=ENTITY_NAME, type=EntityType.PRIMARY),
        ],
    ),
    semantic_model_with_guaranteed_meta(
        name="conversion",
        measures=[
            PydanticMeasure(name=CONVERSION_MEASURE_NAME, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1")
        ],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        ],
        entities=[
            PydanticEntity(name=ENTITY_NAME, type=EntityType.PRIMARY),
        ],
    ),
]


@pytest.mark.parametrize(
    "metric, error_substrings_if_errors, warning_substrings_if_warnings",
    [
        (
            metric_with_guaranteed_meta(
                name="proper_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # No error; this should pass
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_measure_metric",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=INVALID_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "For conversion metrics, the measure must be COUNT/SUM(1)/COUNT_DISTINCT. "
                f"Measure: {INVALID_MEASURE_NAME} is agg type: AggregationType.MAX",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="entity_doesnt_exist",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=INVALID_ENTITY_NAME,
                    )
                ),
            ),
            [
                f"Entity: {INVALID_ENTITY_NAME} not found in base semantic model: base",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="constant_property_doesnt_exist",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                        constant_properties=[
                            PydanticConstantPropertyInput(base_property="bad_dim", conversion_property="bad_dim2")
                        ],
                    )
                ),
            ),
            [
                "The provided constant property: bad_dim, cannot be found in semantic model base",
                "The provided constant property: bad_dim2, cannot be found in semantic model conversion",
            ],
            None,  # No warning; this should pass
        ),
        (
            metric_with_guaranteed_meta(
                name="filter_on_conversion_measure",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                            filter=PydanticWhereFilterIntersection(
                                where_filters=[
                                    PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}""")
                                ]
                            ),
                        ),
                        window=DEFAULT_WINDOW,
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            None,  # This only fires a warning, not an error.
            [
                f"Measure input {CONVERSION_MEASURE_NAME} has a filter. For conversion metrics, "
                "filtering on a conversion input measure is not fully supported yet.",
            ],
        ),
        (
            metric_with_guaranteed_meta(
                name="bad_window",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 moons"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'moons' in window: '7 moons'",
            ],
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="custom_grain_window",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 martian_week"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'martian_week' in window: '7 martian_week' "
                "Custom granularities are not supported for this field yet.",
            ],
            None,
        ),
        (
            metric_with_guaranteed_meta(
                name="custom_grain_window_plural_grain_name",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(
                    conversion_type_params=PydanticConversionTypeParams(
                        base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                        conversion_measure=PydanticMetricInputMeasure(
                            name=CONVERSION_MEASURE_NAME,
                        ),
                        window=PydanticMetricTimeWindow.parse("7 martian_weeks"),
                        entity=ENTITY_NAME,
                    )
                ),
            ),
            [
                "Invalid time granularity 'martian_week' in window: '7 martian_weeks' "
                "Custom granularities are not supported for this field yet."
            ],
            None,
        ),
    ],
)
def test_conversion_metrics(  # noqa: D
    metric: PydanticMetric,
    error_substrings_if_errors: Optional[List[str]],
    warning_substrings_if_warnings: Optional[List[str]],
) -> None:
    validation_results = VALIDATOR.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=SEMANTIC_MODELS,
            metrics=[
                metric,
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    if error_substrings_if_errors:
        check_error_in_issues(error_substrings=error_substrings_if_errors, issues=validation_results.errors)
    if warning_substrings_if_warnings:
        check_error_in_issues(error_substrings=warning_substrings_if_warnings, issues=validation_results.warnings)
    if not error_substrings_if_errors and not warning_substrings_if_warnings:
        assert len(validation_results.all_issues) == 0, "expected this metric to pass validation, but it did not"
