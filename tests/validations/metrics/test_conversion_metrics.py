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
INVALID_ENTITY_NAME = "bad"
INVALID_MEASURE_NAME = "invalid_measure"

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


def test_conversion_metrics() -> None:  # noqa: D
    window = PydanticMetricTimeWindow.parse("7 days")
    validator = SemanticManifestValidator[PydanticSemanticManifest]([ConversionMetricRule()])
    result = validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=SEMANTIC_MODELS,
            metrics=[
                metric_with_guaranteed_meta(
                    name="proper_metric",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                            conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                            window=window,
                            entity=ENTITY_NAME,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="bad_measure_metric",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=INVALID_MEASURE_NAME),
                            conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                            window=window,
                            entity=ENTITY_NAME,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="entity_doesnt_exist",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                            conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                            window=window,
                            entity=INVALID_ENTITY_NAME,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="constant_property_doesnt_exist",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=BASE_MEASURE_NAME),
                            conversion_measure=PydanticMetricInputMeasure(name=CONVERSION_MEASURE_NAME),
                            window=window,
                            entity=ENTITY_NAME,
                            constant_properties=[
                                PydanticConstantPropertyInput(base_property="bad_dim", conversion_property="bad_dim2")
                            ],
                        )
                    ),
                ),
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
                            window=window,
                            entity=ENTITY_NAME,
                        )
                    ),
                ),
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
                metric_with_guaranteed_meta(
                    name="custom_grain_window_plural",
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
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = result.all_issues
    assert len(result.errors) == 8
    assert len(result.warnings) == 1

    expected_substrings = [
        f"{INVALID_ENTITY_NAME} not found in base semantic model",
        f"{INVALID_ENTITY_NAME} not found in conversion semantic model",
        "the measure must be COUNT/SUM(1)/COUNT_DISTINCT",
        "The provided constant property: bad_dim, cannot be found",
        "The provided constant property: bad_dim2, cannot be found",
        "filtering on a conversion input measure is not fully supported yet",
        "Invalid time granularity 'moons' in window",
        "Invalid time granularity 'martian_week' in window: '7 martian_weeks'",
        "Invalid time granularity 'martian_week' in window: '7 martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)
