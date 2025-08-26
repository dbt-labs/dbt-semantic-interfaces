from typing import List, Tuple

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
    PydanticCumulativeTypeParams,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricTimeWindow,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
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
    PeriodAggregation,
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.metrics import (
    ConversionMetricRule,
    CumulativeMetricRule,
    DerivedMetricRule,
    MetricTimeGranularityRule,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
    ValidationIssue,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def check_error_in_issues(error_substrings: List[str], issues: Tuple[ValidationIssue, ...]) -> None:
    """Check error substrings in build issues."""
    missing_error_strings = set()
    for expected_str in error_substrings:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        "Failed to match one or more expected issues: "
        + f"{missing_error_strings} in {set([x.as_readable_str() for x in issues])}"
    )


def test_metric_no_time_dim_dim_only_source() -> None:  # noqa:D
    dim_name = "country"
    dim2_name = "ename"
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]()
    model_validator.checked_validations(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[],
                    dimensions=[PydanticDimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
                semantic_model_with_guaranteed_meta(
                    name="sum_measure2",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension=dim2_name,
                        )
                    ],
                    dimensions=[
                        PydanticDimension(name=f"{dim_name}_dup", type=DimensionType.CATEGORICAL),
                        PydanticDimension(
                            name=dim2_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(
                                time_granularity=TimeGranularity.DAY,
                            ),
                        ),
                    ],
                    entities=[PydanticEntity(name="primary_entity2", type=EntityType.PRIMARY)],
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="metric_with_no_time_dim",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                )
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )


def test_metric_no_time_dim() -> None:  # noqa:D
    with pytest.raises(SemanticManifestValidationException):
        dim_name = "country"
        measure_name = "foo"
        model_validator = SemanticManifestValidator[PydanticSemanticManifest]()
        model_validator.checked_validations(
            PydanticSemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="sum_measure",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.CATEGORICAL,
                            )
                        ],
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="metric_with_no_time_dim",
                        type=MetricType.SIMPLE,
                        type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                    )
                ],
                project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
            )
        )


def test_generated_metrics_only() -> None:  # noqa:D
    dim_reference = DimensionReference(element_name="dim")

    dim2_reference = TimeDimensionReference(element_name="ds")
    measure_name = "measure"
    entity_reference = EntityReference(element_name="primary")
    semantic_model = semantic_model_with_guaranteed_meta(
        name="dim1",
        measures=[
            PydanticMeasure(name=measure_name, agg=AggregationType.SUM, agg_time_dimension=dim2_reference.element_name)
        ],
        dimensions=[
            PydanticDimension(name=dim_reference.element_name, type=DimensionType.CATEGORICAL),
            PydanticDimension(
                name=dim2_reference.element_name,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(
                    time_granularity=TimeGranularity.DAY,
                ),
            ),
        ],
        entities=[
            PydanticEntity(name=entity_reference.element_name, type=EntityType.PRIMARY),
        ],
    )
    semantic_model.measures[0].create_metric = True

    SemanticManifestValidator[PydanticSemanticManifest]().checked_validations(
        PydanticSemanticManifest(
            semantic_models=[semantic_model],
            metrics=[],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )


def test_derived_metric() -> None:  # noqa: D
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([DerivedMetricRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension="ds",
                        )
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
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="random_metric",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                ),
                metric_with_guaranteed_meta(
                    name="random_metric2",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name=measure_name)),
                ),
                metric_with_guaranteed_meta(
                    name="alias_collision",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric2 * 2",
                        metrics=[
                            PydanticMetricInput(name="random_metric", alias="random_metric2"),
                            PydanticMetricInput(name="random_metric2"),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="doesntexist",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="notexist * 2", metrics=[PydanticMetricInput(name="notexist")]
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="no_expr",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(metrics=[PydanticMetricInput(name="random_metric")]),
                ),
                metric_with_guaranteed_meta(
                    name="input_metric_not_in_expr",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(expr="x", metrics=[PydanticMetricInput(name="random_metric")]),
                ),
                metric_with_guaranteed_meta(
                    name="no_input_metrics",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(expr="x"),
                ),
                metric_with_guaranteed_meta(
                    name="has_valid_time_window_params",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric / random_metric3",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 weekies"),
                            ),
                            PydanticMetricInput(
                                name="random_metric",
                                offset_to_grain=TimeGranularity.MONTH.value,
                                alias="random_metric3",
                            ),
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_both_time_offset_params_on_same_input_metric",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 weeks"),
                                offset_to_grain=TimeGranularity.MONTH.value,
                            )
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_custom_grain_offset_window",  # this is valid
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_window=PydanticMetricTimeWindow.parse("3 martian_weeks"),
                            )
                        ],
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="has_custom_offset_to_grain",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        expr="random_metric * 2",
                        metrics=[
                            PydanticMetricInput(
                                name="random_metric",
                                offset_to_grain="martian_week",
                            )
                        ],
                    ),
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    build_issues = validation_results.all_issues
    assert len(build_issues) == 8
    expected_substrings = [
        "is already being used. Please choose another alias",
        "does not exist as a configured metric in the model",
        "Both offset_window and offset_to_grain set",
        "is not used in `expr`",
        "No input metrics found for derived metric",
        "No `expr` set for derived metric",
        "Invalid time granularity 'weekies' in window: '3 weekies'",
        "Custom granularities are not supported",
        "Invalid time granularity found in `offset_to_grain`: 'martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)


def test_conversion_metrics() -> None:  # noqa: D
    base_measure_name = "base_measure"
    conversion_measure_name = "conversion_measure"
    entity = "entity"
    invalid_entity = "bad"
    invalid_measure = "invalid_measure"
    window = PydanticMetricTimeWindow.parse("7 days")
    validator = SemanticManifestValidator[PydanticSemanticManifest]([ConversionMetricRule()])
    result = validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="base",
                    measures=[
                        PydanticMeasure(
                            name=base_measure_name, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1"
                        ),
                        PydanticMeasure(name=invalid_measure, agg=AggregationType.MAX, agg_time_dimension="ds"),
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
                        PydanticEntity(name=entity, type=EntityType.PRIMARY),
                    ],
                ),
                semantic_model_with_guaranteed_meta(
                    name="conversion",
                    measures=[
                        PydanticMeasure(
                            name=conversion_measure_name, agg=AggregationType.COUNT, agg_time_dimension="ds", expr="1"
                        )
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
                        PydanticEntity(name=entity, type=EntityType.PRIMARY),
                    ],
                ),
            ],
            metrics=[
                metric_with_guaranteed_meta(
                    name="proper_metric",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(name=conversion_measure_name),
                            window=window,
                            entity=entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="bad_measure_metric",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=invalid_measure),
                            conversion_measure=PydanticMetricInputMeasure(name=conversion_measure_name),
                            window=window,
                            entity=entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="entity_doesnt_exist",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(name=conversion_measure_name),
                            window=window,
                            entity=invalid_entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="constant_property_doesnt_exist",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(name=conversion_measure_name),
                            window=window,
                            entity=entity,
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
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(
                                name=conversion_measure_name,
                                filter=PydanticWhereFilterIntersection(
                                    where_filters=[
                                        PydanticWhereFilter(where_sql_template="""{{ dimension('some_bool') }}""")
                                    ]
                                ),
                            ),
                            window=window,
                            entity=entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="bad_window",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(
                                name=conversion_measure_name,
                            ),
                            window=PydanticMetricTimeWindow.parse("7 moons"),
                            entity=entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(
                                name=conversion_measure_name,
                            ),
                            window=PydanticMetricTimeWindow.parse("7 martian_week"),
                            entity=entity,
                        )
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window_plural",
                    type=MetricType.CONVERSION,
                    type_params=PydanticMetricTypeParams(
                        conversion_type_params=PydanticConversionTypeParams(
                            base_measure=PydanticMetricInputMeasure(name=base_measure_name),
                            conversion_measure=PydanticMetricInputMeasure(
                                name=conversion_measure_name,
                            ),
                            window=PydanticMetricTimeWindow.parse("7 martian_weeks"),
                            entity=entity,
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
        f"{invalid_entity} not found in base semantic model",
        f"{invalid_entity} not found in conversion semantic model",
        "the measure must be COUNT/SUM(1)/COUNT_DISTINCT",
        "The provided constant property: bad_dim, cannot be found",
        "The provided constant property: bad_dim2, cannot be found",
        "filtering on a conversion input measure is not fully supported yet",
        "Invalid time granularity 'moons' in window",
        "Invalid time granularity 'martian_week' in window: '7 martian_weeks'",
        "Invalid time granularity 'martian_week' in window: '7 martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)


def test_cumulative_metrics() -> None:  # noqa: D
    measure_name = "foo"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([CumulativeMetricRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="sum_measure",
                    measures=[
                        PydanticMeasure(
                            name=measure_name,
                            agg=AggregationType.SUM,
                            agg_time_dimension="ds",
                        )
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
                ),
            ],
            metrics=[
                # Metrics with old type params structure - should get warning
                metric_with_guaranteed_meta(
                    name="metric1",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                        cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.LAST),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="metric2",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.MONTH,
                    ),
                ),
                # Metrics with new type params structure - should have no issues
                metric_with_guaranteed_meta(
                    name="big_mama",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                            period_agg=PeriodAggregation.AVERAGE,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="lil_baby",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(grain_to_date=TimeGranularity.MONTH.value),
                    ),
                ),
                # Metric with both window & grain across both type_params - should get warning
                metric_with_guaranteed_meta(
                    name="woooooo",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.MONTH,
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK.value),
                            period_agg=PeriodAggregation.FIRST,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="dis_bad",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=2, granularity=TimeGranularity.QUARTER.value),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.QUARTER.value),
                        ),
                    ),
                ),
                # Metric without window or grain_to_date - should have no issues
                metric_with_guaranteed_meta(
                    name="dis_good",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(period_agg=PeriodAggregation.FIRST),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="bad_window",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 moons"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 martian_week"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_window_plural",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow.parse(window="3 martian_weeks"),
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_grain_to_date",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            grain_to_date="3 martian_weeks",
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="custom_window_old",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow.parse(window="5 martian_week"),
                    ),
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    assert len(build_issues) == 7
    expected_substrings = [
        "Invalid time granularity",
        "Both window and grain_to_date set for cumulative metric. Please set one or the other.",
        "Got differing values for `window`",
        "Invalid time granularity 'martian_week' in window: '3 martian_weeks'",
        "Invalid time granularity 'martian_week' in window: '3 martian_week'",
        "Invalid time granularity 'martian_week' in window: '5 martian_week'",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)


def test_time_granularity() -> None:
    """Test that default grain is validated appropriately."""
    week_measure_name = "foo"
    month_measure_name = "boo"
    week_time_dim_name = "ds__week"
    month_time_dim_name = "ds__month"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([MetricTimeGranularityRule()])
    validation_results = model_validator.validate_semantic_manifest(
        PydanticSemanticManifest(
            semantic_models=[
                semantic_model_with_guaranteed_meta(
                    name="semantic_model",
                    measures=[
                        PydanticMeasure(
                            name=month_measure_name, agg=AggregationType.SUM, agg_time_dimension=month_time_dim_name
                        ),
                        PydanticMeasure(
                            name=week_measure_name, agg=AggregationType.SUM, agg_time_dimension=week_time_dim_name
                        ),
                    ],
                    dimensions=[
                        PydanticDimension(
                            name=month_time_dim_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.MONTH),
                        ),
                        PydanticDimension(
                            name=week_time_dim_name,
                            type=DimensionType.TIME,
                            type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.WEEK),
                        ),
                    ],
                ),
            ],
            metrics=[
                # Simple metrics
                metric_with_guaranteed_meta(
                    name="month_metric_with_no_time_granularity_set",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="week_metric_with_valid_time_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=week_measure_name),
                    ),
                    time_granularity=TimeGranularity.MONTH.value,
                ),
                metric_with_guaranteed_meta(
                    name="month_metric_with_invalid_time_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                    time_granularity=TimeGranularity.WEEK.value,
                ),
                # Derived metrics
                metric_with_guaranteed_meta(
                    name="derived_metric_with_no_time_granularity_set",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                        ],
                        expr="week_metric_with_valid_time_granularity + 1",
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_valid_time_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_time_granularity_set"),
                        ],
                        expr=("week_metric_with_valid_time_granularity + month_metric_with_no_time_granularity_set"),
                    ),
                    time_granularity=TimeGranularity.YEAR.value,
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_invalid_time_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_time_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_time_granularity_set"),
                        ],
                        expr=("week_metric_with_valid_time_granularity + month_metric_with_no_time_granularity_set"),
                    ),
                    time_granularity=TimeGranularity.DAY.value,
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    assert len(build_issues) == 2
    expected_substrings = [
        "`time_granularity` for metric 'month_metric_with_invalid_time_granularity' must be >= MONTH.",
        "`time_granularity` for metric 'derived_metric_with_invalid_time_granularity' must be >= MONTH.",
    ]
    check_error_in_issues(error_substrings=expected_substrings, issues=build_issues)
