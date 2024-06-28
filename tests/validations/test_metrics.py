from copy import deepcopy

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
    find_metric_with,
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
    CUMULATIVE_TYPE_PARAMS_SUPPORTED,
    ConversionMetricRule,
    CumulativeMetricRule,
    DefaultGranularityRule,
    DerivedMetricRule,
    WhereFiltersAreParseable,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


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


def test_metric_multiple_primary_time_dims() -> None:  # noqa:D
    with pytest.raises(SemanticManifestValidationException):
        dim_name = "date_created"
        dim2_name = "date_deleted"
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
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                            PydanticDimension(
                                name=dim2_name,
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                        ],
                    )
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name="foo",
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
                                name="random_metric", offset_window=PydanticMetricTimeWindow.parse("3 weeks")
                            ),
                            PydanticMetricInput(
                                name="random_metric", offset_to_grain=TimeGranularity.MONTH, alias="random_metric3"
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
                                offset_to_grain=TimeGranularity.MONTH,
                            )
                        ],
                    ),
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )
    build_issues = validation_results.all_issues
    assert len(build_issues) == 6
    expected_substrings = [
        "is already being used. Please choose another alias",
        "does not exist as a configured metric in the model",
        "Both offset_window and offset_to_grain set",
        "is not used in `expr`",
        "No input metrics found for derived metric",
        "No `expr` set for derived metric",
    ]
    missing_error_strings = set()
    for expected_str in expected_substrings:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        f"Failed to match one or more expected errors: {missing_error_strings} in "
        f"{set([x.as_readable_str() for x in build_issues])}"
    )


def test_where_filter_validations_happy(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    results = validator.validate_semantic_manifest(simple_semantic_manifest__with_primary_transforms)
    assert not results.has_blocking_issues


def test_where_filter_validations_bad_base_filter(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(manifest, lambda metric: metric.filter is not None)
    assert metric.filter is not None
    assert len(metric.filter.where_filters) > 0
    metric.filter.where_filters[0].where_sql_template = "{{ dimension('too', 'many', 'variables', 'to', 'handle') }}"
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(SemanticManifestValidationException, match=f"trying to parse filter of metric `{metric.name}`"):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_measure_filter(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.measure is not None
    )
    assert metric.type_params.measure is not None
    metric.type_params.measure.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException,
        match=f"trying to parse filter of measure input `{metric.type_params.measure.name}` on metric `{metric.name}`",
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_numerator_filter(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.numerator is not None
    )
    assert metric.type_params.numerator is not None
    metric.type_params.numerator.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException, match=f"trying to parse the numerator filter on metric `{metric.name}`"
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_denominator_filter(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest, lambda metric: metric.type_params is not None and metric.type_params.denominator is not None
    )
    assert metric.type_params.denominator is not None
    metric.type_params.denominator.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException, match=f"trying to parse the denominator filter on metric `{metric.name}`"
    ):
        validator.checked_validations(manifest)


def test_where_filter_validations_bad_input_metric_filter(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)

    metric, _ = find_metric_with(
        manifest,
        lambda metric: metric.type_params is not None
        and metric.type_params.metrics is not None
        and len(metric.type_params.metrics) > 0,
    )
    assert metric.type_params.metrics is not None
    input_metric = metric.type_params.metrics[0]
    input_metric.filter = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template="{{ dimension('too', 'many', 'variables', 'to', 'handle') }}")
        ]
    )
    validator = SemanticManifestValidator[PydanticSemanticManifest]([WhereFiltersAreParseable()])
    with pytest.raises(
        SemanticManifestValidationException,
        match=f"trying to parse filter for input metric `{input_metric.name}` on metric `{metric.name}`",
    ):
        validator.checked_validations(manifest)


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
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = result.errors
    assert len(build_issues) == 5
    expected_substr1 = f"{invalid_entity} not found in base semantic model"
    expected_substr2 = f"{invalid_entity} not found in conversion semantic model"
    expected_substr3 = "the measure must be COUNT/SUM(1)/COUNT_DISTINCT"
    expected_substr4 = "The provided constant property: bad_dim, cannot be found"
    expected_substr5 = "The provided constant property: bad_dim2, cannot be found"
    missing_error_strings = set()
    for expected_str in [expected_substr1, expected_substr2, expected_substr3, expected_substr4, expected_substr5]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, "Failed to match one or more expected errors: "
    f"{missing_error_strings} in {set([x.as_readable_str() for x in build_issues])}"


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
                # Metrics with old type params structure - should 2 get warnings
                metric_with_guaranteed_meta(
                    name="metric1",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK),
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
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK),
                            period_agg=PeriodAggregation.AVERAGE,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="lil_baby",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        cumulative_type_params=PydanticCumulativeTypeParams(grain_to_date=TimeGranularity.MONTH),
                    ),
                ),
                # Metric with both window & grain across both type_params - should get 2 warnings
                metric_with_guaranteed_meta(
                    name="woooooo",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.MONTH,
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.WEEK),
                            period_agg=PeriodAggregation.FIRST,
                        ),
                    ),
                ),
                # Metrics with duplicated window or grain_to_date - should 4 get warnings
                metric_with_guaranteed_meta(
                    name="what_a_metric",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        grain_to_date=TimeGranularity.YEAR,
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            grain_to_date=TimeGranularity.HOUR,
                        ),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="dis_bad",
                    type=MetricType.CUMULATIVE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=measure_name),
                        window=PydanticMetricTimeWindow(count=2, granularity=TimeGranularity.QUARTER),
                        cumulative_type_params=PydanticCumulativeTypeParams(
                            window=PydanticMetricTimeWindow(count=1, granularity=TimeGranularity.QUARTER),
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
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    if CUMULATIVE_TYPE_PARAMS_SUPPORTED:
        assert len(build_issues) == 8
        expected_substr1 = "Both window and grain_to_date set for cumulative metric. Please set one or the other."
        expected_substr2 = "Got differing values for `window`"
        expected_substr3 = "Got differing values for `grain_to_date`"
        expected_substr4 = "Cumulative `type_params.window` field has been moved and will soon be deprecated."
        expected_substr5 = "Cumulative `type_params.grain_to_date` field has been moved and will soon be deprecated."
        missing_error_strings = set()
        for expected_str in [expected_substr1, expected_substr2, expected_substr3, expected_substr4, expected_substr5]:
            if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
                missing_error_strings.add(expected_str)
        assert len(missing_error_strings) == 0, "Failed to match one or more expected issues: "
        f"{missing_error_strings} in {set([x.as_readable_str() for x in build_issues])}"
    else:
        assert len(build_issues) == 1
        expected_substr1 = "Both window and grain_to_date set for cumulative metric. Please set one or the other."
        missing_error_strings = set()
        for expected_str in [expected_substr1]:
            if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
                missing_error_strings.add(expected_str)
        assert len(missing_error_strings) == 0, "Failed to match one or more expected issues: "
        f"{missing_error_strings} in {set([x.as_readable_str() for x in build_issues])}"


def test_default_granularity() -> None:
    """Test that default grain is validated appropriately."""
    week_measure_name = "foo"
    month_measure_name = "boo"
    week_time_dim_name = "ds__week"
    month_time_dim_name = "ds__month"
    model_validator = SemanticManifestValidator[PydanticSemanticManifest]([DefaultGranularityRule()])
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
                    name="month_metric_with_no_default_granularity_set",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="week_metric_with_valid_default_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=week_measure_name),
                    ),
                    default_granularity=TimeGranularity.MONTH,
                ),
                metric_with_guaranteed_meta(
                    name="month_metric_with_invalid_default_granularity",
                    type=MetricType.SIMPLE,
                    type_params=PydanticMetricTypeParams(
                        measure=PydanticMetricInputMeasure(name=month_measure_name),
                    ),
                    default_granularity=TimeGranularity.WEEK,
                ),
                # Derived metrics
                metric_with_guaranteed_meta(
                    name="derived_metric_with_no_default_granularity_set",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_default_granularity"),
                        ],
                        expr="week_metric_with_valid_default_granularity + 1",
                    ),
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_valid_default_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_default_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_default_granularity_set"),
                        ],
                        expr=(
                            "week_metric_with_valid_default_granularity + month_metric_with_no_default_granularity_set"
                        ),
                    ),
                    default_granularity=TimeGranularity.YEAR,
                ),
                metric_with_guaranteed_meta(
                    name="derived_metric_with_invalid_default_granularity",
                    type=MetricType.DERIVED,
                    type_params=PydanticMetricTypeParams(
                        metrics=[
                            PydanticMetricInput(name="week_metric_with_valid_default_granularity"),
                            PydanticMetricInput(name="month_metric_with_no_default_granularity_set"),
                        ],
                        expr=(
                            "week_metric_with_valid_default_granularity + month_metric_with_no_default_granularity_set"
                        ),
                    ),
                    default_granularity=TimeGranularity.DAY,
                ),
            ],
            project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
        )
    )

    build_issues = validation_results.all_issues
    assert len(build_issues) == 2
    expected_substr1 = (
        "`default_granularity` for metric 'month_metric_with_invalid_default_granularity' must be >= MONTH."
    )
    expected_substr2 = (
        "`default_granularity` for metric 'derived_metric_with_invalid_default_granularity' must be >= MONTH."
    )
    missing_error_strings = set()
    for expected_str in [expected_substr1, expected_substr2]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, "Failed to match one or more expected issues: "
    f"{missing_error_strings} in {set([x.as_readable_str() for x in build_issues])}"
