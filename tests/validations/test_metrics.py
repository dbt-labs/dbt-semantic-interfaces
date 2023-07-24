import pytest

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
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
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.metrics import DerivedMetricRule
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
                        PydanticDimension(name=dim_name, type=DimensionType.CATEGORICAL),
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
    build_issues = validation_results.errors
    assert len(build_issues) == 3
    expected_substr1 = "is already being used. Please choose another alias"
    expected_substr2 = "does not exist as a configured metric in the model"
    expected_substr3 = "Both offset_window and offset_to_grain set"
    missing_error_strings = set()
    for expected_str in [expected_substr1, expected_substr2, expected_substr3]:
        if not any(actual_str.as_readable_str().find(expected_str) != -1 for actual_str in build_issues):
            missing_error_strings.add(expected_str)
    assert len(missing_error_strings) == 0, (
        f"Failed to match one or more expected errors: {missing_error_strings} in "
        f"{set([x.as_readable_str() for x in build_issues])}"
    )
