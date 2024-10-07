from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInputMeasure,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.time_spine import (
    PydanticTimeSpine,
    PydanticTimeSpineCustomGranularityColumn,
    PydanticTimeSpinePrimaryColumn,
)
from dbt_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)


def test_valid_time_spines() -> None:  # noqa: D
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="sum_measure",
                measures=[
                    PydanticMeasure(name="foo", agg=AggregationType.SUM, agg_time_dimension="dim", create_metric=True)
                ],
                dimensions=[
                    PydanticDimension(
                        name="dim",
                        type=DimensionType.TIME,
                        type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.SECOND),
                    )
                ],
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
            ),
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ds", time_granularity=TimeGranularity.DAY),
                    custom_granularities=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                        PydanticTimeSpineCustomGranularityColumn(name="martian_week", column_name="meep_meep_wk"),
                    ],
                ),
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine2", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ts", time_granularity=TimeGranularity.SECOND),
                ),
            ],
        ),
    )
    SemanticManifestValidator[PydanticSemanticManifest]().checked_validations(semantic_manifest)


def test_duplicate_time_spine_granularity() -> None:  # noqa: D
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="sum_measure",
                measures=[
                    PydanticMeasure(name="foo", agg=AggregationType.SUM, agg_time_dimension="dim", create_metric=True)
                ],
                dimensions=[
                    PydanticDimension(
                        name="dim",
                        type=DimensionType.TIME,
                        type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.SECOND),
                    )
                ],
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
            ),
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ds", time_granularity=TimeGranularity.SECOND),
                    custom_granularities=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                        PydanticTimeSpineCustomGranularityColumn(name="martian_week", column_name="meep_meep_wk"),
                    ],
                ),
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine2", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ts", time_granularity=TimeGranularity.SECOND),
                ),
            ],
        ),
    )
    issues = validator.validate_semantic_manifest(semantic_manifest)
    assert not issues.has_blocking_issues
    assert len(issues.warnings) == 1
    assert "Only one time spine is supported per granularity." in issues.warnings[0].message


def test_dimension_granularity_smaller_than_time_spine() -> None:  # noqa: D
    validator = SemanticManifestValidator[PydanticSemanticManifest]()
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="sum_measure",
                measures=[
                    PydanticMeasure(name="foo", agg=AggregationType.SUM, agg_time_dimension="dim", create_metric=True)
                ],
                dimensions=[
                    PydanticDimension(
                        name="dim",
                        type=DimensionType.TIME,
                        type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.SECOND),
                    )
                ],
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
            ),
        ],
        metrics=[],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ds", time_granularity=TimeGranularity.DAY),
                ),
            ],
        ),
    )
    issues = validator.validate_semantic_manifest(semantic_manifest)
    assert not issues.has_blocking_issues
    assert len(issues.warnings) == 1
    assert (
        "configuring a time spine at or below the smallest time dimension granularity is recommended"
        in issues.warnings[0].message
    )


def test_time_spines_with_invalid_names() -> None:  # noqa: D
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[
            semantic_model_with_guaranteed_meta(
                name="semantic_model",
                measures=[
                    PydanticMeasure(
                        name="sum_measure", agg=AggregationType.SUM, agg_time_dimension="dim", create_metric=True
                    )
                ],
                dimensions=[
                    PydanticDimension(
                        name="dim",
                        type=DimensionType.TIME,
                        type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.SECOND),
                    )
                ],
                entities=[PydanticEntity(name="entity", type=EntityType.PRIMARY)],
            ),
        ],
        metrics=[
            PydanticMetric(
                name="metric",
                description=None,
                type=MetricType.SIMPLE,
                type_params=PydanticMetricTypeParams(measure=PydanticMetricInputMeasure(name="sum_measure")),
            ),
        ],
        project_configuration=PydanticProjectConfiguration(
            time_spine_table_configurations=[],
            time_spines=[
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ds", time_granularity=TimeGranularity.MONTH),
                    custom_granularities=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                    ],
                ),
                PydanticTimeSpine(
                    node_relation=PydanticNodeRelation(alias="time_spine2", schema_name="my_fav_schema"),
                    primary_column=PydanticTimeSpinePrimaryColumn(name="ds", time_granularity=TimeGranularity.DAY),
                    custom_granularities=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                        PydanticTimeSpineCustomGranularityColumn(name="quarter"),
                        PydanticTimeSpineCustomGranularityColumn(name="semantic_model"),
                        PydanticTimeSpineCustomGranularityColumn(name="sum_measure"),
                        PydanticTimeSpineCustomGranularityColumn(name="dim"),
                        PydanticTimeSpineCustomGranularityColumn(name="entity"),
                        PydanticTimeSpineCustomGranularityColumn(name="metric"),
                        PydanticTimeSpineCustomGranularityColumn(name="mYfUnCuStOmGrAnUlArItY"),
                    ],
                ),
            ],
        ),
    )
    issues = SemanticManifestValidator[PydanticSemanticManifest]().validate_semantic_manifest(semantic_manifest)
    assert len(issues.warnings) == 1
    warning_msg = "To avoid unexpected query errors, configuring a time spine at or below the smallest"
    assert issues.warnings[0].message.startswith(warning_msg)

    assert issues.has_blocking_issues
    assert len(issues.errors) == 8
    error_messages = {err.message for err in issues.errors}
    for msg in [
        (
            "Custom granularity names must be unique, but found duplicate custom granularities with the names "
            "{'retail_year'}."
        ),
        "Can't use name `semantic_model` for a custom granularity when it was already used for a semantic_model.",
        "Can't use name `sum_measure` for a custom granularity when it was already used for a measure.",
        "Can't use name `dim` for a custom granularity when it was already used for a dimension.",
        "Can't use name `entity` for a custom granularity when it was already used for a entity.",
        "Can't use name `metric` for a custom granularity when it was already used for a metric.",
        (
            "Invalid name `quarter` - names cannot match reserved time granularity keywords (['NANOSECOND', "
            "'MICROSECOND', 'MILLISECOND', 'SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'])"
        ),
        (
            "Invalid name `mYfUnCuStOmGrAnUlArItY` - names may only contain lower case letters, numbers, and "
            "underscores. Additionally, names must start with a lower case letter, cannot end with an underscore, "
            "cannot contain dunders (double underscores, or __), and must be at least 2 characters long."
        ),
    ]:
        assert msg in error_messages
