from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.project_configuration import (
    PydanticProjectConfiguration,
    PydanticTimeSpineTableConfiguration,
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
                    custom_granularity_columns=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                        PydanticTimeSpineCustomGranularityColumn(name="martian_week"),
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


def test_only_legacy_time_spine() -> None:  # noqa: D
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
            time_spine_table_configurations=[
                PydanticTimeSpineTableConfiguration(location="hurrr", column_name="fun_col", grain=TimeGranularity.DAY)
            ]
        ),
    )
    issues = validator.validate_semantic_manifest(semantic_manifest)
    assert not issues.has_blocking_issues
    assert len(issues.warnings) == 1
    assert "Time spines without YAML configuration are in the process of deprecation." in issues.warnings[0].message


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
                    custom_granularity_columns=[
                        PydanticTimeSpineCustomGranularityColumn(name="retail_year"),
                        PydanticTimeSpineCustomGranularityColumn(name="martian_week"),
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
