import pytest

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.transformations.convert_count import ConvertCountToSumRule
from dbt_semantic_interfaces.type_enums import AggregationType, EntityType
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_legacy_measure_convert_count_rule_does_not_change_non_count_measures() -> None:
    """If a measure does not have agg type COUNT, it remains unchanged."""
    original_measure = PydanticMeasure(name="non_count_measure", agg=AggregationType.AVERAGE, expr="revenue")
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            original_measure.copy(deep=True),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    non_count_measure = next(m for m in out_model.measures if m.name == "non_count_measure")

    assert non_count_measure == original_measure, "Measure should not have been changed, but was."


def test_legacy_measure_convert_count_rule_raises_when_count_without_expr() -> None:
    """Measure with agg type COUNT without expr should raise the expected error."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            PydanticMeasure(name="missing_expr_count", agg=AggregationType.COUNT),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    with pytest.raises(
        ModelTransformError,
        match="uses a COUNT aggregation, which requires an expr to be provided",
    ):
        ConvertCountToSumRule.transform_model(manifest)


def test_legacy_measure_convert_count_rule_leaves_expr_one_but_sets_sum() -> None:
    """Measure with agg type COUNT and expr '1' should keep expr as '1' but set agg to SUM."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            PydanticMeasure(name="count_all_rows", agg=AggregationType.COUNT, expr="1"),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    count_all_rows = next(m for m in out_model.measures if m.name == "count_all_rows")

    assert count_all_rows.agg == AggregationType.SUM, "Aggregation type should have been changed to SUM, but was not."
    assert count_all_rows.expr == "1", "Expression should NOT have been changed, but was."


def test_legacy_measure_convert_count_rule_transforms_count_with_expr_to_case_expression() -> None:
    """Measure with agg type COUNT and expr other than '1' should wrap expr and set agg to SUM."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            PydanticMeasure(name="count_valid_values", agg=AggregationType.COUNT, expr="is_valid"),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)
    out_model = result.semantic_models[0]
    count_valid_values = next(m for m in out_model.measures if m.name == "count_valid_values")

    assert (
        count_valid_values.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        count_valid_values.expr == "CASE WHEN is_valid IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."


def test_legacy_measure_convert_count_rule_transforms_across_multiple_models() -> None:
    """Transform across multiple models, changing one COUNT-with-condition per model and leaving others unchanged.

    This is meant as a larger smoke test, and verifies that the behaviors work
    across multiple models and measures since other tests are focused on one measure at a time.

    When debugging, if other tests in this file are failing, start on the other tests first.
    """
    original_unchanged_metric_one = PydanticMeasure(
        name="non_count_model_one", agg=AggregationType.AVERAGE, expr="revenue"
    )
    semantic_model_one = PydanticSemanticModel(
        name="model_one",
        node_relation=PydanticNodeRelation(alias="model_one", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id_one", type=EntityType.PRIMARY)],
        measures=[
            # Will be transformed
            PydanticMeasure(name="count_with_condition_model_one", agg=AggregationType.COUNT, expr="status"),
            # Will remain the same expression but agg will become SUM
            PydanticMeasure(name="count_all_rows_model_one", agg=AggregationType.COUNT, expr="1"),
            # Will be entirely unchanged
            original_unchanged_metric_one.copy(deep=True),
        ],
    )

    original_unchanged_metric_two = PydanticMeasure(
        name="non_count_model_two", agg=AggregationType.MEDIAN, expr="price"
    )
    semantic_model_two = PydanticSemanticModel(
        name="model_two",
        node_relation=PydanticNodeRelation(alias="model_two", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id_two", type=EntityType.PRIMARY)],
        measures=[
            # Will be transformed
            PydanticMeasure(name="count_with_condition_model_two", agg=AggregationType.COUNT, expr="category"),
            # Will remain the same expression but agg will become SUM
            PydanticMeasure(name="count_all_rows_model_two", agg=AggregationType.COUNT, expr="1"),
            # Will be entirely unchanged
            original_unchanged_metric_two.copy(deep=True),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model_one, semantic_model_two],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    result = ConvertCountToSumRule.transform_model(manifest)

    # Model one checks
    out_model_one = next(sm for sm in result.semantic_models if sm.name == "model_one")

    transformed_measure_one = next(m for m in out_model_one.measures if m.name == "count_with_condition_model_one")
    assert transformed_measure_one.agg == AggregationType.SUM, "Aggregation type should have been changed, but was not."
    assert (
        transformed_measure_one.expr == "CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."

    unchanged_count_one = next(m for m in out_model_one.measures if m.name == "count_all_rows_model_one")
    assert (
        unchanged_count_one.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert unchanged_count_one.expr == "1", "Expression should not have been changed, but was."

    unchanged_non_count_one = next(m for m in out_model_one.measures if m.name == "non_count_model_one")
    assert unchanged_non_count_one == original_unchanged_metric_one, "Metric should not have been changed, but was."

    # Model two checks
    out_model_two = next(sm for sm in result.semantic_models if sm.name == "model_two")

    transformed_measure_two = next(m for m in out_model_two.measures if m.name == "count_with_condition_model_two")
    assert (
        transformed_measure_two.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert (
        transformed_measure_two.expr == "CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END"
    ), "Expression should have been changed, but was not (or was changed incorrectly)."

    unchanged_count_two = next(m for m in out_model_two.measures if m.name == "count_all_rows_model_two")
    assert (
        unchanged_count_two.agg == AggregationType.SUM
    ), "Aggregation type should have been changed to SUM, but was not."
    assert unchanged_count_two.expr == "1", "Expression should not have been changed, but was."

    unchanged_non_count_two = next(m for m in out_model_two.measures if m.name == "non_count_model_two")
    assert unchanged_non_count_two == original_unchanged_metric_two, "Metric should not have been changed, but was."
