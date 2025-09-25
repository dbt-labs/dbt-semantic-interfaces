from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.transformations.boolean_measure import (
    BooleanMeasureAggregationRule,
)
from dbt_semantic_interfaces.type_enums import AggregationType, EntityType
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_boolean_measure_aggregation_rule_transforms_only_sum_boolean_measures() -> None:
    """Validate SUM_BOOLEAN measures get wrapped expr and SUM agg; others unchanged."""
    # Three measures:
    # - other_measure: not SUM_BOOLEAN -> unchanged
    # - measure_with_expr: SUM_BOOLEAN with existing expr -> wrap expr, change agg to SUM
    # - measure_without_expr: SUM_BOOLEAN with no expr -> use measure name, change agg to SUM
    semantic_model = PydanticSemanticModel(
        name="this_semantic_model",
        node_relation=PydanticNodeRelation(alias="this_semantic_model", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        measures=[
            PydanticMeasure(name="other_measure", agg=AggregationType.COUNT, expr="1"),
            PydanticMeasure(name="measure_with_expr", agg=AggregationType.SUM_BOOLEAN, expr="is_active"),
            PydanticMeasure(name="measure_without_expr", agg=AggregationType.SUM_BOOLEAN),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    out = BooleanMeasureAggregationRule.transform_model(manifest)
    out_sm = out.semantic_models[0]

    # Sanity: still three measures
    assert len(out_sm.measures) == 3

    # Unchanged non-boolean measure
    other_measure = next(m for m in out_sm.measures if m.name == "other_measure")
    assert other_measure.agg == AggregationType.COUNT
    assert other_measure.expr == "1"

    # SUM_BOOLEAN with existing expr -> wrapped and agg becomes SUM
    measure_with_expr = next(m for m in out_sm.measures if m.name == "measure_with_expr")
    assert measure_with_expr.agg == AggregationType.SUM
    assert measure_with_expr.expr == "CASE WHEN is_active THEN 1 ELSE 0 END"

    # SUM_BOOLEAN with no expr -> use measure name and agg becomes SUM
    measure_without_expr = next(m for m in out_sm.measures if m.name == "measure_without_expr")
    assert measure_without_expr.agg == AggregationType.SUM
    assert measure_without_expr.expr == "CASE WHEN measure_without_expr THEN 1 ELSE 0 END"
