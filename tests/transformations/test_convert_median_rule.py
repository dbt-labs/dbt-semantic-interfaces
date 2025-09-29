import pytest

from dbt_semantic_interfaces.errors import ModelTransformError
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
)
from dbt_semantic_interfaces.implementations.node_relation import PydanticNodeRelation
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.transformations.convert_median import (
    ConvertMedianToPercentileRule,
)
from dbt_semantic_interfaces.type_enums import AggregationType, EntityType
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def build_manifest_with_single_measure(measure: PydanticMeasure) -> PydanticSemanticManifest:
    """Helper to construct a manifest with a single semantic model and one measure."""
    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[measure],
    )
    return PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )


def test_median_rule_does_not_change_non_median_agg() -> None:
    """A measure with agg not MEDIAN remains unchanged."""
    original = PydanticMeasure(name="not_median", agg=AggregationType.SUM, expr="revenue")
    manifest = build_manifest_with_single_measure(original.copy(deep=True))

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure == original


def test_median_rule_sets_percentile_params_when_missing_and_changes_agg() -> None:
    """MEDIAN without agg_params gets params with percentile 0.5 and agg becomes PERCENTILE."""
    measure = PydanticMeasure(name="median_no_params", agg=AggregationType.MEDIAN, expr="value")
    manifest = build_manifest_with_single_measure(measure)

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure.agg == AggregationType.PERCENTILE
    assert out_measure.agg_params is not None
    assert out_measure.agg_params.percentile == 0.5


def test_median_rule_raises_when_percentile_not_median() -> None:
    """MEDIAN with agg_params.percentile != 0.5 raises an error."""
    measure = PydanticMeasure(
        name="median_bad_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.9),
    )
    manifest = build_manifest_with_single_measure(measure)

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while percentile is set to '0.9', a conflicting value",
    ):
        ConvertMedianToPercentileRule.transform_model(manifest)


def test_median_rule_raises_when_discrete_percentile_true() -> None:
    """MEDIAN with agg_params.use_discrete_percentile set raises an error."""
    measure = PydanticMeasure(
        name="median_discrete_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(use_discrete_percentile=True),
    )
    manifest = build_manifest_with_single_measure(measure)

    with pytest.raises(
        ModelTransformError,
        match="uses a MEDIAN aggregation, while use_discrete_percentile is set to true",
    ):
        ConvertMedianToPercentileRule.transform_model(manifest)


def test_median_rule_preserves_existing_median_percentile_value() -> None:
    """MEDIAN with agg_params.percentile == 0.5 remains 0.5 after transform and agg becomes PERCENTILE."""
    measure = PydanticMeasure(
        name="median_ok_percentile",
        agg=AggregationType.MEDIAN,
        expr="value",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
    )
    manifest = build_manifest_with_single_measure(measure)

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_measure = out.semantic_models[0].measures[0]
    assert out_measure.agg == AggregationType.PERCENTILE
    assert out_measure.agg_params is not None
    assert out_measure.agg_params.percentile == 0.5


def test_median_rule_iterates_across_multiple_measures() -> None:
    """Test that we can apply (and not apply) this to multiple measures in a single model.

    The expectation is that we have two MEDIAN measures that are converted and two
    non-MEDIAN measures that remain unchanged.
    """
    original_non_median_sum = PydanticMeasure(name="total_revenue", agg=AggregationType.SUM, expr="revenue")
    original_non_median_average = PydanticMeasure(name="average_price", agg=AggregationType.AVERAGE, expr="price")

    median_without_params = PydanticMeasure(name="median_value", agg=AggregationType.MEDIAN, expr="value")
    median_with_params = PydanticMeasure(
        name="median_latency",
        agg=AggregationType.MEDIAN,
        expr="latency",
        agg_params=PydanticMeasureAggregationParameters(percentile=0.5),
    )

    semantic_model = PydanticSemanticModel(
        name="example_model",
        node_relation=PydanticNodeRelation(alias="example_model", schema_name="example_schema"),
        entities=[PydanticEntity(name="entity_id", type=EntityType.PRIMARY)],
        measures=[
            median_without_params,
            median_with_params,
            original_non_median_sum.copy(deep=True),
            original_non_median_average.copy(deep=True),
        ],
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[semantic_model],
        metrics=[],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    out = ConvertMedianToPercentileRule.transform_model(manifest)
    out_model = out.semantic_models[0]

    # MEDIAN measures changed to PERCENTILE with percentile 0.5
    out_for_median_without_params = next(m for m in out_model.measures if m.name == "median_value")
    assert (
        out_for_median_without_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN measure, but was not."
    assert out_for_median_without_params.agg_params is not None
    assert (
        out_for_median_without_params.agg_params.percentile == 0.5
    ), "Percentile should be added for this MEDIAN measure and set to 0.5."

    out_for_median_with_params = next(m for m in out_model.measures if m.name == "median_latency")
    assert (
        out_for_median_with_params.agg == AggregationType.PERCENTILE
    ), "Aggregation type should be changed to PERCENTILE for MEDIAN measure, but was not."
    assert out_for_median_with_params.agg_params is not None
    assert (
        out_for_median_with_params.agg_params.percentile == 0.5
    ), "Percentile should be still be 0.5 for this MEDIAN measure, but was changed."

    # Non-MEDIAN measures unchanged
    out_total_revenue = next(m for m in out_model.measures if m.name == "total_revenue")
    assert out_total_revenue == original_non_median_sum

    out_average_price = next(m for m in out_model.measures if m.name == "average_price")
    assert out_average_price == original_non_median_average
