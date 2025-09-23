from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import (
    PydanticMeasure,
    PydanticMeasureAggregationParameters,
)
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
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
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel
from dbt_semantic_interfaces.transformations.flatten_simple_metrics_with_measure_inputs import (
    FlattenSimpleMetricsWithMeasureInputsRule,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


def _project_config() -> PydanticProjectConfiguration:
    # Minimal project configuration for constructing a manifest directly in tests
    return PydanticProjectConfiguration()


def test_metric_with_measure_and_metric_agg_params_is_unchanged() -> None:
    """If a simple metric has both measure and metric_aggregation_params, it should not be altered."""
    # Build a minimal semantic model with one measure
    sm = PydanticSemanticModel(
        name="sm1",
        node_relation=PydanticNodeRelation(alias="sm1", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name="m1",
                agg=AggregationType.SUM,
                agg_time_dimension="ds",
                expr="value",
            )
        ],
    )

    # Create a metric that already has both a measure and metric_aggregation_params
    metric = PydanticMetric(
        name="m1",
        description="pre-flatten",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            measure=PydanticMetricInputMeasure(
                name="m1",
                fill_nulls_with=5,
                join_to_timespine=True,
            ),
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model="sm1",
                agg=AggregationType.SUM,
                agg_params=None,
                agg_time_dimension="ds",
                non_additive_dimension=None,
            ),
            expr="value",
            join_to_timespine=False,
            fill_nulls_with=None,
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[metric],
        project_configuration=_project_config(),
    )

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)

    # Verify nothing has changed in the metric
    assert len(transformed.metrics) == 1
    out = transformed.metrics[0]
    assert out.type == MetricType.SIMPLE
    assert out.type_params.measure == PydanticMetricInputMeasure(
        name="m1",
        fill_nulls_with=5,
        join_to_timespine=True,
    )
    assert out.type_params.metric_aggregation_params is not None
    assert out.type_params.metric_aggregation_params.semantic_model == "sm1"
    assert out.type_params.metric_aggregation_params.agg == AggregationType.SUM
    assert out.type_params.metric_aggregation_params.agg_time_dimension == "ds"
    assert out.type_params.expr == "value"
    # These are not changed
    assert out.type_params.join_to_timespine is False
    assert out.type_params.fill_nulls_with is None


def test_metric_with_measure_only_gets_populated_and_referenced_metric_uses_values() -> None:
    """Populates fields for simple metric with only measure; referencing metric remains intact across models."""
    # Two semantic models, the measure lives in sm2
    sm1 = PydanticSemanticModel(
        name="sm1",
        node_relation=PydanticNodeRelation(alias="sm1", schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="ds",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[],
    )

    sm2 = PydanticSemanticModel(
        name="sm2",
        node_relation=PydanticNodeRelation(alias="sm2", schema_name="schema"),
        entities=[PydanticEntity(name="e2", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name="event_time",
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name="orders",
                agg=AggregationType.PERCENTILE,
                agg_params=PydanticMeasureAggregationParameters(percentile=50.0, use_discrete_percentile=True),
                agg_time_dimension="event_time",
                expr="amount",
            )
        ],
    )

    # Metric that should get populated by the rule (has a measure, no metric_aggregation_params)
    metric_to_flatten = PydanticMetric(
        name="orders",
        description="needs flatten",
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            # intentionally no metric_aggregation_params here
            measure=PydanticMetricInputMeasure(name="orders", fill_nulls_with=7, join_to_timespine=True),
            # These fields should have triggered a validation error, but here just
            # to guarantee behavior
            fill_nulls_with=1,
            join_to_timespine=False,
        ),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm1, sm2],
        metrics=[metric_to_flatten],
        project_configuration=_project_config(),
    )

    transformed = FlattenSimpleMetricsWithMeasureInputsRule.transform_model(manifest)

    # Ensure both metrics are present
    assert len(transformed.metrics) == 1

    # Find updated simple metric
    updated_simple = next(m for m in transformed.metrics if m.name == "orders")
    assert updated_simple.type == MetricType.SIMPLE
    # The rule should populate aggregation params from the measure in sm2
    assert updated_simple.type_params.metric_aggregation_params is not None
    agg_params = updated_simple.type_params.metric_aggregation_params
    assert agg_params.semantic_model == "sm2"
    assert agg_params.agg == AggregationType.PERCENTILE
    assert agg_params.agg_time_dimension == "event_time"
    assert updated_simple.type_params.expr == "amount"
    # join_to_timespine should be set to True and fill_nulls_with to 7 by
    # pulling from the measure
    assert updated_simple.type_params.join_to_timespine is True
    assert updated_simple.type_params.fill_nulls_with == 7
