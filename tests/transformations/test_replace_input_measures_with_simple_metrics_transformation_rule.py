import pytest

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.entity import PydanticEntity
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    PydanticConversionTypeParams,
    PydanticCumulativeTypeParams,
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
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
from dbt_semantic_interfaces.transformations.replace_input_measures_with_simple_metrics_transformation import (
    ReplaceInputMeasuresWithSimpleMetricsTransformationRule,
)
from dbt_semantic_interfaces.type_enums import (
    AggregationType,
    DimensionType,
    EntityType,
    MetricType,
    TimeGranularity,
)


def _project_config() -> PydanticProjectConfiguration:
    return PydanticProjectConfiguration()


def _build_semantic_model_with_measure(
    sm_name: str,
    measure_name: str,
    time_dim_name: str = "ds",
) -> PydanticSemanticModel:
    return PydanticSemanticModel(
        name=sm_name,
        node_relation=PydanticNodeRelation(alias=sm_name, schema_name="schema"),
        entities=[PydanticEntity(name="e1", type=EntityType.PRIMARY)],
        dimensions=[
            PydanticDimension(
                name=time_dim_name,
                type=DimensionType.TIME,
                type_params=PydanticDimensionTypeParams(time_granularity=TimeGranularity.DAY),
            )
        ],
        measures=[
            PydanticMeasure(
                name=measure_name,
                agg=AggregationType.SUM,
                agg_time_dimension=time_dim_name,
                expr="value",
            )
        ],
    )


def _matching_simple_metric(
    name: str,
    sm_name: str,
    measure_name: str,
    time_dim_name: str,
    fill_nulls_with: int | None,
    join_to_timespine: bool,
) -> PydanticMetric:
    return PydanticMetric(
        name=name,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(
            metric_aggregation_params=PydanticMetricAggregationParams(
                semantic_model=sm_name,
                agg=AggregationType.SUM,
                agg_params=None,
                agg_time_dimension=time_dim_name,
                non_additive_dimension=None,
            ),
            expr="value",
            join_to_timespine=join_to_timespine,
            fill_nulls_with=fill_nulls_with,
        ),
        description=None,
        label=None,
        config=None,
    )


def test_cumulative_no_measure_with_metric_input_is_unchanged() -> None:
    """If only a metric input is provided on cumulative, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    type_params = PydanticMetricTypeParams(cumulative_type_params=PydanticCumulativeTypeParams())
    assert (
        type_params.cumulative_type_params is not None
    ), "cumulative_type_params should be set as part of the test setup here."
    type_params.cumulative_type_params.metric = PydanticMetricInput(name="preexisting_simple")

    metric = PydanticMetric(name="cum", type=MetricType.CUMULATIVE, type_params=type_params)

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=[metric], project_configuration=_project_config())

    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == manifest.metrics


def test_cumulative_with_measure_and_metric_input_is_unchanged() -> None:
    """If both measure and metric inputs are provided, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True)
    type_params = PydanticMetricTypeParams(cumulative_type_params=PydanticCumulativeTypeParams())
    type_params.measure = input_measure
    assert (
        type_params.cumulative_type_params is not None
    ), "cumulative_type_params should be set as part of the test setup here."
    type_params.cumulative_type_params.metric = PydanticMetricInput(name="preexisting_simple")

    metric = PydanticMetric(name="cum", type=MetricType.CUMULATIVE, type_params=type_params)

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=[metric], project_configuration=_project_config())

    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == manifest.metrics


def test_cumulative_with_measure_reuses_existing_simple_metric() -> None:
    """With a preexisting matching simple metric, reuse it without creating a new one."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True)
    type_params = PydanticMetricTypeParams(cumulative_type_params=PydanticCumulativeTypeParams())
    type_params.measure = input_measure

    metric = PydanticMetric(name="cum", type=MetricType.CUMULATIVE, type_params=type_params)

    existing_simple = _matching_simple_metric(
        name="existing_simple_for_m1",
        sm_name="sm",
        measure_name="m1",
        time_dim_name="ds",
        fill_nulls_with=5,
        join_to_timespine=True,
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm], metrics=[metric, existing_simple], project_configuration=_project_config()
    )

    initial_simple_metric_count = sum(1 for m in manifest.metrics if m.type == MetricType.SIMPLE)
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = sum(1 for m in out.metrics if m.type == MetricType.SIMPLE)

    assert post_simple_metric_count == initial_simple_metric_count

    out_metric = next(m for m in out.metrics if m.type == MetricType.CUMULATIVE)
    assert (
        out_metric.type_params.cumulative_type_params is not None
        and out_metric.type_params.cumulative_type_params.metric is not None
    ), "cumulative_type_params should be set as part of the test setup here."
    assert out_metric.type_params.cumulative_type_params.metric.name == "existing_simple_for_m1"


def test_cumulative_with_measure_creates_one_for_multiple_metrics() -> None:
    """With two cumulative metrics, create a single shared simple metric when none exists."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=5, join_to_timespine=True)

    metrics: list[PydanticMetric] = []
    for i in range(2):
        type_params = PydanticMetricTypeParams(cumulative_type_params=PydanticCumulativeTypeParams())
        type_params.measure = input_measure
        metrics.append(PydanticMetric(name=f"cum_{i}", type=MetricType.CUMULATIVE, type_params=type_params))

    manifest = PydanticSemanticManifest(semantic_models=[sm], metrics=metrics, project_configuration=_project_config())

    initial_simple_metric_count = sum(1 for m in manifest.metrics if m.type == MetricType.SIMPLE)
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = sum(1 for m in out.metrics if m.type == MetricType.SIMPLE)

    assert post_simple_metric_count == initial_simple_metric_count + 1

    names = []
    for m in out.metrics:
        if m.type != MetricType.CUMULATIVE:
            continue
        # ughhh ugly type assertion here.
        assert (
            m.type_params.cumulative_type_params is not None and m.type_params.cumulative_type_params.metric is not None
        ), "cumulative_type_params should be set as part of the test setup here."
        names.append(m.type_params.cumulative_type_params.metric.name)
    assert len(set(names)) == 1


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_no_measure_with_metric_input_is_unchanged(side: str) -> None:
    """If only a metric input is provided on conversion, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    params = PydanticConversionTypeParams(entity="e1")
    if side == "base":
        params.base_metric = PydanticMetricInput(name="preexisting_simple")
    else:
        params.conversion_metric = PydanticMetricInput(name="preexisting_simple")

    metric = PydanticMetric(
        name=f"conv_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(conversion_type_params=params),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[metric],
        project_configuration=_project_config(),
    )

    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == manifest.metrics


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_and_metric_input_is_unchanged(side: str) -> None:
    """If both measure and metric inputs are provided, no changes are made."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")

    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True)
    params = PydanticConversionTypeParams(entity="e1")
    if side == "base":
        params.base_measure = input_measure
        params.base_metric = PydanticMetricInput(name="preexisting_simple")
    else:
        params.conversion_measure = input_measure
        params.conversion_metric = PydanticMetricInput(name="preexisting_simple")

    metric = PydanticMetric(
        name=f"conv_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(conversion_type_params=params),
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[metric],
        project_configuration=_project_config(),
    )

    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    assert out.metrics == manifest.metrics


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_reuses_existing_simple_metric(side: str) -> None:
    """With a preexisting matching simple metric, reuse it without creating a new one."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")
    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True)

    params = PydanticConversionTypeParams(entity="e1")
    if side == "base":
        params.base_measure = input_measure
    else:
        params.conversion_measure = input_measure

    metric = PydanticMetric(
        name=f"conv_{side}",
        type=MetricType.CONVERSION,
        type_params=PydanticMetricTypeParams(conversion_type_params=params),
    )

    existing_simple = _matching_simple_metric(
        name="existing_simple_for_m1",
        sm_name="sm",
        measure_name="m1",
        time_dim_name="ds",
        fill_nulls_with=7,
        join_to_timespine=True,
    )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=[metric, existing_simple],
        project_configuration=_project_config(),
    )

    initial_simple_metric_count = sum(1 for m in manifest.metrics if m.type == MetricType.SIMPLE)
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = sum(1 for m in out.metrics if m.type == MetricType.SIMPLE)

    assert post_simple_metric_count == initial_simple_metric_count

    out_params = next(m for m in out.metrics if m.type == MetricType.CONVERSION).type_params.conversion_type_params
    assert out_params is not None, "no conversion metric found; this is a fundamental problem."
    if side == "base":
        assert out_params.base_metric is not None
        chosen_name = out_params.base_metric.name
    else:
        assert out_params.conversion_metric is not None
        chosen_name = out_params.conversion_metric.name
    assert chosen_name == "existing_simple_for_m1"


@pytest.mark.parametrize("side", ["base", "conversion"])
def test_conversion_with_measure_creates_one_for_multiple_metrics(side: str) -> None:
    """With two conversion metrics, create a single shared simple metric when none exists."""
    sm = _build_semantic_model_with_measure("sm", "m1", time_dim_name="ds")
    input_measure = PydanticMetricInputMeasure(name="m1", fill_nulls_with=7, join_to_timespine=True)

    metrics: list[PydanticMetric] = []
    for i in range(2):
        params = PydanticConversionTypeParams(entity="e1")
        if side == "base":
            params.base_measure = input_measure
        else:
            params.conversion_measure = input_measure
        metrics.append(
            PydanticMetric(
                name=f"conv_{side}_{i}",
                type=MetricType.CONVERSION,
                type_params=PydanticMetricTypeParams(conversion_type_params=params),
            )
        )

    manifest = PydanticSemanticManifest(
        semantic_models=[sm],
        metrics=metrics,
        project_configuration=_project_config(),
    )

    initial_simple_metric_count = sum(1 for m in manifest.metrics if m.type == MetricType.SIMPLE)
    out = ReplaceInputMeasuresWithSimpleMetricsTransformationRule.transform_model(manifest)
    post_simple_metric_count = sum(1 for m in out.metrics if m.type == MetricType.SIMPLE)

    assert post_simple_metric_count == initial_simple_metric_count + 1

    names = []
    for m in out.metrics:
        if m.type != MetricType.CONVERSION:
            continue
        assert m.type_params.conversion_type_params is not None, "Conversion metric lacked conversion type params."
        params = m.type_params.conversion_type_params
        if side == "base":
            assert params.base_metric is not None
            names.append(params.base_metric.name)
        else:
            assert params.conversion_metric is not None
            names.append(params.conversion_metric.name)
    assert len(set(names)) == 1
