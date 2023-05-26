import pytest

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.implementations.elements.measure import PydanticMeasure
from dbt_semantic_interfaces.implementations.metric import (
    MetricType,
    PydanticMetric,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.implementations.semantic_model import (
    NodeRelation,
    SemanticModel,
)
from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.references import (
    DimensionReference,
    MeasureReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.test_utils import (
    metric_with_guaranteed_meta,
    semantic_model_with_guaranteed_meta,
)
from dbt_semantic_interfaces.type_enums.aggregation_type import AggregationType
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.dimension_const import DimensionConsistencyRule
from dbt_semantic_interfaces.validations.semantic_models import (
    SemanticModelTimeDimensionWarningsRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationException,
)


def test_incompatible_dimension_type() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"type conflict for dimension"):
        dim_name = "dim"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            SemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="dim1",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                    semantic_model_with_guaranteed_meta(
                        name="categoricaldim",
                        dimensions=[PydanticDimension(name=dim_name, type=DimensionType.CATEGORICAL)],
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=PydanticMetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_incompatible_dimension_is_partition() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"conflicting is_partition attribute for dimension"):
        dim_name = "dim1"
        measure_name = "measure"
        model_validator = ModelValidator([DimensionConsistencyRule()])
        model_validator.checked_validations(
            SemanticManifest(
                semantic_models=[
                    semantic_model_with_guaranteed_meta(
                        name="dim1",
                        measures=[PydanticMeasure(name=measure_name, agg=AggregationType.SUM)],
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=True,
                                type_params=PydanticDimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                    semantic_model_with_guaranteed_meta(
                        name="dim2",
                        dimensions=[
                            PydanticDimension(
                                name=dim_name,
                                type=DimensionType.TIME,
                                is_partition=False,
                                type_params=PydanticDimensionTypeParams(
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            )
                        ],
                    ),
                ],
                metrics=[
                    metric_with_guaranteed_meta(
                        name=measure_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=PydanticMetricTypeParams(measures=[measure_name]),
                    )
                ],
            )
        )


def test_multiple_primary_time_dimensions() -> None:  # noqa:D
    with pytest.raises(ModelValidationException, match=r"one of many defined as primary"):
        dimension_reference = TimeDimensionReference(element_name="ds")
        dimension_reference2 = DimensionReference(element_name="not_ds")
        measure_reference = MeasureReference(element_name="measure")
        model_validator = ModelValidator([SemanticModelTimeDimensionWarningsRule()])
        model_validator.checked_validations(
            model=SemanticManifest(
                semantic_models=[
                    SemanticModel(
                        name="dim1",
                        node_relation=NodeRelation(
                            alias="table",
                            schema_name="schema",
                        ),
                        measures=[
                            PydanticMeasure(
                                name=measure_reference.element_name,
                                agg=AggregationType.SUM,
                                agg_time_dimension=dimension_reference.element_name,
                            )
                        ],
                        dimensions=[
                            PydanticDimension(
                                name=dimension_reference.element_name,
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                            PydanticDimension(
                                name=dimension_reference2.element_name,
                                type=DimensionType.TIME,
                                type_params=PydanticDimensionTypeParams(
                                    is_primary=True,
                                    time_granularity=TimeGranularity.DAY,
                                ),
                            ),
                        ],
                    ),
                ],
                metrics=[
                    PydanticMetric(
                        name=measure_reference.element_name,
                        type=MetricType.MEASURE_PROXY,
                        type_params=PydanticMetricTypeParams(measures=[measure_reference.element_name]),
                    )
                ],
            )
        )
