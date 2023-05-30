from copy import deepcopy

import pytest

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.model_validator import ModelValidator
from dbt_semantic_interfaces.test_utils import find_semantic_model_with
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.validations.agg_time_dimension import (
    AggregationTimeDimensionRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    ModelValidationException,
)


def test_invalid_aggregation_time_dimension(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa:D
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    semantic_model_with_measures.measures[0].agg_time_dimension = "invalid_time_dimension"

    with pytest.raises(
        ModelValidationException,
        match=(
            "has the aggregation time dimension set to 'invalid_time_dimension', which is not a valid time dimension "
            "in the semantic model"
        ),
    ):
        model_validator = ModelValidator([AggregationTimeDimensionRule()])
        model_validator.checked_validations(model)


def test_unset_aggregation_time_dimension(simple_semantic_manifest: PydanticSemanticManifest) -> None:  # noqa:D
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    semantic_model_with_measures.measures[0].agg_time_dimension = None

    with pytest.raises(
        ModelValidationException,
        match=("Aggregation time dimension for measure \\w+ is not set!"),
    ):
        model_validator = ModelValidator([AggregationTimeDimensionRule()])
        model_validator.checked_validations(model)


def test_missing_primary_time_ok_if_all_measures_have_agg_time_dim(  # noqa:D
    simple_semantic_manifest: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest)
    semantic_model_with_measures, _ = find_semantic_model_with(
        model,
        lambda semantic_model: len(semantic_model.measures) > 0,
    )

    for dimension in semantic_model_with_measures.dimensions:
        if dimension.type == DimensionType.TIME:
            assert dimension.type_params, f"Time dimension `{dimension.name}` is missing `type_params`"
            dimension.type_params.is_primary = False

    model_validator = ModelValidator([AggregationTimeDimensionRule()])
    model_validator.checked_validations(model)
