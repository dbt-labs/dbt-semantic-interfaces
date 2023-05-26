import pytest

from dbt_semantic_interfaces.implementations.elements.dimension import (
    PydanticDimension,
    PydanticDimensionTypeParams,
)
from dbt_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from dbt_semantic_interfaces.type_enums.dimension_type import DimensionType
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)


@pytest.mark.skip("TODO: Will convert to validation rule")
def test_semantic_model_invalid_sql() -> None:  # noqa:D
    with pytest.raises(SemanticManifestValidationException, match=r"Invalid SQL"):
        semantic_model_with_guaranteed_meta(
            name="invalid_sql_source",
            dimensions=[
                PydanticDimension(
                    name="ds",
                    type=DimensionType.TIME,
                    type_params=PydanticDimensionTypeParams(
                        time_granularity=TimeGranularity.DAY,
                    ),
                )
            ],
        )
