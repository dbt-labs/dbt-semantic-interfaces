import logging

from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticDimensionInput,
    PydanticEntityInput,
    PydanticTimeDimensionInput,
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from tests.fixtures.semantic_manifest_fixtures import PydanticWhereFilterCompilerT

logger = logging.getLogger(__name__)


def test_extract_dimension_inputs(  # noqa: D
    pydantic_where_filter_compiler: PydanticWhereFilterCompilerT,
) -> None:
    compilation_result = pydantic_where_filter_compiler.compile(
        where_sql_template=(
            """{{ dimension('is_instant') }} AND {{ dimension('country', entity_path=['listing']) }} == 'US'"""
        )
    )

    assert compilation_result == PydanticWhereFilter(
        where_sql_template="""{{ is_instant }} AND {{ country }} == 'US'""",
        input_dimensions=[
            PydanticDimensionInput(name="is_instant"),
            PydanticDimensionInput(name="country", entity_path=["listing"]),
        ],
        input_time_dimensions=[],
        input_entities=[],
    )


def test_extract_time_dimension_inputs(  # noqa: D
    pydantic_where_filter_compiler: PydanticWhereFilterCompilerT,
) -> None:
    compilation_result = pydantic_where_filter_compiler.compile(
        where_sql_template="""{{ time_dimension('created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"""
    )

    assert compilation_result == PydanticWhereFilter(
        where_sql_template="""{{ created_at }} = '2020-01-01'""",
        input_time_dimensions=[
            PydanticTimeDimensionInput(name="created_at", granularity=TimeGranularity.MONTH, entity_path=["listing"])
        ],
        input_dimensions=[],
        input_entities=[],
    )


def test_extract_entity_inputs(  # noqa: D
    pydantic_where_filter_compiler: PydanticWhereFilterCompilerT,
) -> None:
    compilation_result = pydantic_where_filter_compiler.compile(
        where_sql_template=(
            """{{ entity('listing') }} AND {{ entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
        )
    )

    assert compilation_result == PydanticWhereFilter(
        where_sql_template="""{{ listing }} AND {{ user }} == 'TEST_USER_ID'""",
        input_entities=[PydanticEntityInput(name="listing"), PydanticEntityInput(name="user", entity_path=["listing"])],
        input_dimensions=[],
        input_time_dimensions=[],
    )
