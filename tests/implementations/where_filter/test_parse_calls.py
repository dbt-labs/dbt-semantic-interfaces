import logging

import pytest

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity

logger = logging.getLogger(__name__)


def test_extract_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """\
                {{ Dimension('booking__is_instant') }} \
                AND {{ Dimension('user__country', entity_path=['listing']) }} == 'US'\
                """
        )
    ).call_parameter_sets

    assert parse_result == FilterCallParameterSets(
        dimension_call_parameter_sets=(
            DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="is_instant"),
                entity_path=(EntityReference("booking"),),
            ),
            DimensionCallParameterSet(
                dimension_reference=DimensionReference(element_name="country"),
                entity_path=(
                    EntityReference("listing"),
                    EntityReference("user"),
                ),
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_extract_dimension_with_grain_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """
                {{ Dimension('metric_time').grain('WEEK') }} > 2023-09-18
            """
        )
    ).call_parameter_sets

    assert parse_result == FilterCallParameterSets(
        dimension_call_parameter_sets=(),
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                entity_path=(),
                time_dimension_reference=TimeDimensionReference(element_name="metric_time"),
                time_granularity=TimeGranularity.WEEK,
            ),
        ),
        entity_call_parameter_sets=(),
    )


def test_extract_time_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ TimeDimension('user__created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"""
        )
    ).call_parameter_sets

    assert parse_result == FilterCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="created_at"),
                entity_path=(
                    EntityReference("listing"),
                    EntityReference("user"),
                ),
                time_granularity=TimeGranularity.MONTH,
            ),
        )
    )


def test_extract_metric_time_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=("""{{ TimeDimension('metric_time', 'month') }} = '2020-01-01'""")
    ).call_parameter_sets

    assert parse_result == FilterCallParameterSets(
        time_dimension_call_parameter_sets=(
            TimeDimensionCallParameterSet(
                time_dimension_reference=TimeDimensionReference(element_name="metric_time"),
                entity_path=(),
                time_granularity=TimeGranularity.MONTH,
            ),
        )
    )


def test_extract_entity_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ Entity('listing') }} AND {{ Entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
        )
    ).call_parameter_sets

    assert parse_result == FilterCallParameterSets(
        dimension_call_parameter_sets=(),
        entity_call_parameter_sets=(
            EntityCallParameterSet(
                entity_path=(),
                entity_reference=EntityReference("listing"),
            ),
            EntityCallParameterSet(
                entity_path=(EntityReference("listing"),),
                entity_reference=EntityReference("user"),
            ),
        ),
    )


def test_metric_time_in_dimension_call_error() -> None:  # noqa: D
    with pytest.raises(ParseWhereFilterException, match="so it should be referenced using TimeDimension"):
        assert (
            PydanticWhereFilter(where_sql_template="{{ Dimension('metric_time') }} > '2020-01-01'").call_parameter_sets
            is not None
        )


def test_invalid_entity_name_error() -> None:
    """Test to ensure we throw an error if an entity name is invalid."""
    bad_entity_filter = PydanticWhereFilter(where_sql_template="{{ Entity('order_id__is_food_order' )}}")

    with pytest.raises(ParseWhereFilterException, match="Entity name is in an incorrect format"):
        bad_entity_filter.call_parameter_sets
