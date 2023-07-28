import logging

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
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
                {{ dimension('booking__is_instant') }} \
                AND {{ dimension('user__country', entity_path=['listing']) }} == 'US'\
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


def test_extract_time_dimension_call_parameter_sets() -> None:  # noqa: D
    parse_result = PydanticWhereFilter(
        where_sql_template=(
            """{{ time_dimension('user__created_at', 'month', entity_path=['listing']) }} = '2020-01-01'"""
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
        where_sql_template=("""{{ time_dimension('metric_time', 'month') }} = '2020-01-01'""")
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
            """{{ entity('listing') }} AND {{ entity('user', entity_path=['listing']) }} == 'TEST_USER_ID'"""
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
