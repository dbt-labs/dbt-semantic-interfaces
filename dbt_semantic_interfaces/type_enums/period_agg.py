from dbt_semantic_interfaces.enum_extension import ExtendedEnum


class PeriodAggregation(ExtendedEnum):
    """Options for how to aggregate across a time period."""

    FIRST = "first"
    LAST = "last"
    AVERAGE = "average"
