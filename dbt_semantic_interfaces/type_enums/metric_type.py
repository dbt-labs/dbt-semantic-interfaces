from dbt_semantic_interfaces.enum_extension import ExtendedEnum


class MetricType(ExtendedEnum):
    """Currently supported metric types."""

    SIMPLE = "simple"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"
