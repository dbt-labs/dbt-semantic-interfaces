from dbt_semantic_interfaces.enum_extension import ExtendedEnum


class ConversionCalculationType(ExtendedEnum):
    """Types of calculations for a conversion metric."""

    CONVERSIONS = "conversions"
    CONVERSION_RATE = "conversion_rate"
