from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.naming.keywords import (
    METRIC_TIME_ELEMENT_NAME,
    is_metric_time_name,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart


class ParameterSetFactory:
    """Creates parameter sets for use in the Jinja sandbox."""

    @staticmethod
    def _exception_message_for_incorrect_format(element_name: str) -> str:
        return (
            f"Name is in an incorrect format: {repr(element_name)}. It should be of the form: "
            f"<primary entity name>__<dimension_name>"
        )

    @staticmethod
    def create_time_dimension(
        time_dimension_name: str,
        time_granularity_name: Optional[str] = None,
        entity_path: Sequence[str] = (),
        date_part_name: Optional[str] = None,
    ) -> TimeDimensionCallParameterSet:
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}."""
        group_by_item_name = DunderedNameFormatter.parse_name(time_dimension_name)

        # metric_time is the only time dimension that does not have an associated primary entity, so the
        # GroupByItemName would not have any entity links.
        if is_metric_time_name(group_by_item_name.element_name):
            if len(group_by_item_name.entity_links) != 0 or group_by_item_name.time_granularity is not None:
                raise ParseWhereFilterException(
                    f"Name is in an incorrect format: {time_dimension_name} "
                    f"When referencing {METRIC_TIME_ELEMENT_NAME},"
                    "the name should not have any dunders (double underscores, or __)."
                )
        else:
            if len(group_by_item_name.entity_links) < 1 or group_by_item_name.time_granularity is not None:
                raise ParseWhereFilterException(
                    ParameterSetFactory._exception_message_for_incorrect_format(time_dimension_name)
                )

        return TimeDimensionCallParameterSet(
            time_dimension_reference=TimeDimensionReference(element_name=group_by_item_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + group_by_item_name.entity_links
            ),
            time_granularity=TimeGranularity(time_granularity_name) if time_granularity_name is not None else None,
            date_part=DatePart(date_part_name.lower()) if date_part_name else None,
        )

    @staticmethod
    def create_dimension(dimension_name: str, entity_path: Sequence[str] = ()) -> DimensionCallParameterSet:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        group_by_item_name = DunderedNameFormatter.parse_name(dimension_name)
        if is_metric_time_name(group_by_item_name.element_name):
            raise ParseWhereFilterException(
                f"{METRIC_TIME_ELEMENT_NAME} is a time dimension, so it should be referenced using "
                f"TimeDimension(...) or Dimension(...).grain(...)"
            )

        if len(group_by_item_name.entity_links) < 1:
            raise ParseWhereFilterException(ParameterSetFactory._exception_message_for_incorrect_format(dimension_name))

        return DimensionCallParameterSet(
            dimension_reference=DimensionReference(element_name=group_by_item_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + group_by_item_name.entity_links
            ),
        )

    @staticmethod
    def create_entity(entity_name: str, entity_path: Sequence[str] = ()) -> EntityCallParameterSet:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        structured_dundered_name = DunderedNameFormatter.parse_name(entity_name)
        if structured_dundered_name.time_granularity is not None:
            raise ParseWhereFilterException(
                f"Name is in an incorrect format: {repr(entity_name)}. " f"It should not contain a time grain suffix."
            )

        additional_entity_path_elements = tuple(
            EntityReference(element_name=entity_path_item) for entity_path_item in entity_path
        )

        return EntityCallParameterSet(
            entity_path=additional_entity_path_elements + structured_dundered_name.entity_links,
            entity_reference=EntityReference(element_name=structured_dundered_name.element_name),
        )

    @staticmethod
    def create_metric(metric_name: str, group_by: Sequence[str] = ()) -> MetricCallParameterSet:
        """Gets called by Jinja when rendering {{ Metric(...) }}."""
        if not group_by:
            raise ParseWhereFilterException(
                "`group_by` parameter is required for Metric in where filter. This is needed to determine 1) the "
                "granularity to aggregate the metric to and 2) how to join the metric to the rest of the query."
            )
        return MetricCallParameterSet(
            metric_reference=MetricReference(element_name=metric_name),
            group_by=tuple([LinkableElementReference(element_name=group_by_name) for group_by_name in group_by]),
        )
