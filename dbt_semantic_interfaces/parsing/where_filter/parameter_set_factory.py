from typing import Optional, Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    MetricCallParameterSet,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.naming.dundered import DunderedNameFormatter
from dbt_semantic_interfaces.naming.keywords import is_metric_time_name
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.date_part import DatePart


class ParameterSetFactory:
    """Creates parameter sets for use in the Jinja sandbox.

    This is ONLY to be used for parsing where filters, and then only for the purposes of extracting
    some basic information about which elements are being accessed in the filter expression in the
    small number of contexts where a more complete semantic layer implementation is not available.

    In practice, today, this is used by the dbt core parser, which cannot take on a MetricFlow
    dependency, in order to provide some DAG annotations around elements referenced in where filters.
    """

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
        """Gets called by Jinja when rendering {{ TimeDimension(...) }}.

        There is a lot of strangeness around the time granularity specification here. Historically,
        we accepted time dimension names of the form `metric_time__week` or `customer__registration_date__month`
        in this interface. We have not yet fully deprecated this, and it's unclear if we ever will.

        Key points to note:
          1. The time dimension name parsing only accepts standard time granularities. This will not change.
          2. The time granularity parameter is what we want everybody to use because it's more explicit.
          3. The time granularity parameter will support custom granularities, so that's nice

        While this all may seem pretty bad it's not as terrible as all that - this class is only used
        for parsing where filters. When we solve the problems with our current where filter spec this will
        persist as a backwards compatibility model, but nothing more.
        """
        group_by_item_name = DunderedNameFormatter.parse_name(time_dimension_name)
        if len(group_by_item_name.entity_links) != 1 and not is_metric_time_name(group_by_item_name.element_name):
            raise ParseWhereFilterException(
                ParameterSetFactory._exception_message_for_incorrect_format(time_dimension_name)
            )
        grain_parsed_from_name = (
            group_by_item_name.time_granularity.value if group_by_item_name.time_granularity else None
        )
        grain_from_param = time_granularity_name
        if grain_parsed_from_name and grain_from_param and grain_from_param != grain_parsed_from_name:
            raise ParseWhereFilterException(
                f"Received different grains in `time_dimension_name` parameter ('{time_dimension_name}') "
                f"and `time_granularity_name` parameter ('{time_granularity_name}'). Remove the grain suffix "
                f"(`{grain_parsed_from_name}`) from the time dimension name and use the `time_granularity_name` "
                "parameter to specify the intendend grain."
            )

        time_granularity_name = grain_parsed_from_name or grain_from_param

        return TimeDimensionCallParameterSet(
            time_dimension_reference=TimeDimensionReference(element_name=group_by_item_name.element_name),
            entity_path=(
                tuple(EntityReference(element_name=arg) for arg in entity_path) + group_by_item_name.entity_links
            ),
            time_granularity_name=time_granularity_name.lower() if time_granularity_name else None,
            date_part=DatePart(date_part_name.lower()) if date_part_name else None,
        )

    @staticmethod
    def create_dimension(dimension_name: str, entity_path: Sequence[str] = ()) -> DimensionCallParameterSet:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        group_by_item_name = DunderedNameFormatter.parse_name(dimension_name)

        if len(group_by_item_name.entity_links) != 1 and not is_metric_time_name(group_by_item_name.element_name):
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
