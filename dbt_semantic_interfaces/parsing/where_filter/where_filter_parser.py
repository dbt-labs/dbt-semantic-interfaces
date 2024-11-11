from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
    ParseWhereFilterException,
)
from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.parsing.text_input.ti_description import (
    ObjectBuilderItemDescription,
    QueryItemType,
)
from dbt_semantic_interfaces.parsing.text_input.ti_processor import (
    ObjectBuilderTextProcessor,
)
from dbt_semantic_interfaces.parsing.text_input.valid_method import (
    ConfiguredValidMethodMapping,
)
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)


class WhereFilterParser:
    """Parses the template in the WhereFilter into FilterCallParameterSets."""

    @staticmethod
    def parse_item_descriptions(where_sql_template: str) -> Sequence[ObjectBuilderItemDescription]:
        """Parses the filter and returns the item descriptions."""
        text_processor = ObjectBuilderTextProcessor()

        try:
            return text_processor.collect_descriptions_from_template(
                jinja_template=where_sql_template,
                valid_method_mapping=ConfiguredValidMethodMapping.DEFAULT_MAPPING,
            )
        except Exception as e:
            raise ParseWhereFilterException(f"Error while parsing Jinja template:\n{where_sql_template}") from e

    @staticmethod
    def parse_call_parameter_sets(
        where_sql_template: str, custom_granularity_names: Sequence[str]
    ) -> FilterCallParameterSets:
        """Return the result of extracting the semantic objects referenced in the where SQL template string."""
        descriptions = WhereFilterParser.parse_item_descriptions(where_sql_template)

        """
        Dimensions that are created with a grain or date_part parameter, for instance Dimension(...).grain(...), are
        added to time_dimension_call_parameter_sets otherwise they are add to dimension_call_parameter_sets
        """
        dimension_call_parameter_sets = []
        time_dimension_call_parameter_sets = []
        entity_call_parameter_sets = []
        metric_call_parameter_sets = []

        for description in descriptions:
            item_type = description.item_type

            if item_type is QueryItemType.DIMENSION:
                if description.time_granularity_name or description.date_part_name:
                    time_dimension_call_parameter_sets.append(
                        ParameterSetFactory.create_time_dimension(
                            time_dimension_name=description.item_name,
                            time_granularity_name=description.time_granularity_name,
                            entity_path=description.entity_path,
                            date_part_name=description.date_part_name,
                            custom_granularity_names=custom_granularity_names,
                        )
                    )
                else:
                    dimension_call_parameter_sets.append(
                        ParameterSetFactory.create_dimension(
                            dimension_name=description.item_name,
                            entity_path=description.entity_path,
                        )
                    )
            elif item_type is QueryItemType.TIME_DIMENSION:
                time_dimension_call_parameter_sets.append(
                    ParameterSetFactory.create_time_dimension(
                        time_dimension_name=description.item_name,
                        time_granularity_name=description.time_granularity_name,
                        entity_path=description.entity_path,
                        date_part_name=description.date_part_name,
                        custom_granularity_names=custom_granularity_names,
                    )
                )
            elif item_type is QueryItemType.ENTITY:
                entity_call_parameter_sets.append(
                    ParameterSetFactory.create_entity(
                        entity_name=description.item_name,
                        entity_path=description.entity_path,
                    )
                )
            elif item_type is QueryItemType.METRIC:
                metric_call_parameter_sets.append(
                    ParameterSetFactory.create_metric(
                        metric_name=description.item_name,
                        group_by=description.group_by_for_metric_item,
                    )
                )
            else:
                assert_values_exhausted(item_type)

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
            metric_call_parameter_sets=tuple(metric_call_parameter_sets),
        )
