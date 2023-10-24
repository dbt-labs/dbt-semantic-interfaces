from __future__ import annotations

from jinja2 import StrictUndefined
from jinja2.exceptions import SecurityError, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import SandboxedEnvironment

from dbt_semantic_interfaces.call_parameter_sets import (
    FilterCallParameterSets,
    ParseWhereFilterException,
)
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import (
    ParameterSetFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_dimension import (
    WhereFilterDimensionFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_entity import (
    WhereFilterEntityFactory,
)
from dbt_semantic_interfaces.parsing.where_filter.where_filter_time_dimension import (
    WhereFilterTimeDimensionFactory,
)


class WhereFilterParser:
    """Parses the template in the WhereFilter into FilterCallParameterSets."""

    @staticmethod
    def parse_call_parameter_sets(where_sql_template: str) -> FilterCallParameterSets:
        """Return the result of extracting the semantic objects referenced in the where SQL template string."""
        time_dimension_factory = WhereFilterTimeDimensionFactory()
        dimension_factory = WhereFilterDimensionFactory()
        entity_factory = WhereFilterEntityFactory()

        try:
            # the string that the sandbox renders is unused
            SandboxedEnvironment(undefined=StrictUndefined).from_string(where_sql_template).render(
                Dimension=dimension_factory.create,
                TimeDimension=time_dimension_factory.create,
                Entity=entity_factory.create,
            )
        except (UndefinedError, TemplateSyntaxError, SecurityError) as e:
            raise ParseWhereFilterException(f"Error while parsing Jinja template:\n{where_sql_template}") from e

        """
        Dimensions that are created with a grain or date_part parameter, for instance Dimension(...).grain(...), are
        added to time_dimension_call_parameter_sets otherwise they are add to dimension_call_parameter_sets
        """
        dimension_call_parameter_sets = []
        for dimension in dimension_factory.created:
            if dimension.time_granularity_name or dimension.date_part_name:
                time_dimension_factory.time_dimension_call_parameter_sets.append(
                    ParameterSetFactory.create_time_dimension(
                        dimension.name, dimension.time_granularity_name, dimension.entity_path, dimension.date_part_name
                    )
                )
            else:
                dimension_call_parameter_sets.append(
                    ParameterSetFactory.create_dimension(dimension.name, dimension.entity_path)
                )

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_factory.time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_factory.entity_call_parameter_sets),
        )
