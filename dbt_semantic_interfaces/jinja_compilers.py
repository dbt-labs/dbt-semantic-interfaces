from typing import Generic, List, Optional, Type

import jinja2

from dbt_semantic_interfaces.protocols.where_filter import (
    DimensionInputT,
    EntityInputT,
    TimeDimensionInputT,
    WhereFilterT,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity


class WhereFilterCompiler(Generic[WhereFilterT, DimensionInputT, TimeDimensionInputT, EntityInputT]):
    """Compiler which compiles a where filter string to a valid WhereFilter.

    Example:
    >>> where_filter_str = "{{ dimension('country', ['user']) }} = 'US' " \
    ...     " AND {{ time_dimension('ds', 'month', entity_path=['transaction']) }} >= '2023-07-01'" \
    ...     " AND {{ entity('user') }} = 'SOME_USER_ID'"
    >>> compiler = WhereFilterCompiler[ImpWhereFilter, ImpDimensionInput, ImpTimeDimensionInput, ImpEntityInput](
    >>>     where_filter_class=ImpWhereFilterCompiler,
    >>>     dimension_input_class=ImpDimensionInput,
    >>>     time_dimension_input_class=ImpTimeDimensionInput,
    >>>     entity_input_class=ImpEntityInput
    >>> )
    >>> compiler.compile(where_filter_str)
    ImpWhereFilter(
        where_sql_template="{{ country }} = 'US' AND {{ ds }} >= '2023-07-01' AND {{ user }} = 'SOME_USER_ID'"
        input_dimensions=[ImpDimensionInput(name='country', entity_path= ['user']),],
        input_time_dimensions=[ImpTimeDimensionInput(
            name='ds',
            granularity=TimeGranularity.MONTH,
            entity_path=['transaction']
        ),],
        input_entities=[ImpEntityInput(name='user'),]
    )
    """

    where_filter_class: Type[WhereFilterT]
    dimension_input_class: Type[DimensionInputT]
    time_dimension_input_class: Type[TimeDimensionInputT]
    entity_input_class: Type[EntityInputT]

    def __init__(  # noqa: D
        self,
        where_filter_class: Type[WhereFilterT],
        dimension_input_class: Type[DimensionInputT],
        time_dimension_input_class: Type[TimeDimensionInputT],
        entity_input_class: Type[EntityInputT],
    ):
        self.where_filter_class = where_filter_class
        self.dimension_input_class = dimension_input_class
        self.time_dimension_input_class = time_dimension_input_class
        self.entity_input_class = entity_input_class

    def compile(self, where_sql_template: str) -> WhereFilterT:
        """Compiles the `where_sql_template` to a valid `WhereFilterT`."""
        dimesion_inputs: List[DimensionInputT] = []
        time_dimension_inputs: List[TimeDimensionInputT] = []
        entity_inputs: List[EntityInputT] = []

        def _dimension(name: str, entity_path: Optional[List[str]] = None) -> str:
            input = self.dimension_input_class(name=name, entity_path=entity_path)
            dimesion_inputs.append(input)
            return f"{{{{ {name} }}}}"

        def _time_dimension(name: str, granularity: str, entity_path: Optional[List[str]] = None):
            input = self.time_dimension_input_class(
                name=name, granularity=TimeGranularity(granularity), entity_path=entity_path
            )
            time_dimension_inputs.append(input)
            return f"{{{{ {name} }}}}"

        def _entity(name: str, entity_path: Optional[List[str]] = None):
            input = self.entity_input_class(name=name, entity_path=entity_path)
            entity_inputs.append(input)
            return f"{{{{ {name} }}}}"

        compiled_filter_str = jinja2.Template(where_sql_template).render(
            dimension=_dimension,
            time_dimension=_time_dimension,
            entity=_entity,
        )

        return self.where_filter_class(
            where_sql_template=compiled_filter_str,
            input_dimensions=dimesion_inputs,
            input_time_dimensions=time_dimension_inputs,
            input_entities=entity_inputs,
        )
