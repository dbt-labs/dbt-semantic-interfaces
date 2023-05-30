from __future__ import annotations

from typing import List, Sequence

import jinja2

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.implementations.filters.call_parameter_sets import (
    DimensionCallParameterSet,
    EntityCallParameterSet,
    FilterCallParameterSets,
    ParseWhereFilterException,
    TimeDimensionCallParameterSet,
)
from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums.time_granularity import TimeGranularity


class PydanticWhereFilter(PydanticCustomInputParser, HashableBaseModel):
    """A filter applied to the data set containing measures, dimensions, identifiers relevant to the query.

    TODO: Clarify whether the filter applies to aggregated or un-aggregated data sets.

    The data set will contain dimensions as required by the query and the dimensions that a referenced in any of the
    filters that are used in the definition of metrics.
    """

    where_sql_template: str

    @classmethod
    def _from_yaml_value(
        cls,
        input: PydanticParseableValueType,
    ) -> PydanticWhereFilter:
        """Parses a WhereFilter from a string found in a user-provided model specification.

        User-provided constraint strings are SQL snippets conforming to the expectations of SQL WHERE clauses,
        and as such we parse them using our standard parse method below.
        """
        if isinstance(input, str):
            return PydanticWhereFilter(where_sql_template=input)
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")

    @property
    def call_parameter_sets(self) -> FilterCallParameterSets:
        """Return the result of extracting the semantic objects referenced in the where SQL template string."""
        # To extract the parameters to the calls, we use a function to record the parameters while rendering the Jinja
        # template. The rendered result is not used, but since Jinja has to render something, using this as a
        # placeholder. An alternative approach would have been to use the Jinja AST API, but this seemed simpler.
        _DUMMY_PLACEHOLDER = "DUMMY_PLACEHOLDER"

        dimension_call_parameter_sets: List[DimensionCallParameterSet] = []
        time_dimension_call_parameter_sets: List[TimeDimensionCallParameterSet] = []
        entity_call_parameter_sets: List[EntityCallParameterSet] = []

        def _dimension_call(dimension_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ dimension(...) }}."""
            dimension_call_parameter_sets.append(
                DimensionCallParameterSet(
                    dimension_reference=DimensionReference(element_name=dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                )
            )
            return _DUMMY_PLACEHOLDER

        def _time_dimension_call(
            time_dimension_name: str, time_granularity_name: str, entity_path: Sequence[str] = ()
        ) -> str:
            """Gets called by Jinja when rendering {{ time_dimension(...) }}."""
            time_dimension_call_parameter_sets.append(
                TimeDimensionCallParameterSet(
                    time_dimension_reference=TimeDimensionReference(element_name=time_dimension_name),
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    time_granularity=TimeGranularity(time_granularity_name),
                )
            )
            return _DUMMY_PLACEHOLDER

        def _entity_call(entity_name: str, entity_path: Sequence[str] = ()) -> str:
            """Gets called by Jinja when rendering {{ entity(...) }}."""
            entity_call_parameter_sets.append(
                EntityCallParameterSet(
                    entity_path=tuple(EntityReference(element_name=arg) for arg in entity_path),
                    entity_reference=EntityReference(element_name=entity_name),
                )
            )
            return _DUMMY_PLACEHOLDER

        try:
            jinja2.Template(self.where_sql_template, undefined=jinja2.StrictUndefined).render(
                dimension=_dimension_call,
                time_dimension=_time_dimension_call,
                entity=_entity_call,
            )
        except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
            raise ParseWhereFilterException(f"Error while parsing Jinja template:\n{self.where_sql_template}") from e

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
        )
