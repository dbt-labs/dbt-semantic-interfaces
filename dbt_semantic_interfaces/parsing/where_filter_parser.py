from __future__ import annotations

from typing import List, Sequence

from jinja2 import StrictUndefined
from jinja2.exceptions import SecurityError, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import SandboxedEnvironment

from dbt_semantic_interfaces.call_parameter_sets import (
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
from dbt_semantic_interfaces.type_enums import TimeGranularity


class WhereFilterParser:
    """Parses the template in the WhereFilter into FilterCallParameterSets."""

    @staticmethod
    def parse_call_parameter_sets(where_sql_template: str) -> FilterCallParameterSets:
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
            SandboxedEnvironment(undefined=StrictUndefined).from_string(where_sql_template).render(
                dimension=_dimension_call,
                time_dimension=_time_dimension_call,
                entity=_entity_call,
            )
        except (UndefinedError, TemplateSyntaxError, SecurityError) as e:
            raise ParseWhereFilterException(f"Error while parsing Jinja template:\n{where_sql_template}") from e

        return FilterCallParameterSets(
            dimension_call_parameter_sets=tuple(dimension_call_parameter_sets),
            time_dimension_call_parameter_sets=tuple(time_dimension_call_parameter_sets),
            entity_call_parameter_sets=tuple(entity_call_parameter_sets),
        )
