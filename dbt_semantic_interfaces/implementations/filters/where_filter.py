from __future__ import annotations

from typing import List, Optional

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    PydanticCustomInputParser,
    PydanticParseableValueType,
)
from dbt_semantic_interfaces.jinja_compilers import WhereFilterCompiler
from dbt_semantic_interfaces.type_enums import TimeGranularity


class PydanticDimensionInput(HashableBaseModel):
    """Dimension input information for the WhereFilter."""

    name: str
    entity_path: Optional[List[str]] = None


class PydanticTimeDimensionInput(HashableBaseModel):
    """Time dimension input information for the WhereFilter."""

    name: str
    granularity: TimeGranularity
    entity_path: Optional[List[str]] = None


class PydanticEntityInput(HashableBaseModel):
    """Entity input information for the WhereFilter."""

    name: str
    entity_path: Optional[List[str]] = None


class PydanticWhereFilter(PydanticCustomInputParser, HashableBaseModel):
    """A filter applied to the data set containing measures, dimensions, identifiers relevant to the query.

    TODO: Clarify whether the filter applies to aggregated or un-aggregated data sets.

    The data set will contain dimensions as required by the query and the dimensions that a referenced in any of the
    filters that are used in the definition of metrics.
    """

    where_sql_template: str
    input_dimensions: List[PydanticDimensionInput]
    input_time_dimensions: List[PydanticTimeDimensionInput]
    input_entities: List[PydanticEntityInput]

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
            compiler = WhereFilterCompiler[
                PydanticWhereFilter, PydanticDimensionInput, PydanticTimeDimensionInput, PydanticEntityInput
            ](
                where_filter_class=PydanticWhereFilter,
                dimension_input_class=PydanticDimensionInput,
                time_dimension_input_class=PydanticTimeDimensionInput,
                entity_input_class=PydanticEntityInput,
            )
            return compiler.compile(where_sql_template=input)
        else:
            raise ValueError(f"Expected input to be of type string, but got type {type(input)} with value: {input}")
