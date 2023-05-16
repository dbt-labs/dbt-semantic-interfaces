from __future__ import annotations

from typing import Any, Optional, Sequence

from pydantic import validator

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.entity import Entity
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.protocols.semantic_model import _SemanticModelMixin
from dbt_semantic_interfaces.references import (
    LinkableElementReference,
    MeasureReference,
)


class NodeRelation(HashableBaseModel):
    """Path object to where the data should be."""

    alias: str
    schema_name: str
    database: Optional[str] = None
    relation_name: str = ""

    @validator("relation_name", always=True)
    @classmethod
    def __create_default_relation_name(cls, value: Any, values: Any) -> str:  # type: ignore[misc]
        """Dynamically build the dot path for `relation_name`, if not specified."""
        if value:
            # Only build the relation_name if it was not present in config.
            return value

        alias, schema, database = values.get("alias"), values.get("schema_name"), values.get("database")
        if alias is None or schema is None:
            raise ValueError(
                f"Failed to build relation_name because alias and/or schema was None. schema: {schema}, alias: {alias}"
            )

        if database is not None:
            value = f"{database}.{schema}.{alias}"
        else:
            value = f"{schema}.{alias}"
        return value

    @staticmethod
    def from_string(sql_str: str) -> NodeRelation:  # noqa: D
        sql_str_split = sql_str.split(".")
        if len(sql_str_split) == 2:
            return NodeRelation(schema_name=sql_str_split[0], alias=sql_str_split[1])
        elif len(sql_str_split) == 3:
            return NodeRelation(database=sql_str_split[0], schema_name=sql_str_split[1], alias=sql_str_split[2])
        raise RuntimeError(
            f"Invalid input for a SQL table, expected form '<schema>.<table>' or '<db>.<schema>.<table>' "
            f"but got: {sql_str}"
        )


class SemanticModel(_SemanticModelMixin, HashableBaseModel, ModelWithMetadataParsing):
    """Describes a semantic model."""

    name: str
    description: Optional[str]
    node_relation: NodeRelation

    entities: Sequence[Entity] = []
    measures: Sequence[Measure] = []
    dimensions: Sequence[Dimension] = []

    metadata: Optional[Metadata]

    def get_measure(self, measure_reference: MeasureReference) -> Measure:  # noqa: D
        for measure in self.measures:
            if measure.reference == measure_reference:
                return measure

        raise ValueError(
            f"No dimension with name ({measure_reference.element_name}) in semantic_model with name ({self.name})"
        )

    def get_dimension(self, dimension_reference: LinkableElementReference) -> Dimension:  # noqa: D
        for dim in self.dimensions:
            if dim.reference == dimension_reference:
                return dim

        raise ValueError(f"No dimension with name ({dimension_reference}) in semantic_model with name ({self.name})")

    def get_entity(self, entity_reference: LinkableElementReference) -> Entity:  # noqa: D
        for entity in self.entities:
            if entity.reference == entity_reference:
                return entity

        raise ValueError(f"No entity with name ({entity_reference}) in semantic_model with name ({self.name})")
