from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Protocol, Sequence

from dbt_semantic_interfaces.implementations.elements.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.references import (
    LinkableElementReference,
    MeasureReference,
    SemanticModelReference,
)


class NodeRelation(Protocol):
    """Path object to where the data should be."""

    alias: str
    schema_name: str
    database: Optional[str]
    relation_name: str


class SemanticModel(Protocol):
    """Describes a semantic model."""

    name: str
    description: Optional[str]
    node_relation: NodeRelation

    entities: Sequence[Entity]
    measures: Sequence[Measure]
    dimensions: Sequence[Dimension]

    @property
    @abstractmethod
    def entity_references(self) -> List[LinkableElementReference]:
        """Returns a list of references to all entities in the semantic model."""
        ...

    @property
    @abstractmethod
    def dimension_references(self) -> List[LinkableElementReference]:
        """Returns a list of references to all dimensions in the semantic model."""
        ...

    @property
    @abstractmethod
    def measure_references(self) -> List[MeasureReference]:
        """Returns a list of references to all measures in the semantic model."""
        ...

    @property
    @abstractmethod
    def has_validity_dimensions(self) -> bool:
        """Returns True if there are validity params set on one or more dimensions."""
        ...

    @property
    @abstractmethod
    def validity_start_dimension(self) -> Optional[Dimension]:
        """Returns the validity window start dimension, if one is set."""
        ...

    @property
    @abstractmethod
    def validity_end_dimension(self) -> Optional[Dimension]:
        """Returns the validity window end dimension, if one is set."""
        ...

    @property
    @abstractmethod
    def partitions(self) -> List[Dimension]:
        """Returns a list of all partition dimensions."""
        ...

    @property
    @abstractmethod
    def partition(self) -> Optional[Dimension]:
        """Returns the partition dimension, if one is set."""
        ...

    @property
    @abstractmethod
    def reference(self) -> SemanticModelReference:
        """Returns a reference to this semantic model."""
        ...
