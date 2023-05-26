from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol, Sequence

from dbt_semantic_interfaces.protocols.dimension import Dimension
from dbt_semantic_interfaces.protocols.entity import Entity
from dbt_semantic_interfaces.protocols.measure import Measure
from dbt_semantic_interfaces.protocols.metadata import Metadata
from dbt_semantic_interfaces.references import (
    LinkableElementReference,
    MeasureReference,
    SemanticModelReference,
)


class NodeRelation(Protocol):
    """Path object to where the data should be."""

    @property
    @abstractmethod
    def alias(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def schema_name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def database(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def relation_name(self) -> str:  # noqa: D
        pass


class SemanticModel(Protocol):
    """Describes a semantic model."""

    @property
    @abstractmethod
    def name(self) -> str:  # noqa: D
        pass

    @property
    @abstractmethod
    def description(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def node_relation(self) -> NodeRelation:  # noqa: D
        pass

    @property
    @abstractmethod
    def entities(self) -> Sequence[Entity]:  # noqa: D
        pass

    @property
    @abstractmethod
    def measures(self) -> Sequence[Measure]:  # noqa: D
        pass

    @property
    @abstractmethod
    def dimensions(self) -> Sequence[Dimension]:  # noqa: D
        pass

    @property
    @abstractmethod
    def entity_references(self) -> Sequence[LinkableElementReference]:
        """Returns a list of references to all entities in the semantic model."""
        ...

    @property
    @abstractmethod
    def dimension_references(self) -> Sequence[LinkableElementReference]:
        """Returns a list of references to all dimensions in the semantic model."""
        ...

    @property
    @abstractmethod
    def measure_references(self) -> Sequence[MeasureReference]:
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
    def partitions(self) -> Sequence[Dimension]:
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

    @property
    @abstractmethod
    def metadata(self) -> Optional[Metadata]:  # noqa: D
        pass
