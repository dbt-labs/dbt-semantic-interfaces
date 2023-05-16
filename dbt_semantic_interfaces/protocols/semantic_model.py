from __future__ import annotations

from abc import abstractmethod
from typing import List, Optional, Protocol, Sequence

from dbt_semantic_interfaces.objects.elements.dimension import Dimension
from dbt_semantic_interfaces.objects.elements.measure import Measure
from dbt_semantic_interfaces.protocols.entity import Entity
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


class _SemanticModelMixin:
    """Some useful default implementation details of SemanticModelProtocol."""

    name: str
    entities: Sequence[Entity] = []
    measures: Sequence[Measure] = []
    dimensions: Sequence[Dimension] = []

    @property
    def entity_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.entities]

    @property
    def dimension_references(self) -> List[LinkableElementReference]:  # noqa: D
        return [i.reference for i in self.dimensions]

    @property
    def measure_references(self) -> List[MeasureReference]:  # noqa: D
        return [i.reference for i in self.measures]

    @property
    def has_validity_dimensions(self) -> bool:
        return any([dim.validity_params is not None for dim in self.dimensions])

    @property
    def validity_start_dimension(self) -> Optional[Dimension]:
        validity_start_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_start]
        if not validity_start_dims:
            return None
        assert (
            len(validity_start_dims) == 1
        ), "Found more than one validity start dimension. This should have been blocked in validation!"
        return validity_start_dims[0]

    @property
    def validity_end_dimension(self) -> Optional[Dimension]:
        validity_end_dims = [dim for dim in self.dimensions if dim.validity_params and dim.validity_params.is_end]
        if not validity_end_dims:
            return None
        assert (
            len(validity_end_dims) == 1
        ), "Found more than one validity end dimension. This should have been blocked in validation!"
        return validity_end_dims[0]

    @property
    def partitions(self) -> List[Dimension]:
        return [dim for dim in self.dimensions or [] if dim.is_partition]

    @property
    def partition(self) -> Optional[Dimension]:
        partitions = self.partitions
        if not partitions:
            return None
        if len(partitions) > 1:
            raise ValueError(f"too many partitions for semantic_model {self.name}")
        return partitions[0]

    @property
    def reference(self) -> SemanticModelReference:
        return SemanticModelReference(semantic_model_name=self.name)
