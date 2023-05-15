from __future__ import annotations

from abc import abstractmethod
from typing import Optional, Protocol

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.entity_type import EntityType


class Entity(Protocol):
    """Describes a entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None

    @property
    @abstractmethod
    def reference(self) -> EntityReference:
        """Returns a reference to the entity."""
        ...

    @property
    @abstractmethod
    def is_linkable_entity_type(self) -> bool:
        """Indicates whether or not this entity can be used as a linkable entity type for joins.

        That is, can you use the entity as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings data source can be linked via listing__country, because listing
        is the primary key.

        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style data sources.
        """
        ...


class _Entity:
    """Some useful default implementation details of EntityProtocol."""

    name: str
    type: EntityType
    expr: Optional[str] = None

    @property
    def reference(self) -> EntityReference:
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
