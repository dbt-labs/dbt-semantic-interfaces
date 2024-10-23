from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional, Protocol

from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import EntityType


class EntityConfig(Protocol):  # noqa: D
    """The config property allows you to configure additional resources/metadata."""

    @property
    @abstractmethod
    def meta(self) -> Dict[str, Any]:
        """The meta field can be used to set metadata for a resource."""
        pass


class Entity(Protocol):
    """Describes a entity."""

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
    def type(self) -> EntityType:  # noqa: D
        pass

    @property
    @abstractmethod
    def role(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def expr(self) -> Optional[str]:  # noqa: D
        pass

    @property
    @abstractmethod
    def reference(self) -> EntityReference:
        """Returns a reference to the entity."""
        ...

    @property
    @abstractmethod
    def is_linkable_entity_type(self) -> bool:
        """Indicates whether this entity can be used as a linkable entity type for joins.

        That is, can you use the entity as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings data source can be linked via listing__country, because listing
        is the primary key.

        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style data sources.
        """
        ...

    @property
    @abstractmethod
    def label(self) -> Optional[str]:
        """Returns a string representing a human readable label for the entity."""
        pass

    @property
    @abstractmethod
    def config(self) -> Optional[EntityConfig]:  # noqa: D
        pass
