from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.metadata import Metadata
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums.entity_type import EntityType


class PydanticEntity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None
    metadata: Optional[Metadata] = None

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:  # noqa: D
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
