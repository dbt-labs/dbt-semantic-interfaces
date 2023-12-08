from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import EntityType


class PydanticEntity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity."""

    name: str
    type: EntityType
    description: Optional[str] = None
    role: Optional[str] = None
    expr: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:  # noqa: D
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
