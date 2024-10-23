from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import Field
from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.protocols.entity import EntityConfig
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.references import EntityReference
from dbt_semantic_interfaces.type_enums import EntityType


class PydanticEntityConfig(HashableBaseModel, ProtocolHint[EntityConfig]):
    """PydanticEntity config."""

    @override
    def _implements_protocol(self) -> EntityConfig:  # noqa: D
        return self

    meta: Dict[str, Any] = Field(default_factory=dict)


class PydanticEntity(HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None
    config: Optional[PydanticEntityConfig]

    @property
    def reference(self) -> EntityReference:  # noqa: D
        return EntityReference(element_name=self.name)

    @property
    def is_linkable_entity_type(self) -> bool:  # noqa: D
        return self.type in (EntityType.PRIMARY, EntityType.UNIQUE, EntityType.NATURAL)
