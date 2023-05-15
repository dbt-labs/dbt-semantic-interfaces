from __future__ import annotations

from typing import Optional

from dbt_semantic_interfaces.objects.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.objects.metadata import Metadata
from dbt_semantic_interfaces.protocols.entity import _EntityMixin
from dbt_semantic_interfaces.type_enums.entity_type import EntityType


class Entity(_EntityMixin, HashableBaseModel, ModelWithMetadataParsing):
    """Describes a entity."""

    name: str
    description: Optional[str]
    type: EntityType
    role: Optional[str]
    expr: Optional[str] = None
    metadata: Optional[Metadata] = None
