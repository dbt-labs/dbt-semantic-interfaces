from __future__ import annotations

from typing import List, Optional

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.saved_query import SavedQuery


class PydanticSavedQuery(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[SavedQuery]):
    """Pydantic implementation of SavedQuery."""

    @override
    def _implements_protocol(self) -> SavedQuery:
        return self

    name: str
    metrics: List[str]
    group_bys: List[str] = []
    where: List[PydanticWhereFilter] = []

    description: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None
