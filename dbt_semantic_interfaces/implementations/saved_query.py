from __future__ import annotations

from typing import List, Optional

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.implementations.export import PydanticExport
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilterIntersection,
)
from dbt_semantic_interfaces.implementations.metadata import PydanticMetadata
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.saved_query import (
    SavedQuery,
    SavedQueryQueryParams,
)
from dsi_pydantic_shim import Field


class PydanticSavedQueryQueryParams(HashableBaseModel, ProtocolHint[SavedQueryQueryParams]):
    """Pydantic implementation of SavedQuery."""

    @override
    def _implements_protocol(self) -> SavedQueryQueryParams:
        return self

    metrics: List[str]
    group_by: List[str] = Field(default_factory=list)
    order_by: List[str] = Field(default_factory=list)
    limit: Optional[int] = None
    where: Optional[PydanticWhereFilterIntersection] = None


class PydanticSavedQuery(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[SavedQuery]):
    """Pydantic implementation of SavedQuery."""

    @override
    def _implements_protocol(self) -> SavedQuery:
        return self

    name: str
    query_params: PydanticSavedQueryQueryParams
    description: Optional[str] = None
    metadata: Optional[PydanticMetadata] = None
    label: Optional[str] = None
    exports: List[PydanticExport] = Field(default_factory=list)
