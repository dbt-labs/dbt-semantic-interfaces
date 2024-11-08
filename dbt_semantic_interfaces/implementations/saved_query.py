from __future__ import annotations

from typing import Any, List, Optional, Union

from typing_extensions import Self, override

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


class PydanticSavedQuery(
    HashableBaseModel,
    ModelWithMetadataParsing,
    ProtocolHint[SavedQuery],
):
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
    tags: Union[str, List[str]] = Field(
        default_factory=list,
    )

    @classmethod
    def parse_obj(cls, input: Any) -> Self:  # noqa
        if isinstance(input, dict):
            if isinstance(input.get("tags"), str):
                input["tags"] = [input["tags"]]
            if isinstance(input.get("tags"), list):
                input["tags"].sort()
        return super(HashableBaseModel, cls).parse_obj(input)
