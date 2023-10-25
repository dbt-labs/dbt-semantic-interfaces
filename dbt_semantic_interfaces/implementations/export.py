from __future__ import annotations

from typing import Optional

from pydantic import Field
from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.export import Export, ExportConfig
from dbt_semantic_interfaces.type_enums.export_destination_type import (
    ExportDestinationType,
)


class PydanticExportConfig(HashableBaseModel, ProtocolHint[ExportConfig]):
    """Pydantic implementation of ExportConfig."""

    @override
    def _implements_protocol(self) -> ExportConfig:
        return self

    export_as: ExportDestinationType
    schema_name: Optional[str] = Field(alias="schema")  # `schema` is a BaseModel attribute
    alias: Optional[str] = None


class PydanticExport(HashableBaseModel, ProtocolHint[Export]):
    """Pydantic implementation of Export."""

    @override
    def _implements_protocol(self) -> Export:
        return self

    name: str
    config: PydanticExportConfig
