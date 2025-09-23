from typing import Any, Dict

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.protocols.meta import SemanticLayerElementConfig
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dsi_pydantic_shim import Field


class PydanticSemanticLayerElementConfig(HashableBaseModel, ProtocolHint[SemanticLayerElementConfig]):
    """PydanticDimension config."""

    @override
    def _implements_protocol(self) -> SemanticLayerElementConfig:  # noqa: D
        return self

    meta: Dict[str, Any] = Field(default_factory=dict)
