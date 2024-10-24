from typing import Any, Dict

from pydantic import Field
from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.protocols.meta import ConfigMeta
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint


class SemanticLayerElementConfig(HashableBaseModel, ProtocolHint[ConfigMeta]):
    """PydanticDimension config."""

    @override
    def _implements_protocol(self) -> ConfigMeta:  # noqa: D
        return self

    meta: Dict[str, Any] = Field(default_factory=dict)
