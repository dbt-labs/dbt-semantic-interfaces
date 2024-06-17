from __future__ import annotations

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import (
    HashableBaseModel,
    ModelWithMetadataParsing,
)
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.time_spine import (
    TimeSpine,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity


class PydanticTimeSpine(HashableBaseModel, ModelWithMetadataParsing, ProtocolHint[TimeSpine]):
    """Pydantic implementation of SemanticVersion."""

    @override
    def _implements_protocol(self) -> TimeSpine:
        return self

    location: str
    column_name: str
    grain: TimeGranularity
