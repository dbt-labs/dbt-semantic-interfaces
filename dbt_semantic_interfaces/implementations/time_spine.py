from __future__ import annotations

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.time_spine import (
    TimeSpine,
    TimeSpinePrimaryColumn,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity


class PydanticTimeSpinePrimaryColumn(HashableBaseModel, ProtocolHint[TimeSpinePrimaryColumn]):
    """Legacy Pydantic implementation of SemanticVersion. In the process of deprecation."""

    @override
    def _implements_protocol(self) -> TimeSpinePrimaryColumn:
        return self

    name: str
    time_granularity: TimeGranularity


class PydanticTimeSpine(HashableBaseModel, ProtocolHint[TimeSpine]):
    """Legacy Pydantic implementation of SemanticVersion. In the process of deprecation."""

    @override
    def _implements_protocol(self) -> TimeSpine:
        return self

    node_relation: PydanticNodeRelation
    primary_column: PydanticTimeSpinePrimaryColumn
