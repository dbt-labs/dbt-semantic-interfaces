from __future__ import annotations

from typing import Optional, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from dbt_semantic_interfaces.protocols import ProtocolHint
from dbt_semantic_interfaces.protocols.time_spine import (
    TimeSpine,
    TimeSpineCustomGranularityColumn,
    TimeSpinePrimaryColumn,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dsi_pydantic_shim import Field


class PydanticTimeSpinePrimaryColumn(HashableBaseModel, ProtocolHint[TimeSpinePrimaryColumn]):  # noqa: D101
    @override
    def _implements_protocol(self) -> TimeSpinePrimaryColumn:
        return self

    name: str
    time_granularity: TimeGranularity


class PydanticTimeSpineCustomGranularityColumn(  # noqa: D101
    HashableBaseModel, ProtocolHint[TimeSpineCustomGranularityColumn]
):
    @override
    def _implements_protocol(self) -> TimeSpineCustomGranularityColumn:
        return self

    name: str
    column_name: Optional[str] = None


class PydanticTimeSpine(HashableBaseModel, ProtocolHint[TimeSpine]):  # noqa: D101
    @override
    def _implements_protocol(self) -> TimeSpine:
        return self

    node_relation: PydanticNodeRelation
    primary_column: PydanticTimeSpinePrimaryColumn
    custom_granularities: Sequence[PydanticTimeSpineCustomGranularityColumn] = Field(default_factory=list)
