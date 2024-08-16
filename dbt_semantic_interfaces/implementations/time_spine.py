from __future__ import annotations

from typing import Sequence

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


class PydanticTimeSpine(HashableBaseModel, ProtocolHint[TimeSpine]):  # noqa: D101
    @override
    def _implements_protocol(self) -> TimeSpine:
        return self

    node_relation: PydanticNodeRelation
    primary_column: PydanticTimeSpinePrimaryColumn
    custom_granularity_columns: Sequence[PydanticTimeSpineCustomGranularityColumn] = []
