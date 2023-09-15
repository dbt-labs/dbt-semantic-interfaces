from __future__ import annotations

from typing import List, Sequence

from dbt_semantic_interfaces.call_parameter_sets import EntityCallParameterSet
from dbt_semantic_interfaces.parsing.where_filter.parameter_set_factory import ParameterSetFactory
from dbt_semantic_interfaces.parsing.where_filter.query_interface import (
    QueryInterfaceEntity,
    QueryInterfaceEntityFactory,
)
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from typing_extensions import override


class EntityStub(ProtocolHint[QueryInterfaceEntity]):
    @override
    def _implements_protocol(self) -> QueryInterfaceEntity:
        return self


class WhereFilterEntityFactory(ProtocolHint[QueryInterfaceEntityFactory]):
    @override
    def _implements_protocol(self) -> QueryInterfaceEntityFactory:
        return self

    def __init__(self):  # noqa
        self.entity_call_parameter_sets: List[EntityCallParameterSet] = []

    def create(self, entity_name: str, entity_path: Sequence[str] = ()) -> EntityStub:
        """Gets called by Jinja when rendering {{ Entity(...) }}."""
        self.entity_call_parameter_sets.append(ParameterSetFactory.create_entity(entity_name, entity_path))
        return EntityStub()
