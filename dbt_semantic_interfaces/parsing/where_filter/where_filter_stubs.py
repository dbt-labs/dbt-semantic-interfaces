from dataclasses import dataclass
from typing import Sequence
from typing_extensions import override
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import QueryInterfaceEntity


@dataclass
class EntityStub(ProtocolHint[QueryInterfaceEntity]):
    """An Entity implementation that just satisfies the protocol.

    QueryInterfaceEntity currently has no methods and the parameter set is created in the factory.
    So, there is nothing to do here.
    """

    element_name: str
    entity_links: Sequence[str] = ()

    @override
    def _implements_protocol(self) -> QueryInterfaceEntity:
        return self
