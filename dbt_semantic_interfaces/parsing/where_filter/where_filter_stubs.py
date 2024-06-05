from dataclasses import dataclass
from typing import Optional, Sequence

from typing_extensions import override

from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.protocols.protocol_hint import ProtocolHint
from dbt_semantic_interfaces.protocols.query_interface import (
    QueryInterfaceEntity,
    QueryInterfaceMetric,
    QueryInterfaceTimeDimension,
)
from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity


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


class MetricStub(ProtocolHint[QueryInterfaceMetric]):
    """A Metric implementation that just satisfies the protocol.

    QueryInterfaceMetric currently has no methods and the parameter set is created in the factory.
    """

    @override
    def _implements_protocol(self) -> QueryInterfaceMetric:
        return self

    def descending(self, _is_descending: bool) -> QueryInterfaceMetric:  # noqa: D
        raise InvalidQuerySyntax("descending is invalid in the where parameter and filter spec")


@dataclass
class TimeDimensionStub(ProtocolHint[QueryInterfaceTimeDimension]):
    """A TimeDimension implementation that just satisfies the protocol.

    QueryInterfaceTimeDimension currently has no methods and the parameter set is created in the factory.
    So, there is nothing to do here.
    """

    element_name: str
    time_granularity: Optional[TimeGranularity] = None
    entity_path: Sequence[str] = ()
    date_part: Optional[DatePart] = None

    @override
    def _implements_protocol(self) -> QueryInterfaceTimeDimension:
        return self
