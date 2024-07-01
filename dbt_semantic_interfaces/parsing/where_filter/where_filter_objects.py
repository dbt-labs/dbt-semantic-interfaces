from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from dbt_semantic_interfaces.type_enums import DatePart, TimeGranularity


@dataclass
class WhereFilterEntity:
    """A dimension that is passed in through the where filter parameter using Jinja syntax."""

    element_name: str
    entity_links: Sequence[str] = ()


@dataclass
class WhereFilterMetric:
    """A metric that is passed in through the where filter parameter using Jinja syntax."""

    element_name: str
    group_by: Sequence[str]


@dataclass
class WhereFilterDimension:
    """A dimension that is passed in through the where filter parameter using Jinja syntax."""

    name: str
    entity_path: Sequence[str] = ()  # Default is new here - consistent with TimeDimension
    # Behavior change: allows passing these params on init (why shouldn't we allow that?)
    # don't love the names, though. Copy MFS jinja object?
    time_granularity_name: Optional[str] = None
    date_part_name: Optional[str] = None

    def grain(self, time_granularity: str) -> WhereFilterDimension:  # noqa: D
        if self.time_granularity_name:
            raise RuntimeError("Grain was already set in the Dimension object parameters.")
        self.time_granularity_name = time_granularity
        return self

    def date_part(self, date_part: str) -> WhereFilterDimension:  # noqa: D
        if self.date_part_name:
            raise RuntimeError("Date part was already set in the Dimension object parameters.")
        self.date_part_name = date_part
        return self


@dataclass
class WhereFilterTimeDimension:
    """A time dimension that is passed in through the where filter parameter using Jinja syntax."""

    element_name: str
    time_granularity: Optional[TimeGranularity] = None  # not str?
    entity_path: Sequence[str] = ()
    # Can we change the name below? Breaking change bo one is using date part anyway, right? And it's not documented?
    date_part_name: Optional[DatePart] = None

    def grain(self, time_granularity: str) -> WhereFilterTimeDimension:  # noqa: D
        if self.time_granularity:
            raise RuntimeError("Grain was already set in the Dimension object parameters.")
        self.time_granularity = TimeGranularity(time_granularity.lower())
        return self

    def date_part(self, date_part: str) -> WhereFilterTimeDimension:  # noqa: D
        if self.date_part_name:
            raise RuntimeError("Date part was already set in the Dimension object parameters.")
        self.date_part_name = DatePart(date_part.lower())
        return self
