from __future__ import annotations

from typing import List, Optional, Sequence


class WhereFilterDimension:
    """A dimension that is passed in through the where filter parameter."""

    def __init__(  # noqa
        self,
        name: str,
        entity_path: Sequence[str],
        time_granularity_name: Optional[str] = None,
        date_part_name: Optional[str] = None,
    ) -> None:
        self.name = name
        self.entity_path = entity_path
        self.time_granularity_name = time_granularity_name
        self.date_part_name = date_part_name

    def grain(self, time_granularity: str) -> WhereFilterDimension:
        """The time granularity."""
        self.time_granularity_name = time_granularity
        return self

    def date_part(self, date_part_name: str) -> WhereFilterDimension:
        """Date part to extract from the dimension."""
        self.date_part_name = date_part_name
        return self


class WhereFilterDimensionFactory:
    """Creates a WhereFilterDimension.

    Each call to `create` adds a WhereFilterDimension to `created`.
    """

    def __init__(self) -> None:  # noqa
        self.created: List[WhereFilterDimension] = []

    def create(self, dimension_name: str, entity_path: Sequence[str] = ()) -> WhereFilterDimension:
        """Gets called by Jinja when rendering {{ Dimension(...) }}."""
        dimension = WhereFilterDimension(name=dimension_name, entity_path=entity_path)
        self.created.append(dimension)
        return dimension
