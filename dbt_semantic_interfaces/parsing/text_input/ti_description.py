from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple


@dataclass(frozen=True)
class QueryItemDescription:
    """Describes a query item specified by the user.

    For example, the following specified in an order-by of a saved query:

        Dimension("user__created_at", entity_path=['listing']).grain('day').date_part('month').descending(True)

        ->

        QueryItemDescription(
            item_type=GroupByItemType.DIMENSION,
            item_name="user__created_at",
            entity_path=['listing'],
            time_granularity_name='day'
            date_part_name='month'
            descending=True
        )

    * This is named "...Description" to keep it general as the way users specify query items will change significantly
      in the not-so-distant future.
    * This can be later expanded to a set of classes for better typing.
    """

    item_type: QueryItemType
    item_name: str
    entity_path: Tuple[str, ...]
    group_by_for_metric_item: Tuple[str, ...]
    time_granularity_name: Optional[str]
    date_part_name: Optional[str]
    descending: Optional[bool]

    def __post_init__(self) -> None:  # noqa: D105
        if self.item_type is QueryItemType.ENTITY or self.item_type is QueryItemType.METRIC:
            assert (
                self.time_granularity_name is None
            ), f"{self.time_granularity_name=} is not supported for {self.item_type=}"
            assert self.date_part_name is None, f"{self.date_part_name=} is not supported for {self.item_type=}"

        if self.item_type is not QueryItemType.METRIC:
            assert (
                not self.group_by_for_metric_item
            ), f"{self.group_by_for_metric_item=} is not supported for {self.item_type=}"

    def create_modified(
        self,
        time_granularity_name: Optional[str] = None,
        date_part_name: Optional[str] = None,
        descending: Optional[bool] = None,
    ) -> QueryItemDescription:
        """Create one with the same fields as self except the ones provided."""
        return QueryItemDescription(
            item_type=self.item_type,
            item_name=self.item_name,
            entity_path=self.entity_path,
            time_granularity_name=time_granularity_name or self.time_granularity_name,
            date_part_name=date_part_name or self.date_part_name,
            group_by_for_metric_item=self.group_by_for_metric_item,
            descending=descending or self.descending,
        )

    def with_descending_unset(self) -> QueryItemDescription:
        """Return this with the `descending` field set to None."""
        return QueryItemDescription(
            item_type=self.item_type,
            item_name=self.item_name,
            entity_path=self.entity_path,
            time_granularity_name=self.time_granularity_name,
            date_part_name=self.date_part_name,
            group_by_for_metric_item=self.group_by_for_metric_item,
            descending=None,
        )


class QueryItemType(Enum):
    """Enumerates the types of items that a used to group items in a filter or a query.

    e.g. in the object-builder syntax: QueryItemType.DIMENSION refers to `Dimension(...)`.

    The value of the enum is the name of the builder "object".
    """

    DIMENSION = "Dimension"
    TIME_DIMENSION = "TimeDimension"
    ENTITY = "Entity"
    METRIC = "Metric"


class ObjectBuilderMethod(Enum):
    """In the object builder notation, the possible methods that can be called on the builder object.

    e.g. ObjectBuilderMethod.GRAIN refers to `.grain` in `Dimension(...).grain('month')`

    The value of the enum is the name of the method.
    """

    GRAIN = "grain"
    DATE_PART = "date_part"
    DESCENDING = "descending"
