from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

from dbt_semantic_interfaces.references import (
    DimensionReference,
    EntityReference,
    LinkableElementReference,
    MetricReference,
    TimeDimensionReference,
)
from dbt_semantic_interfaces.type_enums import TimeGranularity
from dbt_semantic_interfaces.type_enums.date_part import DatePart


@dataclass(frozen=True)
class DimensionCallParameterSet:
    """When 'Dimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    dimension_reference: DimensionReference


@dataclass(frozen=True)
class TimeDimensionCallParameterSet:
    """When 'TimeDimension(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    time_dimension_reference: TimeDimensionReference
    time_granularity: Optional[TimeGranularity] = None
    date_part: Optional[DatePart] = None


@dataclass(frozen=True)
class EntityCallParameterSet:
    """When 'Entity(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    entity_path: Tuple[EntityReference, ...]
    entity_reference: EntityReference


@dataclass(frozen=True)
class MetricCallParameterSet:
    """When 'Metric(...)' is used in the Jinja template of the where filter, the parameters to that call."""

    metric_reference: MetricReference
    group_by: Tuple[LinkableElementReference, ...]


@dataclass(frozen=True)
class FilterCallParameterSets:
    """The calls for metric items made in the Jinja template of the where filter."""

    dimension_call_parameter_sets: Tuple[DimensionCallParameterSet, ...] = ()
    time_dimension_call_parameter_sets: Tuple[TimeDimensionCallParameterSet, ...] = ()
    entity_call_parameter_sets: Tuple[EntityCallParameterSet, ...] = ()
    metric_call_parameter_sets: Tuple[MetricCallParameterSet, ...] = ()


class ParseWhereFilterException(Exception):  # noqa: D
    pass
