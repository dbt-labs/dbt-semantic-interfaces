from __future__ import annotations

from enum import Enum
from typing import Any, List, Optional

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dsi_pydantic_shim import Field


class OSIDialect(str, Enum):
    """Supported SQL and expression language dialects."""

    ANSI_SQL = "ANSI_SQL"
    SNOWFLAKE = "SNOWFLAKE"
    MDX = "MDX"
    TABLEAU = "TABLEAU"
    DATABRICKS = "DATABRICKS"


class OSIDialectExpression(HashableBaseModel):
    """Expression in a specific dialect."""

    dialect: OSIDialect
    expression: str


class OSIExpression(HashableBaseModel):
    """Expression definition with multi-dialect support."""

    dialects: List[OSIDialectExpression]


class OSIDimension(HashableBaseModel):
    """Dimension metadata on a field."""

    is_time: bool


class OSIField(HashableBaseModel):
    """Row-level attribute for grouping, filtering, and metric expressions."""

    name: str
    expression: OSIExpression
    dimension: Optional[OSIDimension] = None
    label: Optional[str] = None
    description: Optional[str] = None


class OSIDataset(HashableBaseModel):
    """Logical dataset representing a business entity (fact or dimension table)."""

    name: str
    source: str
    primary_key: Optional[List[str]] = None
    unique_keys: Optional[List[List[str]]] = None
    description: Optional[str] = None
    fields: Optional[List[OSIField]] = None


class OSIRelationship(HashableBaseModel):
    """Foreign key relationship between datasets."""

    name: str
    from_dataset: str = Field(..., alias="from")
    to: str
    from_columns: List[str]
    to_columns: List[str]

    class Config:  # noqa: D
        allow_population_by_field_name = True


class OSIMetric(HashableBaseModel):
    """Quantitative measure defined on business data (stub -- not yet populated by converter)."""

    name: str
    expression: OSIExpression
    description: Optional[str] = None


class OSISemanticModel(HashableBaseModel):
    """Top-level container representing a complete semantic model."""

    name: str
    description: Optional[str] = None
    datasets: List[OSIDataset]
    relationships: Optional[List[OSIRelationship]] = None
    metrics: Optional[List[OSIMetric]] = None


class OSIDocument(HashableBaseModel):
    """Root OSI document."""

    version: str = "1.0"
    dialects: Optional[List[OSIDialect]] = None
    semantic_model: List[OSISemanticModel]

    def to_osi_json(self, **kwargs: Any) -> str:
        """Serialize to OSI-compliant JSON (uses field aliases like 'from' and excludes None values)."""
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return self.json(**kwargs)
