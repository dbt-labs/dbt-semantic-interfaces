from typing import List

from importlib_metadata import version
from pydantic import validator

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.metric import PydanticMetric
from dbt_semantic_interfaces.implementations.semantic_model import PydanticSemanticModel


class PydanticSemanticManifest(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query."""

    semantic_models: List[PydanticSemanticModel]
    metrics: List[PydanticMetric]
    interfaces_version: str = ""

    @validator("interfaces_version", always=True)
    @classmethod
    def __create_default_interfaces_version(cls, value: str) -> str:  # type: ignore[misc]
        """Returns the version of the dbt_semantic_interfaces package that generated this manifest."""
        if value:
            return value
        return version("dbt_semantic_interfaces")
