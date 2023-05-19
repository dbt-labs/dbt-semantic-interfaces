from typing import List

from importlib_metadata import version
from pydantic import validator

from dbt_semantic_interfaces.objects.base import HashableBaseModel
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel


class SemanticManifest(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query."""

    semantic_models: List[SemanticModel]
    metrics: List[Metric]
    interfaces_version: str = ""

    @validator("interfaces_version", always=True)
    @classmethod
    def __create_default_interfaces_version(cls, value: str) -> str:  # type: ignore[misc]
        """Returns the version of the dbt_semantic_interfaces package that generated this manifest."""
        if value:
            return value
        return version("dbt_semantic_interfaces")
