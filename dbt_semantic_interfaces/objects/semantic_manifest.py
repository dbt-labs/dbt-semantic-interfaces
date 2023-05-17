from typing import List

from importlib_metadata import PackageNotFoundError, version

from dbt_semantic_interfaces.objects.base import HashableBaseModel
from dbt_semantic_interfaces.objects.metric import Metric
from dbt_semantic_interfaces.objects.semantic_model import SemanticModel


class SemanticManifest(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query."""

    semantic_models: List[SemanticModel]
    metrics: List[Metric]

    @property
    def interfaces_version(self) -> str:
        """Returns the version of the dbt_semantic_interfaces package that generated this manifest."""
        try:
            return version("dbt_semantic_interfaces")
        except PackageNotFoundError:
            return "dbt_semantic_interfaces is not installed"
