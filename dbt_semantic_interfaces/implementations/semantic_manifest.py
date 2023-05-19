from typing import List

from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.metric import Metric
from dbt_semantic_interfaces.implementations.semantic_model import SemanticModel


class SemanticManifest(HashableBaseModel):
    """Model holds all the information the SemanticLayer needs to render a query."""

    semantic_models: List[SemanticModel]
    metrics: List[Metric]
