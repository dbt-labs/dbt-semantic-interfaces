from typing import List, Protocol

from dbt_semantic_interfaces.protocols.metric import Metric
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel


class SemanticManifest(Protocol):
    """Semantic Manifest holds all the information a SemanticLayer needs to render a query."""

    semantic_models: List[SemanticModel]
    metrics: List[Metric]
