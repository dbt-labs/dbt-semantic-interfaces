from abc import abstractmethod
from typing import Protocol, Sequence, TypeVar

from dbt_semantic_interfaces.protocols.metric import Metric
from dbt_semantic_interfaces.protocols.semantic_model import SemanticModel


class SemanticManifest(Protocol):
    """Semantic Manifest holds all the information a SemanticLayer needs to render a query."""

    @property
    @abstractmethod
    def semantic_models(self) -> Sequence[SemanticModel]:  # noqa: D
        pass

    @property
    @abstractmethod
    def metrics(self) -> Sequence[Metric]:  # noqa: D
        pass

    @property
    @abstractmethod
    def interfaces_version(self) -> str:  # noqa: D
        pass


SemanticManifestT = TypeVar("SemanticManifestT", bound=SemanticManifest)
