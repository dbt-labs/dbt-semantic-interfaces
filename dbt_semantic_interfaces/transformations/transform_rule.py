from abc import ABC, abstractmethod

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)


class ModelTransformRule(ABC):
    """Encapsulates logic for transforming a model. e.g. add metrics based on measures."""

    @staticmethod
    @abstractmethod
    def transform_model(model: PydanticSemanticManifest) -> PydanticSemanticManifest:
        """Copy and transform the given model into a new model."""
        pass
