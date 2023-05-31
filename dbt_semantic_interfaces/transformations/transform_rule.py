from __future__ import annotations

from abc import abstractmethod
from typing import Protocol, TypeVar

from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifestT


class SemanticManifestTransformRule(Protocol[SemanticManifestT]):
    """Encapsulates logic for transforming a model. e.g. add metrics based on measures."""

    @abstractmethod
    def transform_model(self, semantic_manifest: SemanticManifestT) -> SemanticManifestT:
        """Copy and transform the given model into a new model."""
        pass


SemanticManifestTransformRuleT = TypeVar("SemanticManifestTransformRuleT", bound=SemanticManifestTransformRule)
SemanticManifestTransformRuleT_co = TypeVar(
    "SemanticManifestTransformRuleT_co", bound=SemanticManifestTransformRule, covariant=True
)
