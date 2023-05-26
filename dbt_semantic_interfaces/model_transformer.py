import copy
import logging
from typing import Optional, Sequence

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.transformations.pydantic_rule_set import (
    PydanticSemanticManifestTransformRuleSet,
)
from dbt_semantic_interfaces.transformations.transform_rule import (
    SemanticManifestTransformRule,
)

logger = logging.getLogger(__name__)


class PydanticSemanticManifestTransformer:
    """Helps to make transformations to a model for convenience.

    Generally used to make it more convenient for the user to develop their model.
    """

    @staticmethod
    def transform(
        model: PydanticSemanticManifest,
        ordered_rule_sequences: Optional[Sequence[Sequence[SemanticManifestTransformRule]]] = None,
    ) -> PydanticSemanticManifest:
        """Copies the passed in model, applies the rules to the new model, and then returns that model.

        It's important to note that some rules need to happen before or after other rules. Thus rules
        are passed in as an ordered tuple of rule sequences. Primary rules are run first, and then
        secondary rules. We don't currently have tertiary, quaternary, or etc currently, but this
        system easily allows for it.
        """
        if ordered_rule_sequences is None:
            ordered_rule_sequences = PydanticSemanticManifestTransformRuleSet().all_rules

        model_copy = copy.deepcopy(model)

        for rule_sequence in ordered_rule_sequences:
            for rule in rule_sequence:
                model_copy = rule.transform_model(model_copy)

        return model_copy
