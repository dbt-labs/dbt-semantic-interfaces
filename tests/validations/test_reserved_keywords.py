import random
from copy import deepcopy

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_model import PydanticNodeRelation
from dbt_semantic_interfaces.test_utils import find_semantic_model_with
from dbt_semantic_interfaces.validations.reserved_keywords import (
    RESERVED_KEYWORDS,
    ReservedKeywordsRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import ValidationIssueLevel


def random_keyword() -> str:  # noqa: D
    return random.choice(RESERVED_KEYWORDS)


def test_no_reserved_keywords(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    issues = ReservedKeywordsRule.validate_manifest(simple_semantic_manifest__with_primary_transforms)
    assert len(issues) == 0


def test_reserved_keywords_in_dimensions(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.dimensions) > 0
    )
    dimension = semantic_model.dimensions[0]
    dimension.name = random_keyword()

    issues = ReservedKeywordsRule.validate_manifest(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_measures(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.measures) > 0
    )
    measure = semantic_model.measures[0]
    measure.name = random_keyword()

    issues = ReservedKeywordsRule.validate_manifest(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_entities(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    (semantic_model, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: len(semantic_model.entities) > 0
    )
    entity = semantic_model.entities[0]
    entity.name = random_keyword()

    issues = ReservedKeywordsRule.validate_manifest(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR


def test_reserved_keywords_in_node_relation(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    (semantic_model_with_node_relation, _index) = find_semantic_model_with(
        model=model, function=lambda semantic_model: semantic_model.node_relation is not None
    )
    semantic_model_with_node_relation.node_relation = PydanticNodeRelation(
        alias=random_keyword(),
        schema_name="some_schema",
    )
    issues = ReservedKeywordsRule.validate_manifest(model)
    assert len(issues) == 1
    assert issues[0].level == ValidationIssueLevel.ERROR
