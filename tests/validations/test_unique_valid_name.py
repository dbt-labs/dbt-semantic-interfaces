from copy import deepcopy

import more_itertools
import pytest

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.test_utils import find_semantic_model_with
from dbt_semantic_interfaces.validations.semantic_manifest_validator import (
    SemanticManifestValidator,
)
from dbt_semantic_interfaces.validations.unique_valid_name import (
    MetricFlowReservedKeywords,
    PrimaryEntityDimensionPairs,
    UniqueAndValidNameRule,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    SemanticManifestValidationException,
)

"""
    Top Level Tests
    Top level elements include
    - Semantic Models
    - Metrics
    - PydanticMetric Sets
    - Dimension Sets

    A name for any of these elements must be unique to all other top level elements
    except metrics. PydanticMetric names only need to be unique in comparison to other metric
    names.
"""


def test_duplicate_semantic_model_name(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    duplicated_semantic_model = model.semantic_models[0]
    model.semantic_models.append(duplicated_semantic_model)
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Can't use name `{duplicated_semantic_model.name}` for a semantic model when it was already used for "
        "a semantic model",
    ):
        SemanticManifestValidator[PydanticSemanticManifest](
            [UniqueAndValidNameRule[PydanticSemanticManifest]()]
        ).checked_validations(model)


def test_duplicate_metric_name(  # noqa:D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)
    duplicated_metric = model.metrics[0]
    model.metrics.append(duplicated_metric)
    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"Can't use name `{duplicated_metric.name}` for a metric when it was already used for a metric",
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([UniqueAndValidNameRule()]).checked_validations(model)


def test_top_level_metric_can_have_same_name_as_any_other_top_level_item(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    metric_name = simple_semantic_manifest__with_primary_transforms.metrics[0].name

    model_semantic_model = deepcopy(simple_semantic_manifest__with_primary_transforms)

    model_semantic_model.semantic_models[0].name = metric_name

    SemanticManifestValidator[PydanticSemanticManifest]([UniqueAndValidNameRule()]).checked_validations(
        model_semantic_model
    )


"""
    Semantic Model Element Tests
    There are three types of semantic model elements
    - measures
    - entities
    - dimensions

    A name for any of these elements must be unique to all other element names
    for the given semantic model.
"""


def test_duplicate_measure_name(  # noqa:D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)

    # Ensure we have a usable semantic model for the test
    semantic_model_with_measures, _ = find_semantic_model_with(
        model, lambda semantic_model: len(semantic_model.measures) > 0
    )

    duplicated_measure = semantic_model_with_measures.measures[0]
    duplicated_measures_tuple = (semantic_model_with_measures.measures, (duplicated_measure,))
    semantic_model_with_measures.measures = tuple(more_itertools.flatten(duplicated_measures_tuple))

    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"can't use name `{duplicated_measure.reference.element_name}` for a measure when it was already used "
        "for a measure",
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([UniqueAndValidNameRule()]).checked_validations(model)


def test_duplicate_dimension_name(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)

    # Ensure we have a usable semantic model for the test
    semantic_model_with_dimensions, _ = find_semantic_model_with(
        model, lambda semantic_model: len(semantic_model.dimensions) > 0
    )

    duplicated_dimension = semantic_model_with_dimensions.dimensions[0]
    duplicated_dimensions_tuple = (semantic_model_with_dimensions.dimensions, (duplicated_dimension,))
    semantic_model_with_dimensions.dimensions = tuple(more_itertools.flatten(duplicated_dimensions_tuple))

    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"can't use name `{duplicated_dimension.reference.element_name}` for a "
        rf"dimension when it was already used for a dimension",
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([UniqueAndValidNameRule()]).checked_validations(model)


def test_duplicate_entity_name(  # noqa:D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    model = deepcopy(simple_semantic_manifest__with_primary_transforms)

    # Ensure we have a usable semantic model for the test
    semantic_model_with_entities, _ = find_semantic_model_with(
        model, lambda semantic_model: len(semantic_model.entities) > 0
    )

    duplicated_entity = semantic_model_with_entities.entities[0]
    duplicated_entities_tuple = (semantic_model_with_entities.entities, (duplicated_entity,))
    semantic_model_with_entities.entities = tuple(more_itertools.flatten(duplicated_entities_tuple))

    with pytest.raises(
        SemanticManifestValidationException,
        match=rf"can't use name `{duplicated_entity.reference.element_name}` for a entity when it was already used "
        "for a entity",
    ):
        SemanticManifestValidator[PydanticSemanticManifest]([UniqueAndValidNameRule()]).checked_validations(model)


"""
    Test PrimaryEntityDimensionPairs rule
"""


def test_primary_entity_dimension_pairs_without_issues(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    validator = SemanticManifestValidator[PydanticSemanticManifest]([PrimaryEntityDimensionPairs()])
    results = validator.validate_semantic_manifest(simple_semantic_manifest__with_primary_transforms)
    assert not results.has_blocking_issues


def test_primary_entity_dimension_pairs_with_issues(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> None:
    manifest = deepcopy(simple_semantic_manifest__with_primary_transforms)
    semantic_model_with_dimensions, _ = find_semantic_model_with(
        manifest, lambda semantic_model: len(semantic_model.dimensions) > 0
    )
    duplicate_semantic_model = deepcopy(semantic_model_with_dimensions)
    duplicate_semantic_model.name = "semantic_model_causes_duplicate_dimension_primary_entity_pairs"
    manifest.semantic_models.append(duplicate_semantic_model)

    validator = SemanticManifestValidator[PydanticSemanticManifest]([PrimaryEntityDimensionPairs()])
    results = validator.validate_semantic_manifest(manifest)
    assert results.has_blocking_issues


"""
    Tests for valid naming
"""


def test_name_is_valid() -> None:  # noqa:D
    assert UniqueAndValidNameRule.check_valid_name("this_is_valid1") == []


def test_invalid_names() -> None:  # noqa:D
    # This is non-exhaustive, but covers some "common" error cases
    assert UniqueAndValidNameRule.check_valid_name("this is invalid") != []
    assert UniqueAndValidNameRule.check_valid_name("100%invalid") != []
    assert UniqueAndValidNameRule.check_valid_name("no-hyphens-allowed") != []
    assert UniqueAndValidNameRule.check_valid_name("punctuation.is.bad") != []
    assert UniqueAndValidNameRule.check_valid_name("cantwildstarme*") != []
    assert UniqueAndValidNameRule.check_valid_name("noemails@hellowdotcom") != []
    assert UniqueAndValidNameRule.check_valid_name("(no)(ordering)(operations)") != []
    assert UniqueAndValidNameRule.check_valid_name("path/to/invalid/name") != []
    assert UniqueAndValidNameRule.check_valid_name("#notwitterhere") != []
    assert UniqueAndValidNameRule.check_valid_name("_no_leading_underscore") != []
    assert UniqueAndValidNameRule.check_valid_name("no_trailing_underscore_") != []
    assert UniqueAndValidNameRule.check_valid_name("_definitely_no_leading_and_trailing_underscore_") != []
    assert UniqueAndValidNameRule.check_valid_name("name__with__dunders") != []

    # time granularity values are reserved
    assert UniqueAndValidNameRule.check_valid_name("day") != []
    assert UniqueAndValidNameRule.check_valid_name("week") != []
    assert UniqueAndValidNameRule.check_valid_name("month") != []
    assert UniqueAndValidNameRule.check_valid_name("quarter") != []
    assert UniqueAndValidNameRule.check_valid_name("year") != []


def test_reserved_name() -> None:  # noqa: D
    reserved_keyword = MetricFlowReservedKeywords.METRIC_TIME
    reserved_reason = MetricFlowReservedKeywords.get_reserved_reason(reserved_keyword)
    issues = UniqueAndValidNameRule.check_valid_name(reserved_keyword.value.lower())
    match = False
    for issue in issues:
        if issue.message.find(reserved_reason) != -1:
            match = True
    assert (
        match
    ), f"Did not find reason: '{reserved_reason}' in issues: {issues} for name: '{reserved_keyword.value.lower()}'"
