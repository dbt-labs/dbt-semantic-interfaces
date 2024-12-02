from __future__ import annotations

import enum
import re
from typing import Dict, Generic, List, Optional, Sequence, Set, Tuple, Union

from dbt_semantic_interfaces.enum_extension import assert_values_exhausted
from dbt_semantic_interfaces.protocols import (
    Metric,
    SavedQuery,
    SemanticManifest,
    SemanticManifestT,
    SemanticModel,
)
from dbt_semantic_interfaces.references import (
    ElementReference,
    SemanticModelElementReference,
)
from dbt_semantic_interfaces.type_enums import (
    EntityType,
    SemanticManifestNodeType,
    TimeGranularity,
)
from dbt_semantic_interfaces.validations.validator_helpers import (
    FileContext,
    SemanticManifestValidationRule,
    SemanticModelElementContext,
    SemanticModelElementType,
    ValidationContext,
    ValidationError,
    ValidationIssue,
    ValidationIssueContext,
    validate_safely,
)


@enum.unique
class MetricFlowReservedKeywords(enum.Enum):
    """Enumeration of reserved keywords with helper for accessing the reason they are reserved."""

    METRIC_TIME = "metric_time"
    MF_INTERNAL_UUID = "mf_internal_uuid"

    @staticmethod
    def get_reserved_reason(keyword: MetricFlowReservedKeywords) -> str:
        """Get the reason a given keyword is reserved. Guarantees an exhaustive switch."""
        if keyword is MetricFlowReservedKeywords.METRIC_TIME:
            return (
                "Used as the query input for creating time series metrics from measures with "
                "different time dimension names."
            )
        elif keyword is MetricFlowReservedKeywords.MF_INTERNAL_UUID:
            return "Used internally to reference a column that has a uuid generated by MetricFlow."
        else:
            assert_values_exhausted(keyword)


class UniqueAndValidNameRule(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """Check that names are unique and valid.

    * Names of elements in semantic models are unique / valid within the semantic model.
    * Names of semantic models, dimension sets and metric sets in the model are unique / valid.
    """

    # name must start with a lower case letter
    # name must end with a number or lower case letter
    # name may include lower case letters, numbers, and underscores
    # name may not contain dunders (two sequential underscores
    NAME_REGEX = re.compile(r"\A[a-z]((?!__)[a-z0-9_])*[a-z0-9]\Z")

    @staticmethod
    def check_valid_name(  # noqa: D
        name: str, context: Optional[ValidationContext] = None
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        if not UniqueAndValidNameRule.NAME_REGEX.match(name):
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Invalid name `{name}` - names may only contain lower case letters, numbers, "
                    f"and underscores. Additionally, names must start with a lower case letter, cannot end "
                    f"with an underscore, cannot contain dunders (double underscores, or __), and must be "
                    f"at least 2 characters long.",
                )
            )
        if name.upper() in TimeGranularity.list_names():
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Invalid name `{name}` - names cannot match reserved time granularity keywords "
                    f"({TimeGranularity.list_names()})",
                )
            )
        if name.lower() in {reserved_name.value for reserved_name in MetricFlowReservedKeywords}:
            reason = MetricFlowReservedKeywords.get_reserved_reason(MetricFlowReservedKeywords(name.lower()))
            issues.append(
                ValidationError(
                    context=context,
                    message=f"Invalid name `{name}` - this name is reserved by MetricFlow. Reason: {reason}",
                )
            )
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking semantic model sub element names are unique")
    def _validate_semantic_model_elements_and_time_spines(
        semantic_manifest: SemanticManifest,
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []
        custom_granularity_restricted_names_and_types: Dict[str, str] = {}

        for semantic_model in semantic_manifest.semantic_models:
            element_info_tuples: List[Tuple[ElementReference, str, ValidationContext]] = []
            for measure in semantic_model.measures:
                custom_granularity_restricted_names_and_types[measure.name] = SemanticModelElementType.MEASURE.value
                element_info_tuples.append(
                    (
                        measure.reference,
                        "measure",
                        SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=measure.name
                            ),
                            element_type=SemanticModelElementType.MEASURE,
                        ),
                    )
                )
            for entity in semantic_model.entities:
                custom_granularity_restricted_names_and_types[entity.name] = SemanticModelElementType.ENTITY.value
                element_info_tuples.append(
                    (
                        entity.reference,
                        "entity",
                        SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=entity.name
                            ),
                            element_type=SemanticModelElementType.ENTITY,
                        ),
                    )
                )
            for dimension in semantic_model.dimensions:
                custom_granularity_restricted_names_and_types[dimension.name] = SemanticModelElementType.DIMENSION.value
                element_info_tuples.append(
                    (
                        dimension.reference,
                        "dimension",
                        SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=dimension.name
                            ),
                            element_type=SemanticModelElementType.DIMENSION,
                        ),
                    )
                )

            # Verify uniqueness for this type within each semantic model
            semantic_model_element_reference_to_type: Dict[ElementReference, str] = {}
            for reference, _type, context in element_info_tuples:
                if reference in semantic_model_element_reference_to_type:
                    issues.append(
                        ValidationError(
                            context=context,
                            message=f"In semantic model `{semantic_model.name}`, can't use name "
                            f"`{reference.element_name}` for a {_type} when it was already used for a "
                            f"{semantic_model_element_reference_to_type[reference]}",
                        )
                    )
                else:
                    semantic_model_element_reference_to_type[reference] = _type

            for name, _, context in element_info_tuples:
                issues += UniqueAndValidNameRule.check_valid_name(name=name.element_name, context=context)

        for metric in semantic_manifest.metrics:
            custom_granularity_restricted_names_and_types[metric.name] = SemanticManifestNodeType.METRIC.value
        for semantic_model in semantic_manifest.semantic_models:
            custom_granularity_restricted_names_and_types[
                semantic_model.name
            ] = SemanticManifestNodeType.SEMANTIC_MODEL.value

        # Verify custom granularity names are unique across relevant elements
        seen_custom_granularity_names: Set[str] = set()
        duplicate_custom_granularity_names: Set[str] = set()
        for time_spine in semantic_manifest.project_configuration.time_spines:
            time_spine_context = ValidationIssueContext(
                file_context=FileContext(),
                object_name=time_spine.node_relation.alias,
                object_type=SemanticManifestNodeType.TIME_SPINE.value,
            )
            for custom_granularity in time_spine.custom_granularities:
                issues += UniqueAndValidNameRule.check_valid_name(
                    name=custom_granularity.name, context=time_spine_context
                )
                if custom_granularity.name in custom_granularity_restricted_names_and_types:
                    issues.append(
                        ValidationError(
                            context=time_spine_context,
                            message=f"Can't use name `{custom_granularity.name}` for a custom granularity when it was "
                            "already used for a "
                            f"{custom_granularity_restricted_names_and_types[custom_granularity.name]}.",
                        )
                    )
                if custom_granularity.name in seen_custom_granularity_names:
                    duplicate_custom_granularity_names.add(custom_granularity.name)
                seen_custom_granularity_names.add(custom_granularity.name)

        if duplicate_custom_granularity_names:
            issues.append(
                ValidationError(
                    context=time_spine_context,
                    message=f"Custom granularity names must be unique, but found duplicate custom granularities with "
                    f"the names {duplicate_custom_granularity_names}.",
                )
            )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking top level elements of a specific type have unique and valid names")
    def _validate_top_level_objects_of_type(
        objects: Union[Sequence[SemanticModel], Sequence[Metric], Sequence[SavedQuery]],
        object_type: SemanticManifestNodeType,
    ) -> Sequence[ValidationIssue]:
        """Validates uniqeness and validaty of top level objects of singular type."""
        issues: List[ValidationIssue] = []
        object_names = set()

        for object in objects:
            context = ValidationIssueContext(
                file_context=FileContext.from_metadata(object.metadata),
                object_name=object.name,
                object_type=object_type.value,
            )
            issues += UniqueAndValidNameRule.check_valid_name(name=object.name, context=context)
            if object.name in object_names:
                issues.append(
                    ValidationError(
                        context=context,
                        message=f"Can't use name `{object.name}` for a {object_type} when it was already "
                        f"used for another {object_type}",
                    )
                )
            else:
                object_names.add(object.name)
        return issues

    @staticmethod
    @validate_safely(whats_being_done="checking model top level element names are sufficiently unique")
    def _validate_top_level_objects(semantic_manifest: SemanticManifest) -> Sequence[ValidationIssue]:
        """Checks names of objects that are not nested."""
        issues = list(
            UniqueAndValidNameRule._validate_top_level_objects_of_type(
                semantic_manifest.semantic_models, SemanticManifestNodeType.SEMANTIC_MODEL
            )
        )

        issues.extend(
            UniqueAndValidNameRule._validate_top_level_objects_of_type(
                semantic_manifest.metrics, SemanticManifestNodeType.METRIC
            )
        )

        issues.extend(
            UniqueAndValidNameRule._validate_top_level_objects_of_type(
                semantic_manifest.saved_queries, SemanticManifestNodeType.SAVED_QUERY
            )
        )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="running model validation ensuring elements have adequately unique names")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        issues += UniqueAndValidNameRule._validate_top_level_objects(semantic_manifest=semantic_manifest)
        issues += UniqueAndValidNameRule._validate_semantic_model_elements_and_time_spines(semantic_manifest)

        return issues


class PrimaryEntityDimensionPairs(SemanticManifestValidationRule[SemanticManifestT], Generic[SemanticManifestT]):
    """All dimension + primary entity pairs across the semantic manifest are unique."""

    @staticmethod
    @validate_safely(
        whats_being_done="validating the semantic model doesn't have dimension + primary entity pair conflicts"
    )
    def _check_semantic_model(  # noqa: D
        semantic_model: SemanticModel, known_pairings: Dict[str, Dict[str, str]]
    ) -> Sequence[ValidationIssue]:
        issues: List[ValidationIssue] = []

        primary_entity = semantic_model.primary_entity
        if primary_entity is None:
            for entity in semantic_model.entities:
                if entity.type is EntityType.PRIMARY:
                    primary_entity = entity.name
                    break

        # If primary entity is still none, return early. It's an issue,
        # but not the subject of this validation. This is handled by
        # PrimaryEntityRule
        if primary_entity is None:
            return issues

        safe = False
        if known_pairings.get(primary_entity) is None:
            known_pairings[primary_entity] = {}
            safe = True

        for dimension in semantic_model.dimensions:
            if safe or known_pairings[primary_entity].get(dimension.name) is None:
                known_pairings[primary_entity][dimension.name] = semantic_model.name
            else:
                issues.append(
                    ValidationError(
                        context=SemanticModelElementContext(
                            file_context=FileContext.from_metadata(metadata=semantic_model.metadata),
                            semantic_model_element=SemanticModelElementReference(
                                semantic_model_name=semantic_model.name, element_name=dimension.name
                            ),
                            element_type=SemanticModelElementType.DIMENSION,
                        ),
                        message="Duplicate dimension + primary entity pairing detected, dimension + primary entity "
                        f"pairings must be unique. Semantic model `{semantic_model.name}` has a primary entity of "
                        f"`{primary_entity}` and dimension `{dimension.name}`, but this pairing is already in use on "
                        f"semantic model `{known_pairings[primary_entity][dimension.name]}`.",
                    )
                )

        return issues

    @staticmethod
    @validate_safely(whats_being_done="validating there are no duplicate dimension primary entity pairs")
    def validate_manifest(semantic_manifest: SemanticManifestT) -> Sequence[ValidationIssue]:  # noqa: D
        issues: List[ValidationIssue] = []
        known_pairings: Dict[str, Dict[str, str]] = {}
        for semantic_model in semantic_manifest.semantic_models:
            issues += PrimaryEntityDimensionPairs._check_semantic_model(
                semantic_model=semantic_model, known_pairings=known_pairings
            )

        return issues
