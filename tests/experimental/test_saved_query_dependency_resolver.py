import pytest

from dbt_semantic_interfaces.experimental.saved_query_dependency_resolver import (
    SavedQueryDependencyResolver,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.references import (
    SavedQueryReference,
    SemanticModelReference,
)


@pytest.fixture(scope="session")
def resolver(  # noqa: D
    simple_semantic_manifest__with_primary_transforms: PydanticSemanticManifest,
) -> SavedQueryDependencyResolver:
    return SavedQueryDependencyResolver(simple_semantic_manifest__with_primary_transforms)


def test_saved_query_dependency_resolver(resolver: SavedQueryDependencyResolver) -> None:  # noqa: D
    dependency_set = resolver.resolve_dependencies(SavedQueryReference("p0_booking"))
    assert tuple(dependency_set.semantic_model_references) == (
        SemanticModelReference(semantic_model_name="accounts_source"),
        SemanticModelReference(semantic_model_name="bookings_source"),
        SemanticModelReference(semantic_model_name="companies"),
        SemanticModelReference(semantic_model_name="id_verifications"),
        SemanticModelReference(semantic_model_name="listings_latest"),
        SemanticModelReference(semantic_model_name="lux_listing_mapping"),
        SemanticModelReference(semantic_model_name="revenue"),
        SemanticModelReference(semantic_model_name="users_ds_source"),
        SemanticModelReference(semantic_model_name="users_latest"),
        SemanticModelReference(semantic_model_name="views_source"),
    )
