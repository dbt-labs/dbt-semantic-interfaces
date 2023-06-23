from importlib_metadata import version

from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.implementations.semantic_version import (
    PydanticSemanticVersion,
)
from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATIONS


def test_interfaces_version_matches() -> None:
    """Test that the interfaces_version property returns the installed version of dbt_semantic_interfaces."""
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[],
        project_configurations=EXAMPLE_PROJECT_CONFIGURATIONS,
    )

    # get the actual installed version
    installed_version = version("dbt_semantic_interfaces")
    assert len(semantic_manifest.project_configurations) == 1
    assert semantic_manifest.project_configurations[
        0
    ].dsi_package_version == PydanticSemanticVersion.create_from_string(installed_version)
