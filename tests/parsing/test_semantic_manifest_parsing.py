from importlib_metadata import version

from dbt_semantic_interfaces.objects.semantic_manifest import SemanticManifest


def test_interfaces_version() -> None:
    """Test that the interfaces_version property returns the installed version of dbt_semantic_interfaces."""
    semantic_manifest = SemanticManifest(
        semantic_models=[],
        metrics=[],
    )

    # get the actual installed version
    installed_version = version("dbt_semantic_interfaces")
    assert semantic_manifest.interfaces_version == installed_version
