from __future__ import annotations

import datetime
import logging
import os
import uuid
from typing import Dict

import pytest

from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticDimensionInput,
    PydanticEntityInput,
    PydanticTimeDimensionInput,
    PydanticWhereFilter,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import (
    PydanticSemanticManifest,
)
from dbt_semantic_interfaces.jinja_compilers import WhereFilterCompiler
from dbt_semantic_interfaces.parsing.dir_to_model import (
    parse_directory_of_yaml_files_to_semantic_manifest,
)
from dbt_semantic_interfaces.transformations.pydantic_rule_set import (
    PydanticSemanticManifestTransformRuleSet,
)
from dbt_semantic_interfaces.transformations.semantic_manifest_transformer import (
    PydanticSemanticManifestTransformer,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def template_mapping() -> Dict[str, str]:
    """Mapping for template variables in the semantic manifest YAML files."""
    current_time = datetime.datetime.now().strftime("%Y_%m_%d")
    random_suffix = uuid.uuid4()
    system_schema = f"mf_test_{current_time}_{random_suffix}"
    return {"source_schema": system_schema}


@pytest.fixture(scope="session")
def simple_semantic_manifest(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Semantic Manifest used for many tests."""
    model_build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "semantic_manifest_yamls/simple_semantic_manifest"),
        template_mapping=template_mapping,
    )
    return model_build_result.semantic_manifest


@pytest.fixture(scope="session")
def simple_semantic_manifest__with_primary_transforms(template_mapping: Dict[str, str]) -> PydanticSemanticManifest:
    """Semantic Manifest used for tests pre-transformations."""
    model_build_result = parse_directory_of_yaml_files_to_semantic_manifest(
        os.path.join(os.path.dirname(__file__), "semantic_manifest_yamls/simple_semantic_manifest"),
        template_mapping=template_mapping,
        apply_transformations=False,
    )
    transformed_model = PydanticSemanticManifestTransformer.transform(
        model=model_build_result.semantic_manifest,
        ordered_rule_sequences=(PydanticSemanticManifestTransformRuleSet().primary_rules,),
    )
    return transformed_model


PydanticWhereFilterCompilerT = WhereFilterCompiler[
    PydanticWhereFilter, PydanticDimensionInput, PydanticTimeDimensionInput, PydanticEntityInput
]


@pytest.fixture(scope="session")
def pydantic_where_filter_compiler() -> PydanticWhereFilterCompilerT:  # noqa: D
    return WhereFilterCompiler[
        PydanticWhereFilter, PydanticDimensionInput, PydanticTimeDimensionInput, PydanticEntityInput
    ](
        where_filter_class=PydanticWhereFilter,
        dimension_input_class=PydanticDimensionInput,
        time_dimension_input_class=PydanticTimeDimensionInput,
        entity_input_class=PydanticEntityInput,
    )
