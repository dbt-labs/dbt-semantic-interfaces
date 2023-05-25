import logging

from dbt_semantic_interfaces.protocol_example import (
    ExplicitPlainSemanticManifest,
    ImplicitDataclassSemanticManifest,
    ImplicitDataclassSemanticModel,
    ImplicitPlainSemanticManifest,
    ImplicitPydanticSemanticManifest,
    ImplicitPydanticSemanticModel,
    InvalidImplicitPydanticSemanticManifest,
    check_function,
)

logger = logging.getLogger(__name__)


def test_type_checking() -> None:  # noqa: D
    # No type / runtime errors.
    check_function(ImplicitPydanticSemanticManifest(semantic_models=(ImplicitPydanticSemanticModel(name="bar"),)))
    check_function(ImplicitDataclassSemanticManifest(semantic_models=(ImplicitDataclassSemanticModel(name="bazz"),)))
    check_function(ExplicitPlainSemanticManifest())
    check_function(ImplicitPlainSemanticManifest())

    # Has type / runtime errors.
    check_function(InvalidImplicitPydanticSemanticManifest(class_type_check_error="baz"))
