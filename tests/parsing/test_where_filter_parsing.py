"""Tests various where filter parsing conditions.

WhereFilter parsing operations can be fairly complex, as they must be able to accept input that is
either a bare string filter expression or some partially or fully deserialized filter object type.

This module tests the various combinations we might encounter in the wild, with a particular focus
on inputs to parse_obj or parse_raw, as that is what the pydantic models will generally encounter.
"""


from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
)

__BOOLEAN_EXPRESSION__ = "1 > 0"


class ModelWithWhereFilter(HashableBaseModel):
    """Defines a test model to allow for evaluation of different parsing modes for where filter expressions."""

    where_filter: PydanticWhereFilter


def test_partially_deserialized_object_string_parsing() -> None:
    """Tests parsing a where filter specified as a string within partially deserialized json object."""
    obj = {"where_filter": __BOOLEAN_EXPRESSION__}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_partially_deserialized_object_parsing() -> None:
    """Tests parsing a where filter that was serialized and then json decoded, but not fully parsed."""
    obj = {"where_filter": {"where_sql_template": __BOOLEAN_EXPRESSION__}}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_injected_object_parsing() -> None:
    """Tests parsing where, for some reason, a PydanticWhereFilter has been injected into the object.

    This covers the (hopefully vanishingly rare) cases where some raw validator in a pydantic implementation
    is updating the input object to convert something to a PydanticWhereFilter.
    """
    obj = {"where_filter": PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)}

    parsed_model = ModelWithWhereFilter.parse_obj(obj)

    assert parsed_model.where_filter == PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)


def test_serialize_deserialize_operations() -> None:
    """Tests serializing and deserializing an object with a WhereFilter.

    This should cover the most common scenarios, where we need to parse a serialized SemanticManifest.
    """
    base_obj = ModelWithWhereFilter(where_filter=PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__))

    serialized = base_obj.json()
    deserialized = ModelWithWhereFilter.parse_raw(serialized)

    assert deserialized == base_obj
