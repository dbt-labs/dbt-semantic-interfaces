"""Tests various where filter parsing conditions.

WhereFilter parsing operations can be fairly complex, as they must be able to accept input that is
either a bare string filter expression or some partially or fully deserialized filter object type.

In addition, due to the migration from WhereFilter to WhereFilterIntersection types, this tests the
various conversion operations we will need to perform on semantic manifests defined out in the world.

This module tests the various combinations we might encounter in the wild, with a particular focus
on inputs to parse_obj or parse_raw, as that is what the pydantic models will generally encounter.
"""


from dbt_semantic_interfaces.implementations.base import HashableBaseModel
from dbt_semantic_interfaces.implementations.filters.where_filter import (
    PydanticWhereFilter,
    PydanticWhereFilterIntersection,
)

__BOOLEAN_EXPRESSION__ = "1 > 0"


class ModelWithWhereFilter(HashableBaseModel):
    """Defines a test model to allow for evaluation of different parsing modes for where filter expressions."""

    where_filter: PydanticWhereFilter


class ModelWithWhereFilterIntersection(HashableBaseModel):
    """Defines a test model to allow for evaluation of different parsing modes for where filter intersections.

    This has the same schema, apart from the filter type, as the ModelWithWhereFilter in order to allow for
    testing conversion from a WhereFilter to a WhereFilterIntersection.
    """

    where_filter: PydanticWhereFilterIntersection


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


def test_conversion_from_partially_deserialized_where_filter_string() -> None:
    """Tests converting a partially deserialized ModelWithWhereFilter into a ModelWithWhereFilterIntersection.

    This covers the case where the input is still a bare string, such as might happen in a raw YAML read.
    """
    obj = {"where_filter": __BOOLEAN_EXPRESSION__}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_conversion_from_partially_deserialized_where_filter_object() -> None:
    """Tests converting a partially deserialized WhereFilter into a WhereFilterIntersection."""
    obj = {"where_filter": {"where_sql_template": __BOOLEAN_EXPRESSION__}}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_conversion_from_injected_where_filter_object() -> None:
    """Tests conversion from a PydanticWhereFilter instance, such as one inserted via a raw validator."""
    obj = {"where_filter": PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)}
    expected_conversion_output = PydanticWhereFilterIntersection(
        where_filters=[PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__)]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_conversion_output


def test_where_filter_intersection_from_partially_deserialized_list_of_strings() -> None:
    """Tests parsing a PydanticWhereFilterIntersection when the input is a list of strings.

    This simulates handling YAML input, which may be a list or other sequence of filters.
    """
    obj = {"where_filter": [__BOOLEAN_EXPRESSION__, "0 < 1"]}
    expected_parsed_output = PydanticWhereFilterIntersection(
        where_filters=[
            PydanticWhereFilter(where_sql_template=__BOOLEAN_EXPRESSION__),
            PydanticWhereFilter(where_sql_template="0 < 1"),
        ]
    )

    parsed_model = ModelWithWhereFilterIntersection.parse_obj(obj)

    assert parsed_model.where_filter == expected_parsed_output
