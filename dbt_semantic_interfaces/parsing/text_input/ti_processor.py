from __future__ import annotations

from abc import ABC, abstractmethod
from textwrap import indent
from typing import List, Sequence

from jinja2 import StrictUndefined, TemplateSyntaxError, UndefinedError
from jinja2.exceptions import SecurityError
from jinja2.sandbox import SandboxedEnvironment
from typing_extensions import override

from dbt_semantic_interfaces.errors import InvalidQuerySyntax
from dbt_semantic_interfaces.parsing.text_input.description_renderer import (
    QueryItemDescriptionRenderer,
)
from dbt_semantic_interfaces.parsing.text_input.rendering_helper import (
    ObjectBuilderJinjaRenderHelper,
)
from dbt_semantic_interfaces.parsing.text_input.ti_description import (
    QueryItemDescription,
)
from dbt_semantic_interfaces.parsing.text_input.ti_exceptions import (
    QueryItemJinjaException,
)
from dbt_semantic_interfaces.parsing.text_input.valid_method import ValidMethodMapping


class QueryItemTextProcessor:
    """Performs processing actions for text containing query items specified in the object-builder syntax.

    This currently supports:
    * Collecting `QueryItemDescription`s from a Jinja template.
    * Rendering a Jinja template using a specified renderer.
    """

    def get_description(self, query_item_input: str, valid_method_mapping: ValidMethodMapping) -> QueryItemDescription:
        """Get the `QueryItemDescription` for a single item e.g. `Dimension('listing__country').descending(True)`."""
        descriptions = self.collect_descriptions_from_template(
            jinja_template="{{ " + query_item_input + " }}",
            valid_method_mapping=valid_method_mapping,
        )
        if len(descriptions) != 1:
            raise InvalidQuerySyntax(
                f"Did not get exactly one query item from: {query_item_input!r} Got: {descriptions}"
            )
        return descriptions[0]

    def collect_descriptions_from_template(
        self,
        jinja_template: str,
        valid_method_mapping: ValidMethodMapping,
    ) -> Sequence[QueryItemDescription]:
        """Returns the `QueryItemDescription`s that are found in a Jinja template.

        Args:
            jinja_template: A Jinja-template string like `{{ Dimension('listing__country') }} = 'US'`.
            valid_method_mapping: Mapping from the builder object to the valid methods. See
            `ConfiguredValidMethodMapping`.

        Returns:
            A sequence of the descriptions found in the template.

        Raises:
            QueryItemJinjaException: See definition.
            InvalidBuilderMethodException: See definition.
        """
        description_collector = _CollectDescriptionProcessor()
        self._process_template(
            jinja_template=jinja_template,
            valid_method_mapping=valid_method_mapping,
            description_processor=description_collector,
        )
        return description_collector.collected_descriptions()

    def render_template(
        self,
        jinja_template: str,
        renderer: QueryItemDescriptionRenderer,
        valid_method_mapping: ValidMethodMapping,
    ) -> str:
        """Renders the Jinja template using the specified renderer.

        Args:
            jinja_template: A Jinja template string like `{{ Dimension('listing__country') }} = 'US'`.
            renderer: The renderer to use for rendering each item.
            valid_method_mapping: Mapping from the builder object to the valid methods. See
            `ConfiguredValidMethodMapping`.

        Returns:
            The rendered Jinja template.

        Raises:
            QueryItemJinjaException: See definition.
            InvalidBuilderMethodException: See definition.
        """
        render_processor = _RendererProcessor(renderer)
        return self._process_template(
            jinja_template=jinja_template,
            valid_method_mapping=valid_method_mapping,
            description_processor=render_processor,
        )

    def _process_template(
        self,
        jinja_template: str,
        valid_method_mapping: ValidMethodMapping,
        description_processor: QueryItemDescriptionProcessor,
    ) -> str:
        """Helper to run a `QueryItemDescriptionProcessor` on a Jinja template."""
        render_helper = ObjectBuilderJinjaRenderHelper(
            description_processor=description_processor,
            valid_method_mapping=valid_method_mapping,
        )
        try:
            # the string that the sandbox renders is unused
            rendered = (
                SandboxedEnvironment(undefined=StrictUndefined)
                .from_string(jinja_template)
                .render(
                    Dimension=render_helper.get_function_for_dimension(),
                    TimeDimension=render_helper.get_function_for_time_dimension(),
                    Entity=render_helper.get_function_for_entity(),
                    Metric=render_helper.get_function_for_metric(),
                )
            )
        except (UndefinedError, TemplateSyntaxError, SecurityError) as e:
            raise QueryItemJinjaException(
                f"Error while processing Jinja template:" f"\n{indent(jinja_template, prefix='    ')}"
            ) from e

        return rendered


class QueryItemDescriptionProcessor(ABC):
    """General processor that does something to a query-item description seen in a Jinja template."""

    @abstractmethod
    def process_description(self, item_description: QueryItemDescription) -> str:
        """Process the given description, and return a string that would be substituted into the Jinja template."""
        raise NotImplementedError


class _CollectDescriptionProcessor(QueryItemDescriptionProcessor):
    """Processor that collects all descriptions that were processed."""

    def __init__(self) -> None:  # noqa: D107
        self._items: List[QueryItemDescription] = []

    def collected_descriptions(self) -> Sequence[QueryItemDescription]:
        """Return all descriptions that were processed so far."""
        return self._items

    @override
    def process_description(self, item_description: QueryItemDescription) -> str:
        if item_description not in self._items:
            self._items.append(item_description)

        return ""


class _RendererProcessor(QueryItemDescriptionProcessor):
    """Processor that renders the descriptions in a Jinja template using the given renderer.

    This is just a pass-through, but it allows `QueryItemDescriptionRenderer` to be a facade that has more appropriate
    method names.
    """

    def __init__(self, renderer: QueryItemDescriptionRenderer) -> None:  # noqa: D107
        self._renderer = renderer

    @override
    def process_description(self, item_description: QueryItemDescription) -> str:
        return self._renderer.render_description(item_description)
