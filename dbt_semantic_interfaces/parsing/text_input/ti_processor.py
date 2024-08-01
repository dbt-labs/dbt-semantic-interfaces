from __future__ import annotations

from typing import Sequence

from dbt_semantic_interfaces.parsing.text_input.description_renderer import (
    QueryItemDescriptionRenderer,
)
from dbt_semantic_interfaces.parsing.text_input.ti_description import (
    QueryItemDescription,
)
from dbt_semantic_interfaces.parsing.text_input.valid_method import ValidMethodMapping


class QueryItemTextProcessor:
    """Performs processing actions for text containing query items specified in the object-builder syntax.

    This currently supports:
    * Collecting `QueryItemDescription`s from a Jinja template.
    * Rendering a Jinja template using a specified renderer.
    """

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
        raise NotImplementedError

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
        raise NotImplementedError
