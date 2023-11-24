from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import model_validator

from dbt_semantic_interfaces.implementations.base import HashableBaseModel


class PydanticSemanticVersion(HashableBaseModel):
    """Pydantic implementation of SemanticVersion."""

    major_version: str
    minor_version: str
    patch_version: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _create_from_string(cls, input: Any) -> Dict[str, Any]:  # noqa: D
        if isinstance(input, str):
            version_str_split = input.split(".")
            if len(version_str_split) < 2:
                raise ValueError(f"Expected version string to be of the form x.y or x.y.z, but got {input}")
            return {
                "major_version": version_str_split[0],
                "minor_version": version_str_split[1],
                "patch_version": ".".join(version_str_split[2:]) if len(version_str_split) >= 3 else None,
            }
        elif not isinstance(input, dict):
            raise ValueError(
                f"Expected input to be of type string or Dict[str, Any], but got type {type(input)} with value: {input}"
            )
        return input


UNKNOWN_VERSION_SENTINEL = PydanticSemanticVersion(major_version="0", minor_version="0", patch_version="0")
