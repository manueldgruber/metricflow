from __future__ import annotations

from enum import Enum


class ParameterType(Enum):
    """Supported scalar parameter types for parameterized metrics."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ENUM = "enum"
