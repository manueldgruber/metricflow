from __future__ import annotations

import copy
import re
from collections.abc import Mapping, Sequence
from typing import Any

from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.protocols.semantic_manifest import SemanticManifest
from dbt_semantic_interfaces.type_enums import ParameterType
from metricflow_semantics.errors.error_classes import InvalidQueryException

PARAMETER_REFERENCE_PATTERN = re.compile(r"\{\{\s*parameter\(\s*'([^']+)'\s*\)\s*\}\}")
MetricParameterValue = str | Mapping[str, str]


def bind_metric_parameters(
    semantic_manifest: SemanticManifest,
    parameter_values: Mapping[str, MetricParameterValue],
    target_metric_names: Sequence[str],
) -> PydanticSemanticManifest:
    """Return a manifest copy with bound metric parameter references."""
    manifest = copy.deepcopy(semantic_manifest)
    if not isinstance(manifest, PydanticSemanticManifest):
        manifest = PydanticSemanticManifest.parse_obj(manifest.dict())  # type: ignore[attr-defined]

    target_metric_names_set = _collect_metric_dependency_names(manifest, target_metric_names)
    _validate_parameter_bindings(
        manifest=manifest,
        parameter_values=parameter_values,
        target_metric_names_set=target_metric_names_set,
    )

    for metric in manifest.metrics:
        if not metric.parameters:
            continue
        if metric.name not in target_metric_names_set:
            continue
        bound_values = _resolve_parameter_values(metric.name, metric.parameters, parameter_values)
        rendered_metric_dict = _render_value(metric.dict(), bound_values)
        metric_index = manifest.metrics.index(metric)
        manifest.metrics[metric_index] = type(metric).parse_obj(rendered_metric_dict)

    return manifest


def _resolve_parameter_values(
    metric_name: str,
    parameters: Sequence[Any],
    raw_parameter_values: Mapping[str, MetricParameterValue],
) -> dict[str, Any]:
    global_parameter_values: dict[str, str] = {}
    scoped_parameter_values: dict[str, str] = {}
    for key, value in raw_parameter_values.items():
        if isinstance(value, Mapping):
            if key == metric_name:
                scoped_parameter_values = dict(value)
            continue
        global_parameter_values[key] = value

    resolved_values: dict[str, Any] = {}
    for parameter in parameters:
        raw_value = scoped_parameter_values.get(parameter.name, global_parameter_values.get(parameter.name))
        if raw_value is None:
            if parameter.default is not None:
                resolved_values[parameter.name] = parameter.default
                continue
            if parameter.required:
                raise InvalidQueryException(
                    f"Metric '{metric_name}' requires parameter '{parameter.name}', but it was not provided."
                )
            continue
        coerced_value = _coerce_parameter_value(metric_name=metric_name, parameter=parameter, raw_value=raw_value)
        resolved_values[parameter.name] = coerced_value
    return resolved_values


def _validate_parameter_bindings(
    manifest: PydanticSemanticManifest,
    parameter_values: Mapping[str, MetricParameterValue],
    target_metric_names_set: set[str],
) -> None:
    metrics_by_name = {metric.name: metric for metric in manifest.metrics}
    target_metrics = [metrics_by_name[name] for name in target_metric_names_set if name in metrics_by_name]
    valid_global_parameter_names = {
        parameter.name for metric in target_metrics for parameter in metric.parameters
    }

    unknown_global_parameter_names = sorted(
        key
        for key, value in parameter_values.items()
        if not isinstance(value, Mapping) and key not in valid_global_parameter_names
    )
    if unknown_global_parameter_names:
        raise InvalidQueryException(
            f"Unknown metric parameter(s): {', '.join(repr(name) for name in unknown_global_parameter_names)}"
        )

    for metric_name, scoped_values in parameter_values.items():
        if not isinstance(scoped_values, Mapping):
            continue
        if metric_name not in target_metric_names_set:
            raise InvalidQueryException(
                f"Unknown metric parameter scope {metric_name!r}. Scoped bindings must target a queried metric or one "
                "of its metric dependencies."
            )
        metric = metrics_by_name.get(metric_name)
        if metric is None:
            raise InvalidQueryException(f"Unknown metric parameter scope {metric_name!r}.")
        valid_parameter_names = {parameter.name for parameter in metric.parameters}
        unknown_scoped_parameter_names = sorted(set(scoped_values).difference(valid_parameter_names))
        if unknown_scoped_parameter_names:
            raise InvalidQueryException(
                f"Unknown metric parameter(s) for metric {metric_name!r}: "
                f"{', '.join(repr(name) for name in unknown_scoped_parameter_names)}"
            )


def _collect_metric_dependency_names(
    manifest: PydanticSemanticManifest,
    target_metric_names: Sequence[str],
) -> set[str]:
    metrics_by_name = {metric.name: metric for metric in manifest.metrics}
    visited: set[str] = set()

    def _visit(metric_name: str) -> None:
        if metric_name in visited:
            return
        visited.add(metric_name)
        metric = metrics_by_name.get(metric_name)
        if metric is None:
            return
        for input_metric in metric.input_metrics:
            _visit(input_metric.name)

    for metric_name in target_metric_names:
        _visit(metric_name)
    return visited


def _coerce_parameter_value(metric_name: str, parameter: Any, raw_value: str) -> Any:
    if parameter.type is ParameterType.STRING:
        value: Any = raw_value
    elif parameter.type is ParameterType.ENUM:
        value = raw_value
    elif parameter.type is ParameterType.INTEGER:
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise InvalidQueryException(
                f"Metric '{metric_name}' parameter '{parameter.name}' must be an integer, got {raw_value!r}."
            ) from exc
    elif parameter.type is ParameterType.NUMBER:
        try:
            value = float(raw_value)
        except ValueError as exc:
            raise InvalidQueryException(
                f"Metric '{metric_name}' parameter '{parameter.name}' must be a number, got {raw_value!r}."
            ) from exc
    elif parameter.type is ParameterType.BOOLEAN:
        normalized = raw_value.strip().lower()
        if normalized in ("true", "1", "yes"):
            value = True
        elif normalized in ("false", "0", "no"):
            value = False
        else:
            raise InvalidQueryException(
                f"Metric '{metric_name}' parameter '{parameter.name}' must be a boolean, got {raw_value!r}."
            )
    else:
        value = raw_value

    if parameter.allowed_values is not None and value not in parameter.allowed_values:
        raise InvalidQueryException(
            f"Metric '{metric_name}' parameter '{parameter.name}' must be one of "
            f"{parameter.allowed_values!r}, got {value!r}."
        )
    if parameter.min is not None and value < parameter.min:
        raise InvalidQueryException(
            f"Metric '{metric_name}' parameter '{parameter.name}' must be >= {parameter.min!r}, got {value!r}."
        )
    if parameter.max is not None and value > parameter.max:
        raise InvalidQueryException(
            f"Metric '{metric_name}' parameter '{parameter.name}' must be <= {parameter.max!r}, got {value!r}."
        )
    return value


def _render_value(node: Any, bound_values: Mapping[str, Any]) -> Any:
    if isinstance(node, str):
        return PARAMETER_REFERENCE_PATTERN.sub(
            lambda match: _render_scalar(bound_values[match.group(1)]),
            node,
        )
    if isinstance(node, dict):
        rendered: dict[str, Any] = {}
        for key, value in node.items():
            if key == "parameters":
                rendered[key] = value
                continue
            rendered[key] = _render_value(value, bound_values)
        return rendered
    if isinstance(node, list):
        return [_render_value(item, bound_values) for item in node]
    return node


def _render_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return str(value)
    if isinstance(value, int):
        return str(value)
    return str(value).replace("'", "''")
