"""Validate runtime ``metric_params`` on a query against declared metric parameters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Dict, Final, Optional

from dbt_semantic_interfaces.protocols.metric import Metric
from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.errors.error_classes import InvalidQueryException, MetricNotFoundError
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

_ALLOWED_PARAM_TYPES: Final = frozenset({"string", "int", "float"})

_PLACEHOLDER_STRING_DW_VALIDATION: Final = "__MF_DW_VALIDATION__"


def metric_params_placeholders_for_dw_validation(metric: Metric) -> Optional[Dict[str, str]]:
    """Build placeholder param values so EXPLAIN / data-warehouse validation can run.

    ``mf validate-configs`` issues a minimal metric query per metric. Required params with no default would
    otherwise fail :func:`validate_metric_params_for_query`. Placeholders satisfy that validation and use
    type-correct strings for ``int`` / ``float`` params; they are not meant as real analytics inputs.

    Returns:
        A mapping of param name -> placeholder string, or ``None`` if no placeholders are needed.
    """
    if not metric.params:
        return None
    placeholders: Dict[str, str] = {}
    for p in metric.params:
        if not (p.required and p.default is None):
            continue
        if p.type == "string":
            placeholders[p.name] = _PLACEHOLDER_STRING_DW_VALIDATION
        elif p.type == "int":
            placeholders[p.name] = "0"
        elif p.type == "float":
            placeholders[p.name] = "0.0"
        else:
            # Invalid types are caught by manifest validation; skip so we do not emit unknown keys.
            continue
    return placeholders or None


def validate_metric_params_for_query(
    manifest_lookup: SemanticManifestLookup,
    queried_metric_names: Sequence[str],
    metric_params: Optional[Mapping[str, Mapping[str, str]]],
) -> None:
    """Validate ``metric_params`` for the metrics in this query.

    Rules:
    - Every key in the outer dict must be a metric name that appears in this query.
    - Every key in each inner dict must be a declared param name for that metric.
    - Each required param without a default must be supplied (present in the inner dict).
    - Values supplied for ``int`` / ``float`` params must be parseable as such.

    Raises:
        InvalidQueryException: If validation fails.
    """
    queried_set = set(queried_metric_names)
    params_by_metric = metric_params or {}

    for metric_name in params_by_metric:
        if metric_name not in queried_set:
            raise InvalidQueryException(
                f"Unknown metric name `{metric_name}` in metric_params. "
                f"Metrics in this query: {sorted(queried_set)}"
            )

    for metric_name in queried_set:
        try:
            metric = manifest_lookup.metric_lookup.get_metric(MetricReference(element_name=metric_name))
        except MetricNotFoundError:
            # Resolver will report unknown metrics; avoid duplicate errors here.
            continue
        _validate_params_for_single_metric(
            metric_name=metric_name,
            metric=metric,
            provided=params_by_metric.get(metric_name, {}),
        )


def _validate_params_for_single_metric(
    metric_name: str,
    metric: Metric,
    provided: Mapping[str, str],
) -> None:
    declared = {p.name: p for p in metric.params} if metric.params else {}

    for param_name in provided:
        if param_name not in declared:
            raise InvalidQueryException(
                f"Unknown param `{param_name}` for metric `{metric_name}`. "
                f"Declared params: {sorted(declared) if declared else '(none)'}"
            )

    for name, spec in declared.items():
        if spec.type not in _ALLOWED_PARAM_TYPES:
            raise InvalidQueryException(
                f"Metric `{metric_name}` declares param `{name}` with invalid type `{spec.type}` in the manifest."
            )

        value = provided.get(name)
        if value is None:
            if spec.required and spec.default is None:
                raise InvalidQueryException(
                    f"Missing required param `{name}` for metric `{metric_name}`."
                )
            continue

        _validate_value_matches_declared_type(
            metric_name=metric_name, param_name=name, param_type=spec.type, value=value
        )


def _validate_value_matches_declared_type(
    metric_name: str,
    param_name: str,
    param_type: str,
    value: str,
) -> None:
    if param_type == "string":
        return
    if param_type == "int":
        try:
            int(value)
        except ValueError:
            raise InvalidQueryException(
                f"Param `{param_name}` for metric `{metric_name}` expects type int but got `{value}`."
            ) from None
    elif param_type == "float":
        try:
            float(value)
        except ValueError:
            raise InvalidQueryException(
                f"Param `{param_name}` for metric `{metric_name}` expects type float but got `{value}`."
            ) from None
