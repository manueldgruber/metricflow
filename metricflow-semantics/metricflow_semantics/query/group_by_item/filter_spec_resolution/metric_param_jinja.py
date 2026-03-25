"""Resolve runtime metric parameter bindings for Jinja ``params`` in where-filter templates."""

from __future__ import annotations

from typing import Dict, Mapping, Optional, Sequence

from dbt_semantic_interfaces.references import MetricReference

from metricflow_semantics.errors.error_classes import MetricNotFoundError
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import (
    WhereFilterLocation,
    WhereFilterLocationType,
)


def resolved_jinja_params_for_manifest_metric(
    manifest_metric_name: str,
    metric_params: Optional[Mapping[str, Mapping[str, str]]],
    metric_lookup: MetricLookup,
    *,
    value_lookup_metric_names: Sequence[str],
) -> Optional[Mapping[str, str]]:
    """Declare param names/defaults from ``manifest_metric_name``; values from ``metric_params`` for the given names.

    Later names in ``value_lookup_metric_names`` override earlier keys on duplicate param names.
    """
    provided: Dict[str, str] = {}
    params_by_metric = metric_params or {}
    for mname in value_lookup_metric_names:
        provided.update(params_by_metric.get(mname, {}))
    try:
        metric = metric_lookup.get_metric(MetricReference(element_name=manifest_metric_name))
    except MetricNotFoundError:
        return provided or None
    if not metric.params:
        return provided or None
    for p in metric.params:
        if p.name not in provided and p.default is not None:
            provided[p.name] = p.default
    return provided or None


def resolved_jinja_params_for_metric_name(
    metric_name: str,
    metric_params: Optional[Mapping[str, Mapping[str, str]]],
    metric_lookup: MetricLookup,
) -> Optional[Mapping[str, str]]:
    """Combine query ``metric_params`` for one metric with declared defaults from the manifest."""
    return resolved_jinja_params_for_manifest_metric(
        manifest_metric_name=metric_name,
        metric_params=metric_params,
        metric_lookup=metric_lookup,
        value_lookup_metric_names=(metric_name,),
    )


def jinja_params_for_where_filter_location(
    location: WhereFilterLocation,
    metric_params: Optional[Mapping[str, Mapping[str, str]]],
    metric_lookup: MetricLookup,
    *,
    queried_metric_references: Optional[Sequence[MetricReference]] = None,
) -> Optional[Mapping[str, str]]:
    """Build the Jinja ``params`` namespace for ``{{ params.* }}`` at this filter location.

    ``queried_metric_references`` should be the metric(s) in the user query. Bindings under those names are merged
    first so ``--param`` / ``metric_params`` keyed by a derived metric (e.g. ``product_attachment_rate``) apply to
    filters on nested simple metrics (``WhereFilterLocation.METRIC`` for the input's underlying metric).
    """
    if location.location_type == WhereFilterLocationType.QUERY:
        merged: Dict[str, str] = {}
        for ref in sorted(location.metric_references, key=lambda r: r.element_name):
            part = resolved_jinja_params_for_metric_name(ref.element_name, metric_params, metric_lookup)
            if part:
                merged.update(part)
        return merged or None
    if location.location_type == WhereFilterLocationType.METRIC:
        assert len(location.metric_references) == 1
        mname = location.metric_references[0].element_name
        value_names: list[str] = []
        if queried_metric_references is not None:
            for ref in sorted(queried_metric_references, key=lambda r: r.element_name):
                value_names.append(ref.element_name)
        value_names.append(mname)
        return resolved_jinja_params_for_manifest_metric(
            manifest_metric_name=mname,
            metric_params=metric_params,
            metric_lookup=metric_lookup,
            value_lookup_metric_names=tuple(value_names),
        )
    if location.location_type == WhereFilterLocationType.INPUT_METRIC:
        assert len(location.metric_references) == 1
        input_name = location.metric_references[0].element_name
        value_names: list[str] = []
        if queried_metric_references is not None:
            for ref in sorted(queried_metric_references, key=lambda r: r.element_name):
                value_names.append(ref.element_name)
        if location.derived_metric_reference is not None:
            value_names.append(location.derived_metric_reference.element_name)
        value_names.append(input_name)
        return resolved_jinja_params_for_manifest_metric(
            manifest_metric_name=input_name,
            metric_params=metric_params,
            metric_lookup=metric_lookup,
            value_lookup_metric_names=tuple(value_names),
        )
    raise AssertionError(f"Unhandled WhereFilterLocationType: {location.location_type}")
