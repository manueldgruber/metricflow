from __future__ import annotations

import pytest
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricInput,
    PydanticMetricParameter,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.project_configuration import PydanticProjectConfiguration
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.type_enums import MetricType, ParameterType
from metricflow.engine.metric_parameter_bindings import bind_metric_parameters
from metricflow_semantics.errors.error_classes import InvalidQueryException


def _simple_metric(name: str, description: str, parameter_name: str) -> PydanticMetric:
    return PydanticMetric(
        name=name,
        description=description,
        type=MetricType.SIMPLE,
        type_params=PydanticMetricTypeParams(),
        filter=None,
        metadata=None,
        parameters=[PydanticMetricParameter(name=parameter_name, type=ParameterType.STRING, required=True)],
    )


def test_bind_metric_parameters_applies_global_binding_to_multiple_metrics() -> None:
    manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[
            _simple_metric("revenue", "Metric for {{ parameter('region') }}", "region"),
            _simple_metric("margin", "Metric for {{ parameter('region') }}", "region"),
        ],
        project_configuration=PydanticProjectConfiguration(),
    )

    bound_manifest = bind_metric_parameters(
        semantic_manifest=manifest,
        parameter_values={"region": "emea"},
        target_metric_names=("revenue", "margin"),
    )

    assert [metric.description for metric in bound_manifest.metrics] == ["Metric for emea", "Metric for emea"]


def test_bind_metric_parameters_allows_scoped_overrides_for_multi_metric_queries() -> None:
    manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[
            _simple_metric("revenue", "Metric for {{ parameter('region') }}", "region"),
            _simple_metric("margin", "Metric for {{ parameter('region') }}", "region"),
        ],
        project_configuration=PydanticProjectConfiguration(),
    )

    bound_manifest = bind_metric_parameters(
        semantic_manifest=manifest,
        parameter_values={
            "region": "global",
            "revenue": {"region": "emea"},
            "margin": {"region": "na"},
        },
        target_metric_names=("revenue", "margin"),
    )

    assert [metric.description for metric in bound_manifest.metrics] == ["Metric for emea", "Metric for na"]


def test_bind_metric_parameters_rejects_unknown_scoped_parameter_names() -> None:
    manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[_simple_metric("revenue", "Metric for {{ parameter('region') }}", "region")],
        project_configuration=PydanticProjectConfiguration(),
    )

    with pytest.raises(InvalidQueryException, match="Unknown metric parameter\\(s\\) for metric 'revenue'"):
        bind_metric_parameters(
            semantic_manifest=manifest,
            parameter_values={"revenue": {"country": "de"}},
            target_metric_names=("revenue",),
        )


def test_bind_metric_parameters_rejects_unknown_metric_scopes() -> None:
    manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[_simple_metric("revenue", "Metric for {{ parameter('region') }}", "region")],
        project_configuration=PydanticProjectConfiguration(),
    )

    with pytest.raises(InvalidQueryException, match="Unknown metric parameter scope 'margin'"):
        bind_metric_parameters(
            semantic_manifest=manifest,
            parameter_values={"margin": {"region": "na"}},
            target_metric_names=("revenue",),
        )


def test_bind_metric_parameters_applies_scoped_binding_to_metric_dependencies() -> None:
    manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[
            _simple_metric("revenue", "Metric for {{ parameter('region') }}", "region"),
            PydanticMetric(
                name="revenue_ratio",
                description="Derived",
                type=MetricType.DERIVED,
                type_params=PydanticMetricTypeParams(
                    expr="revenue",
                    metrics=[
                        PydanticMetricInput(
                            name="revenue",
                            filter=None,
                            alias=None,
                            offset_window=None,
                            offset_to_grain=None,
                        )
                    ],
                ),
                filter=None,
                metadata=None,
            ),
        ],
        project_configuration=PydanticProjectConfiguration(),
    )

    bound_manifest = bind_metric_parameters(
        semantic_manifest=manifest,
        parameter_values={"revenue": {"region": "emea"}},
        target_metric_names=("revenue_ratio",),
    )

    revenue_metric = next(metric for metric in bound_manifest.metrics if metric.name == "revenue")
    assert revenue_metric.description == "Metric for emea"
