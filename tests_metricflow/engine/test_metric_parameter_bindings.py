from __future__ import annotations

import pytest
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.implementations.metric import (
    PydanticMetric,
    PydanticMetricAggregationParams,
    PydanticMetricInput,
    PydanticMetricInputMeasure,
    PydanticMetricParameter,
    PydanticMetricTypeParams,
)
from dbt_semantic_interfaces.implementations.semantic_manifest import PydanticSemanticManifest
from dbt_semantic_interfaces.type_enums import AggregationType, MetricType, ParameterType
from metricflow_semantics.errors.error_classes import InvalidQueryException

from metricflow.engine.metric_parameter_bindings import bind_metric_parameters

from tests.example_project_configuration import EXAMPLE_PROJECT_CONFIGURATION


def test_bind_metric_parameters_renders_metric_templates() -> None:
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[
            PydanticMetric(
                name="conversion_rate",
                type=MetricType.RATIO,
                label="Conversion to {{ parameter('numerator_to') }}",
                description="conversion rate metric",
                type_params=PydanticMetricTypeParams(
                    numerator=PydanticMetricInput(
                        name="users",
                        alias="users_to_{{ parameter('numerator_to') }}",
                        filter=PydanticWhereFilterIntersection(
                            where_filters=[
                                {
                                    "where_sql_template": "{{ Dimension('user__step') }} = '{{ parameter('numerator_to') }}'"
                                }
                            ]
                        ),
                        offset_window=None,
                        offset_to_grain=None,
                    ),
                    denominator=PydanticMetricInput(
                        name="users",
                        alias=None,
                        filter=None,
                        offset_window=None,
                        offset_to_grain=None,
                    ),
                ),
                parameters=[
                    PydanticMetricParameter(name="numerator_to", type=ParameterType.STRING, required=True),
                ],
            )
        ],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    bound_manifest = bind_metric_parameters(
        semantic_manifest=semantic_manifest,
        parameter_values={"numerator_to": "buy"},
        target_metric_names=["conversion_rate"],
    )

    bound_metric = bound_manifest.metrics[0]
    assert bound_metric.label == "Conversion to buy"
    assert bound_metric.type_params.numerator is not None
    assert bound_metric.type_params.numerator.alias == "users_to_buy"
    assert bound_metric.type_params.numerator.filter is not None
    assert bound_metric.type_params.numerator.filter.where_filters[0].where_sql_template.endswith("= 'buy'")


def test_bind_metric_parameters_rejects_missing_required_parameter() -> None:
    semantic_manifest = PydanticSemanticManifest(
        semantic_models=[],
        metrics=[
            PydanticMetric(
                name="transactions_for_country",
                type=MetricType.SIMPLE,
                label="Transactions for {{ parameter('country') }}",
                description="transaction filter metric",
                type_params=PydanticMetricTypeParams(
                    measure=PydanticMetricInputMeasure(name="transactions"),
                    metric_aggregation_params=PydanticMetricAggregationParams(
                        semantic_model="transactions",
                        agg=AggregationType.SUM,
                    ),
                ),
                filter=PydanticWhereFilterIntersection(
                    where_filters=[
                        {
                            "where_sql_template": "{{ Dimension('transaction__country') }} = '{{ parameter('country') }}'"
                        }
                    ]
                ),
                parameters=[
                    PydanticMetricParameter(name="country", type=ParameterType.STRING, required=True),
                ],
            )
        ],
        project_configuration=EXAMPLE_PROJECT_CONFIGURATION,
    )

    with pytest.raises(InvalidQueryException, match="requires parameter 'country'"):
        bind_metric_parameters(
            semantic_manifest=semantic_manifest,
            parameter_values={},
            target_metric_names=["transactions_for_country"],
        )
