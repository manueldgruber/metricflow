from __future__ import annotations

import textwrap

import pytest
from dbt_semantic_interfaces.parsing.objects import YamlConfigFile

from metricflow_semantics.errors.error_classes import InvalidQueryException
from metricflow_semantics.query.query_parser import MetricFlowQueryParser
from metricflow_semantics.test_helpers.example_project_configuration import (
    EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
)

from tests_metricflow_semantics.query.conftest import BOOKINGS_YAML, query_parser_from_yaml

# One document per YAML file: DSI expects `metric:` (singular), not `metrics:`.
METRIC_WITH_PARAM_YAML = textwrap.dedent(
    """\
    metric:
      name: bookings_with_region
      type: simple
      type_params:
        measure:
          name: bookings
      params:
        - name: region
          type: string
      filter: |
        {{ Dimension('booking__is_instant') }} = true
    """
)

# Dunder form required for time dimension group-by on this semantic model (plain `ds` does not resolve).
_BOOKINGS_TIME_GROUP_BY = "booking__ds__day"


@pytest.fixture
def bookings_with_param_parser() -> MetricFlowQueryParser:
    return query_parser_from_yaml(
        [
            EXAMPLE_PROJECT_CONFIGURATION_YAML_CONFIG_FILE,
            YamlConfigFile(filepath="inline_bookings_semantic_model", contents=BOOKINGS_YAML),
            YamlConfigFile(filepath="inline_bookings_metric_with_params", contents=METRIC_WITH_PARAM_YAML),
        ]
    )


def test_metric_params_unknown_outer_metric_name_raises(bookings_with_param_parser: MetricFlowQueryParser) -> None:
    with pytest.raises(InvalidQueryException, match="Unknown metric name"):
        bookings_with_param_parser.parse_and_validate_query(
            metric_names=("bookings_with_region",),
            group_by_names=(_BOOKINGS_TIME_GROUP_BY,),
            metric_params={"other_metric": {"region": "US"}},
        )


def test_metric_params_missing_required_raises(bookings_with_param_parser: MetricFlowQueryParser) -> None:
    with pytest.raises(InvalidQueryException, match="Missing required param"):
        bookings_with_param_parser.parse_and_validate_query(
            metric_names=("bookings_with_region",),
            group_by_names=(_BOOKINGS_TIME_GROUP_BY,),
        )


def test_metric_params_unknown_inner_key_raises(bookings_with_param_parser: MetricFlowQueryParser) -> None:
    with pytest.raises(InvalidQueryException, match="Unknown param"):
        bookings_with_param_parser.parse_and_validate_query(
            metric_names=("bookings_with_region",),
            group_by_names=(_BOOKINGS_TIME_GROUP_BY,),
            metric_params={"bookings_with_region": {"not_a_param": "x"}},
        )


def test_metric_params_succeeds_when_required_supplied(bookings_with_param_parser: MetricFlowQueryParser) -> None:
    bookings_with_param_parser.parse_and_validate_query(
        metric_names=("bookings_with_region",),
        group_by_names=(_BOOKINGS_TIME_GROUP_BY,),
        metric_params={"bookings_with_region": {"region": "US"}},
    )
