from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from dbt_metricflow.cli.utils import parse_metric_parameters
from dbt_metricflow.cli.utils import query_options


def test_parse_metric_parameters() -> None:
    assert parse_metric_parameters(("percentile=0.95", "label=95")) == {
        "percentile": "0.95",
        "label": "95",
    }


def test_parse_metric_parameters_supports_scoped_values() -> None:
    assert parse_metric_parameters(
        ("revenue.percentile=0.95", "margin.percentile=0.9"),
        metric_names=("revenue", "margin"),
    ) == {
        "revenue": {"percentile": "0.95"},
        "margin": {"percentile": "0.9"},
    }


def test_parse_metric_parameters_supports_global_and_scoped_values() -> None:
    assert parse_metric_parameters(
        ("percentile=0.95", "margin.percentile=0.9"),
        metric_names=("revenue", "margin"),
    ) == {
        "percentile": "0.95",
        "margin": {"percentile": "0.9"},
    }


def test_parse_metric_parameters_rejects_invalid_syntax() -> None:
    with pytest.raises(click.BadParameter, match="Expected key=value or metric_name.key=value syntax"):
        parse_metric_parameters(("percentile",))


def test_parse_metric_parameters_rejects_unknown_metric_scope() -> None:
    with pytest.raises(click.BadParameter, match="is not in --metrics"):
        parse_metric_parameters(("margin.percentile=0.9",), metric_names=("revenue",))


def test_query_options_accepts_param_alias() -> None:
    @click.command()
    @query_options
    def command(metric_parameters: tuple[str, ...], **_: object) -> None:
        click.echo(",".join(metric_parameters))

    result = CliRunner().invoke(command, ["--metrics", "metric_one", "--param", "percentile=0.95"])

    assert result.exit_code == 0
    assert result.output.strip() == "percentile=0.95"
