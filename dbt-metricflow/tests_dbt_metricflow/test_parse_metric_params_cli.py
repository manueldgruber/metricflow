"""Tests for ``parse_metric_params_from_cli`` (``mf query --param``)."""

from __future__ import annotations

import click
import pytest

from dbt_metricflow.cli.metric_params_cli import parse_metric_params_from_cli


def test_parse_none_or_empty_returns_none() -> None:
    assert parse_metric_params_from_cli(None, ["a"]) is None
    assert parse_metric_params_from_cli((), ["a"]) is None


def test_parse_single_metric_unscoped() -> None:
    assert parse_metric_params_from_cli(
        ("product_name=XY", "min_spend=100"),
        ("product_attachment_rate",),
    ) == {
        "product_attachment_rate": {"product_name": "XY", "min_spend": "100"},
    }


def test_parse_multiple_metrics_scoped() -> None:
    assert parse_metric_params_from_cli(
        (
            "product_attachment_rate.product_name=XY",
            "product_attachment_rate.min_spend=100",
            "category_penetration.category=Electronics",
        ),
        ("product_attachment_rate", "category_penetration"),
    ) == {
        "product_attachment_rate": {"product_name": "XY", "min_spend": "100"},
        "category_penetration": {"category": "Electronics"},
    }


def test_parse_value_may_contain_equals() -> None:
    assert parse_metric_params_from_cli(
        ("expr=a=b",),
        ("m",),
    ) == {"m": {"expr": "a=b"}}


def test_parse_unscoped_requires_exactly_one_metric() -> None:
    with pytest.raises(click.UsageError, match="exactly one metric"):
        parse_metric_params_from_cli(("a=1",), ("m1", "m2"))


def test_parse_rejects_duplicate_param() -> None:
    with pytest.raises(click.UsageError, match="Duplicate"):
        parse_metric_params_from_cli(
            ("a=1", "a=2"),
            ("m",),
        )


def test_parse_rejects_missing_equals() -> None:
    with pytest.raises(click.UsageError, match="expected param_name=value"):
        parse_metric_params_from_cli(("noequals",), ("m",))
