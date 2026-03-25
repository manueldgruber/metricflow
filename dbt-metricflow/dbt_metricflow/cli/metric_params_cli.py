"""CLI parsing for metric runtime parameters (``mf query --param``)."""

from __future__ import annotations

from typing import Dict, Optional, Sequence

import click


def parse_metric_params_from_cli(
    metric_param_args: Optional[Sequence[str]],
    metric_names: Optional[Sequence[str]],
) -> Optional[Dict[str, Dict[str, str]]]:
    """Parse repeatable ``--param`` values into ``MetricFlowQueryRequest.metric_params``.

    * ``param_name=value`` — allowed only when exactly one metric is requested (``--metrics`` has one name).
    * ``metric_name.param_name=value`` — required when multiple metrics are requested, or with ``--saved-query``.

    Raises:
        click.UsageError: On invalid or ambiguous input.
    """
    if not metric_param_args:
        return None

    names = list(metric_names) if metric_names else []

    result: Dict[str, Dict[str, str]] = {}
    for raw in metric_param_args:
        token = raw.strip()
        if "=" not in token:
            raise click.UsageError(
                f"Invalid --param {raw!r}: expected param_name=value or metric_name.param_name=value"
            )
        lhs, rhs = token.split("=", 1)
        lhs = lhs.strip()
        rhs = rhs.strip()
        if not lhs:
            raise click.UsageError(f"Invalid --param {raw!r}: missing name before '='")

        if "." in lhs:
            metric_name, param_name = lhs.split(".", 1)
            metric_name = metric_name.strip()
            param_name = param_name.strip()
            if not metric_name or not param_name:
                raise click.UsageError(
                    f"Invalid --param {raw!r}: expected metric_name.param_name=value"
                )
        else:
            if len(names) != 1:
                raise click.UsageError(
                    "Unscoped --param requires exactly one metric in --metrics. "
                    "Use metric_name.param_name=value when querying multiple metrics or with --saved-query."
                )
            metric_name = names[0]
            param_name = lhs

        bucket = result.setdefault(metric_name, {})
        if param_name in bucket:
            raise click.UsageError(
                f"Duplicate --param for metric {metric_name!r}: {param_name!r} was set more than once."
            )
        bucket[param_name] = rhs

    return result
