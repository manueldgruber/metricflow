from __future__ import annotations

import datetime
import datetime as dt
import logging
import pathlib
import textwrap
import traceback
from functools import update_wrapper, wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import click
from dateutil.parser import parse
from metricflow_semantics.toolkit.mf_logging.lazy_formattable import LazyFormat

import dbt_metricflow.cli.custom_click_types as click_custom
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.cli_link import CliLink
from dbt_metricflow.cli.cli_string import CLIString

logger = logging.getLogger(__name__)

MetricParameterValue = Union[str, Dict[str, str]]


# Click Options
def query_options(function: Callable) -> Callable:
    """Common options for a query."""
    function = click.option(
        "--parameter",
        "--param",
        "metric_parameters",
        multiple=True,
        type=str,
        default=(),
        help=(
            "Bind a metric parameter using key=value or metric_name.key=value syntax. "
            "Repeat the flag for multiple bindings.\n\n"
            "Examples:\n\n"
            "  --parameter percentile=0.95\n"
            "  --param percentile=0.95\n"
            "  --parameter revenue.percentile=0.95 --parameter margin.percentile=0.9\n"
            "  --parameter numerator_to=buy --parameter denominator_from=visit"
        ),
    )(function)
    function = click.option(
        "--order",
        type=click_custom.SequenceParamType(),
        help=(
            "Specify metrics, dimension, or group bys to order by. "
            "Add the `-` prefix to sort query in descending (DESC) order. "
            "Leave blank for ascending (ASC) order.\n\n"
            "Examples:\n\n"
            "  To sort metric_time in DESC order: --order -metric_time\n"
            "  To sort metric_time in ASC and revenue in DESC: --order metric_time,-revenue"
        ),
        required=False,
    )(function)
    function = click.option(
        "--limit",
        type=str,
        help="Limit the number of rows out using an int or leave blank for no limit. For example: --limit 100",
        callback=lambda ctx, param, value: validate_limit(value),
    )(function)
    function = click.option(
        "--where",
        type=str,
        default=None,
        help=(
            "SQL-like where statement provided as a string and wrapped in quotes. "
            "All filter items must explicitly reference fields or dimensions "
            "that are part of your model.\n\n"
            "Examples:\n\n"
            "  Single statement:\n"
            "    --where \"{{ Dimension('order_id__revenue') }} > 100\"\n\n"
            "  Multiple statements:\n"
            "    --where \"{{ Dimension('order_id__revenue') }} > 100\"\n"
            "    --where \"{{ Dimension('user_count') }} < 1000\"\n\n"
            "Use the `Dimension()` template wrapper to indicate that the filter "
            "item is part of your model."
        ),
    )(function)
    function = start_end_time_options(function)
    function = click.option(
        "--group-by",
        type=click_custom.SequenceParamType(),
        default="",
        help=(
            "Group by dimensions or entities.\n\n"
            "Examples:\n\n"
            "  Single dimension: --group-by ds\n"
            "  Multiple dimensions: --group-by ds,org"
        ),
    )(function)
    function = click.option(
        "--metrics",
        type=click_custom.SequenceParamType(min_length=0),
        default="",
        help=(
            "Specify metrics to query.\n\n"
            "Examples:\n\n"
            "  Single metric: --metrics bookings\n"
            "  Multiple metrics: --metrics bookings,messages"
        ),
    )(function)
    return function


def start_end_time_options(function: Callable) -> Callable:
    """Options for start_time and end_time."""
    function = click.option(
        "--start-time",
        type=str,
        default=None,
        help="Optional iso8601 timestamp to constraint the start time of the data (inclusive)",
        callback=lambda ctx, param, value: convert_to_datetime(value),
    )(function)

    function = click.option(
        "--end-time",
        type=str,
        default=None,
        help="Optional iso8601 timestamp to constraint the end time of the data (inclusive)",
        callback=lambda ctx, param, value: convert_to_datetime(value),
    )(function)
    return function


# Parsers/Validators
def convert_to_datetime(datetime_str: Optional[str]) -> Optional[dt.datetime]:
    """Callback to convert string to datetime given as an iso8601 timestamp."""
    if datetime_str is None:
        return None

    try:
        return parse(datetime_str)
    except Exception:
        raise click.BadParameter("must be valid iso8601 timestamp")


def parse_comma_separated_inputs(value: Optional[str]) -> Optional[List[str]]:  # noqa: D103
    # If comma exist, explode this into a list and return
    if value is None:
        return None
    if "," in value:
        return [i.strip() for i in value.split(",")]

    # Return a list of the single value
    return [value]


def validate_limit(limit: Optional[str]) -> Optional[int]:
    """Validates and transform limit input."""
    if limit and not limit.isnumeric():
        raise click.BadParameter("limit must be an int. For no limit, do not pass this argument")
    return int(limit) if limit else None


def parse_metric_parameters(
    metric_parameters: Sequence[str],
    metric_names: Optional[Sequence[str]] = None,
    saved_query_name: Optional[str] = None,
) -> Optional[Dict[str, MetricParameterValue]]:
    """Parse repeated CLI parameter bindings."""
    del saved_query_name
    parsed: Dict[str, MetricParameterValue] = {}
    selected_metric_names = set(metric_names or ())
    for raw_parameter in metric_parameters:
        if "=" not in raw_parameter:
            raise click.BadParameter(
                f"Invalid parameter binding {raw_parameter!r}. Expected key=value or metric_name.key=value syntax."
            )
        key, value = raw_parameter.split("=", 1)
        key = key.strip()
        if not key:
            raise click.BadParameter(f"Invalid parameter binding {raw_parameter!r}. Parameter name cannot be empty.")
        if "." in key:
            metric_name, parameter_name = key.split(".", 1)
            metric_name = metric_name.strip()
            parameter_name = parameter_name.strip()
            if not metric_name or not parameter_name:
                raise click.BadParameter(
                    f"Invalid parameter binding {raw_parameter!r}. Expected metric_name.key=value syntax."
                )
            if selected_metric_names and metric_name not in selected_metric_names:
                raise click.BadParameter(
                    f"Invalid parameter binding {raw_parameter!r}. Metric {metric_name!r} is not in --metrics."
                )
            bucket = parsed.get(metric_name)
            if bucket is None:
                bucket = {}
                parsed[metric_name] = bucket
            elif not isinstance(bucket, dict):
                raise click.BadParameter(
                    f"Invalid parameter binding {raw_parameter!r}. {metric_name!r} is already used as an unscoped "
                    "parameter name."
                )
            if parameter_name in bucket:
                raise click.BadParameter(
                    f"Invalid parameter binding {raw_parameter!r}. Parameter {parameter_name!r} was already set for "
                    f"metric {metric_name!r}."
                )
            bucket[parameter_name] = value
            continue

        existing_value = parsed.get(key)
        if isinstance(existing_value, dict):
            raise click.BadParameter(
                f"Invalid parameter binding {raw_parameter!r}. {key!r} is already used as a metric scope."
            )
        if existing_value is not None:
            raise click.BadParameter(
                f"Invalid parameter binding {raw_parameter!r}. Parameter {key!r} was already set."
            )
        parsed[key] = value
    return parsed or None


def echo_semantic_manifest_context(cli_configuration: CLIConfiguration) -> None:
    """Best-effort attempt to print details about the semantic manifest to the console when an error occurs.

    These messages could help the user figure out if their error is caused by an out-of-date artifact. If so, they
    can re-run `dbt parse` or `dbt build`.
    """
    try:
        if not cli_configuration.is_setup:
            logger.debug("Skipping retrieval of semantic manifest context as the configuration has not been set up.")
            return
        project = cli_configuration.dbt_project_metadata.project
        target_path = pathlib.Path(project.project_root) / pathlib.Path(project.target_path)
        semantic_manifest_json_path = target_path / "semantic_manifest.json"

        click.echo(f"{CLIString.ARTIFACT_PATH} {semantic_manifest_json_path}")
        if semantic_manifest_json_path.exists():
            modified_time = datetime.datetime.fromtimestamp(semantic_manifest_json_path.stat().st_mtime)
            click.echo(f"{CLIString.ARTIFACT_MODIFIED_TIME} {modified_time.isoformat()}")
    except Exception:
        logger.exception("Got an exception while trying to get the semantic-manifest context")


# Misc
def exception_handler(func: Callable[..., Any]) -> Callable[..., Any]:  # type: ignore[misc]
    """Decorator to handle exceptions."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        try:
            func(*args, **kwargs)
        except Exception as e:
            # This will log to the file handlers registered in the root.
            logging.exception("Logging exception handled by the CLI exception handler")
            click.echo(f"\nERROR: {str(e)}".rstrip())

            cli_configuration: Optional[CLIConfiguration] = None
            first_argument = args[0]
            if isinstance(first_argument, CLIConfiguration):
                cli_configuration = first_argument

            if cli_configuration is not None:
                if cli_configuration.is_setup:
                    click.echo(f"\n{CLIString.LOG_FILE_PREFIX}: {cli_configuration.log_file_path}")
                    echo_semantic_manifest_context(cli_configuration)
                else:
                    logger.debug(
                        "CLI configuration is not setup, possibly due to an exception during configuration setup."
                    )
            else:
                logger.error(
                    LazyFormat(
                        "Unexpected first argument in the exception handler - it should have been passed a configuration.",
                        expected_argument_class=CLIConfiguration.__name__,
                        first_argument=first_argument,
                    )
                )

            click.echo(
                "\n"
                + textwrap.dedent(
                    f"""\
                    If you think you found a bug, please report it here:
                        {CliLink.get_report_issue_link()}
                    """
                )
            )

            # Checks if CLIContext has verbose flag set
            if args and hasattr(first_argument, "verbose") and first_argument.verbose is True:
                click.echo(traceback.format_exc())

            exit(1)

    return wrapper


def dbt_project_file_exists() -> bool:
    """Check that the cwd is a dbt project root. Currently done by checking for existence of dbt_project.yml."""
    return pathlib.Path("dbt_project.yml").exists()


def error_if_not_in_dbt_project(func: Callable) -> Callable:
    """Decorator to output an error message and exit if caller is not in a root directory of a dbt project."""

    @click.pass_context
    def new_func(ctx: click.core.Context, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        if not dbt_project_file_exists():
            click.echo(
                "❌ Unable to locate 'dbt_project.yml' in the current directory\n"
                "In order to run the MetricFlow CLI, you must be running in the root directory of a working dbt project.\n"
                "Please check out `https://docs.getdbt.com/reference/commands/init` if you want to get started on building a dbt project."
            )
            exit(1)
        return ctx.invoke(func, *args, **kwargs)

    return update_wrapper(new_func, func)
