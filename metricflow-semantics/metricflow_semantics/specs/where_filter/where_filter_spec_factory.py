from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import List, Optional, Sequence

import jinja2
from dbt_semantic_interfaces.implementations.filters.where_filter import PydanticWhereFilterIntersection
from dbt_semantic_interfaces.protocols import WhereFilter, WhereFilterIntersection

from metricflow_semantics.errors.error_classes import RenderSqlTemplateException
from metricflow_semantics.model.semantics.metric_lookup import MetricLookup
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_location import WhereFilterLocation
from metricflow_semantics.query.group_by_item.filter_spec_resolution.metric_param_jinja import (
    jinja_params_for_where_filter_location,
)
from metricflow_semantics.query.group_by_item.filter_spec_resolution.filter_spec_lookup import (
    FilterSpecResolutionLookUp,
)
from metricflow_semantics.semantic_graph.attribute_resolution.group_by_item_set import (
    GroupByItemSet,
)
from metricflow_semantics.specs.column_assoc import ColumnAssociationResolver
from metricflow_semantics.specs.rendered_spec_tracker import RenderedSpecTracker
from metricflow_semantics.specs.where_filter.where_filter_dimension import WhereFilterDimensionFactory
from metricflow_semantics.specs.where_filter.where_filter_entity import WhereFilterEntityFactory
from metricflow_semantics.specs.where_filter.where_filter_metric import WhereFilterMetricFactory
from metricflow_semantics.specs.where_filter.where_filter_spec import WhereFilterSpec
from metricflow_semantics.specs.where_filter.where_filter_time_dimension import WhereFilterTimeDimensionFactory
from metricflow_semantics.sql.sql_bind_parameters import SqlBindParameterSet

logger = logging.getLogger(__name__)


class WhereFilterSpecFactory:
    """Renders the SQL template in the WhereFilter and converts it to a WhereFilterSpec."""

    def __init__(  # noqa: D107
        self,
        column_association_resolver: ColumnAssociationResolver,
        spec_resolution_lookup: FilterSpecResolutionLookUp,
        custom_grain_names: Sequence[str],
        metric_lookup: Optional[MetricLookup] = None,
        metric_params: Optional[Mapping[str, Mapping[str, str]]] = None,
    ) -> None:
        self._column_association_resolver = column_association_resolver
        self._spec_resolution_lookup = spec_resolution_lookup
        self._custom_grain_names = tuple(custom_grain_names)
        self._metric_lookup = metric_lookup
        self._metric_params = metric_params

    def create_from_where_filter(  # noqa: D102
        self,
        filter_location: WhereFilterLocation,
        where_filter: WhereFilter,
    ) -> WhereFilterSpec:
        return self.create_from_where_filter_intersection(
            filter_location=filter_location,
            filter_intersection=PydanticWhereFilterIntersection(where_filters=[where_filter]),
        )[0]

    def create_from_where_filter_intersection(  # noqa: D102
        self,
        filter_location: WhereFilterLocation,
        filter_intersection: Optional[WhereFilterIntersection],
    ) -> Sequence[WhereFilterSpec]:
        if filter_intersection is None:
            return ()

        filter_specs: List[WhereFilterSpec] = []

        for where_filter in filter_intersection.where_filters:
            rendered_spec_tracker = RenderedSpecTracker()
            dimension_factory = WhereFilterDimensionFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
                custom_granularity_names=self._custom_grain_names,
            )
            time_dimension_factory = WhereFilterTimeDimensionFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
                custom_granularity_names=self._custom_grain_names,
            )
            entity_factory = WhereFilterEntityFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            metric_factory = WhereFilterMetricFactory(
                column_association_resolver=self._column_association_resolver,
                spec_resolution_lookup=self._spec_resolution_lookup,
                where_filter_location=filter_location,
                rendered_spec_tracker=rendered_spec_tracker,
            )
            try:
                # If there was an error with the template, it should have been caught while resolving the specs for
                # the filters during query resolution.
                render_context = {
                    "Dimension": dimension_factory.create,
                    "TimeDimension": time_dimension_factory.create,
                    "Entity": entity_factory.create,
                    "Metric": metric_factory.create,
                }
                if self._metric_lookup is not None:
                    jinja_params = jinja_params_for_where_filter_location(
                        location=filter_location,
                        metric_params=self._metric_params,
                        metric_lookup=self._metric_lookup,
                    )
                    if jinja_params is not None:
                        render_context["params"] = jinja_params
                where_sql = jinja2.Template(where_filter.where_sql_template, undefined=jinja2.StrictUndefined).render(
                    render_context
                )
            except (jinja2.exceptions.UndefinedError, jinja2.exceptions.TemplateSyntaxError) as e:
                raise RenderSqlTemplateException(
                    f"Error while rendering Jinja template:\n{where_filter.where_sql_template}"
                ) from e

            filter_specs.append(
                WhereFilterSpec(
                    where_sql=where_sql,
                    bind_parameters=SqlBindParameterSet(),
                    element_set=GroupByItemSet.create(*rendered_spec_tracker.rendered_specs),
                )
            )

        return filter_specs
