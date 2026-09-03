from typing import List, Dict, Set, Protocol, runtime_checkable

from datapipe.compute import DatapipeApp, Catalog, ComputeStep, DataStore, Pipeline, PipelineStep
from datapipe.step.batch_generate import BatchGenerate
from datapipe.step.batch_transform import BaseBatchTransformStep, BatchTransform, DatatableBatchTransform
from datapipe.step.datatable_transform import DatatableTransform
from datapipe.step.update_external_table import UpdateExternalTable
from datapipe.store.database import TableStoreDB
from datapipe.store.table_store import TableStore
from datapipe.types import Labels

from datapipe_router.types import (
    Graph, 
    PipelineNode, 
    PipelineStepNode,
    MetaPipelineStepNode,
    TableNode,
    TableColumn
)

_PRIMITIVE_STEP_TYPES = (
    BatchGenerate,
    BatchTransform,
    DatatableBatchTransform,
    DatatableTransform,
    UpdateExternalTable,
)

SHOW_STEP_STATUS = True


@runtime_checkable
class LabeledPipelineStep(Protocol):
    labels: Labels | None


def _is_primitive_step(pipeline_step: PipelineStep) -> bool:
    return isinstance(pipeline_step, _PRIMITIVE_STEP_TYPES)


def _pipeline_step_group_name(pipeline_step: PipelineStep) -> str:
    return pipeline_step.__class__.__name__


def _tables_for_steps(step_list: List[ComputeStep]) -> Set[str]:
    used: Set[str] = set()
    for step in step_list:
        used.update(i.dt.name for i in step.input_dts)
        used.update(o.dt.name for o in step.output_dts)
    return used


def _group_boundaries(group_steps: List[ComputeStep]) -> tuple[Set[str], Set[str]]:
    produced = {o.dt.name for step in group_steps for o in step.output_dts}
    consumed = {i.dt.name for step in group_steps for i in step.input_dts}
    return consumed - produced, produced - consumed


def filter_steps_by_labels(
    steps: List[ComputeStep],
    labels: Labels = [],
    name_prefix: str = "",
) -> List[ComputeStep]:
    """Filter steps by label pairs (v1alpha3 semantics).

    Values for the *same* key are OR'd (match any); different keys are AND'd.
    """
    by_key: dict[str, set[str]] = {}
    for key, value in labels:
        by_key.setdefault(key, set()).add(value)

    res: List[ComputeStep] = []
    for step in steps:
        step_pairs = set(step.labels or [])
        matched = True
        for key, values in by_key.items():
            step_values = {v for k, v in step_pairs if k == key}
            if step_values.isdisjoint(values):
                matched = False
                break
        if matched and step.name.startswith(name_prefix):
            res.append(step)

    return res


def get_table_store_schema(table_store: TableStore) -> List[TableColumn]:
    if isinstance(table_store, TableStoreDB):
        return [
            TableColumn(name=column.name, type=str(column.type))
            for column in table_store.data_sql_schema
        ]

    return [
        TableColumn(name=column.name, type=str(column.type))
        for column in table_store.get_primary_schema()
    ]


def extract_stages(steps: list[ComputeStep]) -> list[str]:
    stages: list[str] = []
    seen: set[str] = set()
    for step in steps:
        for k, v in step.labels:
            if k == "stage" and v not in seen:
                stages.append(v)
                seen.add(v)
    return stages


def pipeline_step_labels(step: PipelineStep) -> Labels:
    if isinstance(step, LabeledPipelineStep):
        return step.labels or []
    return []


def table_node(ds: DataStore, catalog: Catalog, table_name: str) -> TableNode:
    tbl = catalog.get_datatable(ds, table_name)

    return TableNode(
        name=tbl.name,
        indexes=tbl.primary_keys,
        size=None,
        store_class=tbl.table_store.__class__.__name__,
        schema=get_table_store_schema(tbl.table_store),
    )

def pipeline_step_node(ds: DataStore, step: ComputeStep) -> PipelineNode:
    inputs = [i.dt.name for i in step.input_dts]
    outputs = [o.dt.name for o in step.output_dts]
    step_labels = [[k, v] for k, v in (step.labels or [])]

    if isinstance(step, BaseBatchTransformStep):
        step_status = None
        if SHOW_STEP_STATUS:
            try:
                step_status = step.get_status(ds=ds)
            except Exception:
                step_status = None

        return PipelineStepNode(
            type="transform",
            transform_type=step.__class__.__name__,
            name=step.get_name(),
            indexes=step.transform_keys,
            inputs=inputs,
            outputs=outputs,
            labels=step_labels,
            has_transform_meta=True,
            total_idx_count=(step_status.total_idx_count if step_status else None),
            changed_idx_count=(step_status.changed_idx_count if step_status else None),
        )

    return PipelineStepNode(
        type="transform",
        transform_type=step.__class__.__name__,
        name=step.get_name(),
        inputs=inputs,
        outputs=outputs,
        labels=step_labels,
    )


def get_pipeline_graph(app: DatapipeApp, labels: Dict[str, str]) -> Graph:
    selected_steps = (
        filter_steps_by_labels(app.steps, labels=labels.items()) 
        if labels 
        else app.steps
    )

    selected_names = {step.get_name() for step in selected_steps}
    pipeline_nodes: List[PipelineNode] = []
    top_level_tables: Set[str] = set()

    for pipeline_step in app.pipeline.steps:
        group_steps = pipeline_step.build_compute(app.ds, app.catalog)
        visible = [step for step in group_steps if step.get_name() in selected_names]
        if not visible:
            continue

        if len(group_steps) <= 1 and _is_primitive_step(pipeline_step):
            pipeline_nodes.append(pipeline_step_node(app.ds, visible[0]))
            top_level_tables.update(_tables_for_steps(visible))
            continue

        group_name = _pipeline_step_group_name(pipeline_step)
        external_inputs, external_outputs = _group_boundaries(visible)
        internal_tables = _tables_for_steps(visible)
        subgroup = Graph(
            catalog={table_name: table_node(app.ds, app.catalog, table_name) for table_name in sorted(internal_tables)},
            pipeline=[pipeline_step_node(app.ds, step) for step in visible],
            stages=extract_stages(visible),
        )
        pipeline_nodes.append(
            MetaPipelineStepNode(
                type="meta",
                name=group_name,
                transform_type=group_name,
                inputs=sorted(external_inputs),
                outputs=sorted(external_outputs),
                labels=[[k, v] for k, v in pipeline_step_labels(pipeline_step)],
                graph=subgroup,
            )
        )
        top_level_tables.update(external_inputs)
        top_level_tables.update(external_outputs)

    catalog_names = (
        [name for name in app.catalog.catalog.keys() if name in top_level_tables]
        if labels
        else list(app.catalog.catalog.keys())
    )

    return Graph(
        catalog={table_name: table_node(app.ds, app.catalog, table_name) for table_name in catalog_names},
        pipeline=pipeline_nodes,
        stages=extract_stages(selected_steps),
    )
