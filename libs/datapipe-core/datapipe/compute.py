import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, Literal, Sequence

from opentelemetry import trace
from sqlalchemy import Column

from datapipe.datatable import DataStore, DataTable
from datapipe.executor import Executor, ExecutorConfig
from datapipe.run_config import RunConfig
from datapipe.store.database import TableStoreDB
from datapipe.store.table_store import TableStore
from datapipe.types import (
    ChangeList,
    IndexDF,
    InputSpec,
    Labels,
    MetaSchema,
    OutputSpec,
    PipelineInput,
    PipelineOutput,
    Required,
    TableOrName,
    get_pipeline_input_table,
    get_pipeline_output_table,
)

logger = logging.getLogger("datapipe.compute")
tracer = trace.get_tracer("datapipe.compute")


@dataclass
class Table:
    store: TableStore
    name: str | None = None


class Catalog:
    def __init__(self, catalog: dict[str, Table]):
        self.catalog = catalog
        self.data_tables: dict[str, DataTable] = {}

    def add_datatable(self, name: str, dt: Table):
        self.catalog[name] = dt

    def remove_datatable(self, name: str):
        del self.catalog[name]

    def init_all_tables(self, ds: DataStore):
        for name in self.catalog.keys():
            self.get_datatable(ds, name)

    def get_datatable(self, ds: DataStore, table: TableOrName) -> DataTable:
        if isinstance(table, str):
            assert table in self.catalog, f"Table {table} not found in catalog"
            return ds.get_or_create_table(name=table, table_store=self.catalog[table].store)

        elif isinstance(table, Table):
            assert table.name is not None, f"Table name must be specified for {table}"

            if table.name not in self.catalog:
                self.add_datatable(table.name, table)
            else:
                existing_table = self.catalog[table.name]
                assert existing_table.store == table.store, (
                    f"Table {table.name} already exists in catalog with different store {existing_table.store}"
                )

            return ds.get_or_create_table(name=table.name, table_store=table.store)

        else:
            table_store = TableStoreDB(ds.meta_dbconn, orm_table=table)
            if table_store.name not in self.catalog:
                self.add_datatable(table_store.name, Table(store=table_store))
            else:
                existing_table_store = self.catalog[table_store.name].store
                assert isinstance(existing_table_store, TableStoreDB), (
                    f"Table {table_store.name} already exists in catalog with different store {existing_table_store}"
                )

                assert existing_table_store.data_table == table.__table__, (  # type: ignore
                    f"Table {table_store.name} already exists in catalog "
                    f"with different orm_table {existing_table_store.data_table}"
                )

            return ds.get_or_create_table(name=table_store.name, table_store=table_store)


@dataclass
class StepStatus:
    name: str
    total_idx_count: int
    changed_idx_count: int


@dataclass
class ComputeInput:
    dt: DataTable
    join_type: Literal["inner", "full"] = "full"

    # If provided, this dict tells how to get key columns from meta table
    #
    # Example: {"idx_col": "meta_col"} means that to get idx_col value
    # we should read meta_col from meta table
    keys: dict[str, str] | None = None

    @property
    def primary_keys(self) -> list[str]:
        if self.keys:
            return list(self.keys.keys())
        else:
            return self.dt.primary_keys

    @property
    def primary_schema(self) -> MetaSchema:
        if self.keys:
            primary_schema_dict = {col.name: col for col in self.dt.primary_schema}

            schema = []
            for k, accessor in self.keys.items():
                source_column = primary_schema_dict[accessor]
                schema.append(
                    Column(
                        k,
                        source_column.type,
                        primary_key=source_column.primary_key,
                    )
                )
            return schema
        else:
            return self.dt.primary_schema


@dataclass
class ComputeOutput:
    dt: DataTable

    # If provided, this dict tells how transform keys map to output primary keys.
    #
    # Example: {"post_id": "id"} means that cleanup for transform key post_id
    # should be applied to output primary key id.
    keys: dict[str, str] | None = None

    @property
    def primary_keys(self) -> list[str]:
        if self.keys:
            return list(self.keys.keys())
        else:
            return self.dt.primary_keys

    @property
    def primary_schema(self) -> MetaSchema:
        if self.keys:
            primary_schema_dict = {col.name: col for col in self.dt.primary_schema}

            schema = []
            for k, accessor in self.keys.items():
                source_column = primary_schema_dict[accessor]
                schema.append(
                    Column(
                        k,
                        source_column.type,
                        primary_key=source_column.primary_key,
                    )
                )
            return schema
        else:
            return self.dt.primary_schema


def make_mungled_step_name(
    cls, base_name: str, input_dts: Sequence[ComputeInput], output_dts: Sequence[ComputeOutput]
) -> str:
    ss = [
        cls.__name__,
        base_name,
        *[i.dt.name for i in input_dts],
        *[o.dt.name for o in output_dts],
    ]

    m = hashlib.shake_128()
    m.update("".join(ss).encode("utf-8"))

    return f"{base_name}_{m.hexdigest(5)}"


def pipeline_input_to_compute_input(ds: DataStore, catalog: Catalog, input: PipelineInput) -> ComputeInput:
    table = get_pipeline_input_table(input)
    keys = input.keys if isinstance(input, InputSpec) else None
    join_type: Literal["inner", "full"] = "inner" if isinstance(input, Required) else "full"
    return ComputeInput(dt=catalog.get_datatable(ds, table), join_type=join_type, keys=keys)


def pipeline_output_to_compute_output(ds: DataStore, catalog: Catalog, output: PipelineOutput) -> ComputeOutput:
    table = get_pipeline_output_table(output)
    keys = output.keys if isinstance(output, OutputSpec) else None
    return ComputeOutput(dt=catalog.get_datatable(ds, table), keys=keys)


class ComputeStep:
    """
    Шаг вычислений в графе вычислений.

    Каждый шаг должен уметь отвечать на вопросы:
    - какие таблицы приходят на вход
    - какие таблицы изменяются в результате трансформации

    Шаг может запускаться в режиме полной обработки, то есть без указания какие
    объекты изменились. Или в changelist-режиме, когда на вход поступают
    измененные индексы для каждой из входных таблиц.

    В changelist-режиме шаг обрабатывает только минимально необходимое
    количество батчей, которые покрывают все измененные индексы.
    """

    def __init__(
        self,
        name: str,
        input_dts: Sequence[ComputeInput],
        output_dts: Sequence[ComputeOutput],
        labels: Labels | None = None,
        executor_config: ExecutorConfig | None = None,
    ) -> None:
        self.name = name
        # Нормализация input_dts: автоматически оборачиваем DataTable в ComputeInput
        self.input_dts = list(input_dts)
        self.output_dts = list(output_dts)
        self._labels = labels
        self.executor_config = executor_config

    def get_name(self) -> str:
        return self.name

    @property
    def labels(self) -> Labels:
        return self._labels if self._labels else []

    def get_status(self, ds: DataStore) -> StepStatus:
        raise NotImplementedError

    # TODO: move to lints
    def validate(self) -> None:
        inp_p_keys_arr = [set(inp.dt.primary_keys) for inp in self.input_dts if inp]
        out_p_keys_arr = [set(out.dt.primary_keys) for out in self.output_dts if out]

        inp_p_keys = set.intersection(*inp_p_keys_arr) if len(inp_p_keys_arr) else set()
        out_p_keys = set.intersection(*out_p_keys_arr) if len(out_p_keys_arr) else set()
        join_keys = set.intersection(inp_p_keys, out_p_keys)

        key_to_column_type_inp = {
            column.name: type(column.type)
            for inp in self.input_dts
            for column in inp.dt.primary_schema
            if column.name in join_keys
        }
        key_to_column_type_out = {
            column.name: type(column.type)
            for inp in self.output_dts
            for column in inp.dt.primary_schema
            if column.name in join_keys
        }

        for key in join_keys:
            if key_to_column_type_inp[key] != key_to_column_type_out[key]:
                raise ValueError(
                    f'Primary key "{key}" in inputs and outputs must have same column\'s type: '
                    f"{key_to_column_type_inp[key]} != {key_to_column_type_out[key]}"
                )

    def get_full_process_ids(
        self,
        ds: DataStore,
        chunk_size: int | None = None,
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
        raise NotImplementedError()

    def get_change_list_process_ids(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: RunConfig | None = None,
    ) -> tuple[int, Iterable[IndexDF]]:
        raise NotImplementedError()

    def run_full(
        self,
        ds: DataStore,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> None:
        raise NotImplementedError()

    def run_changelist(
        self,
        ds: DataStore,
        change_list: ChangeList,
        run_config: RunConfig | None = None,
        executor: Executor | None = None,
    ) -> ChangeList:
        raise NotImplementedError()

    def run_idx(
        self,
        ds: DataStore,
        idx: IndexDF,
        run_config: RunConfig | None = None,
    ) -> ChangeList:
        raise NotImplementedError()


class PipelineStep(ABC):
    """
    Пайплайн описывается через PipelineStep.

    Для выполнения у каждого pipeline_step выполняется build_compute на
    основании которых создается граф вычислений.
    """

    @abstractmethod
    def build_compute(self, ds: DataStore, catalog: Catalog) -> list[ComputeStep]:
        raise NotImplementedError


@dataclass
class Pipeline:
    steps: Sequence[PipelineStep]


class DatapipeApp:
    def __init__(self, ds: DataStore, catalog: Catalog, pipeline: Pipeline):
        self.ds = ds
        self.catalog = catalog
        self.pipeline = pipeline
        self.steps = build_compute(ds, catalog, pipeline)


def build_compute(ds: DataStore, catalog: Catalog, pipeline: Pipeline) -> list[ComputeStep]:
    with tracer.start_as_current_span("build_compute"):
        catalog.init_all_tables(ds)

        compute_pipeline: list[ComputeStep] = []

        seen_steps = []
        for step in pipeline.steps:
            compute_steps = step.build_compute(ds, catalog)
            compute_pipeline.extend(compute_steps)

            for compute_step in compute_steps:
                if compute_step.name in seen_steps:
                    raise Exception(f"Duplicate step name: {compute_step.name}")
                seen_steps.append(compute_step.name)

                # TODO move to lints
                compute_step.validate()

        return compute_pipeline


def print_compute(steps: list[ComputeStep]) -> None:
    import pprint

    pprint.pp(steps)


_run_steps_hooks: list = []


def register_run_steps_hook(hook) -> None:
    """Optional callback(hook) with on_step_start(step) / on_step_end(step, error=None)."""
    _run_steps_hooks.append(hook)


def run_steps(
    ds: DataStore,
    steps: Sequence[ComputeStep],
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None:
    for step in steps:
        for hook in _run_steps_hooks:
            if hasattr(hook, "on_step_start"):
                hook.on_step_start(step)
        try:
            with tracer.start_as_current_span(
                f"{step.name} {[i.dt.name for i in step.input_dts]} -> {[i.dt.name for i in step.output_dts]}"
            ):
                logger.info(
                    f"Running {step.name} {[i.dt.name for i in step.input_dts]} -> {[i.dt.name for i in step.output_dts]}"
                )

                step.run_full(ds=ds, run_config=run_config, executor=executor)
        except Exception as exc:
            for hook in _run_steps_hooks:
                if hasattr(hook, "on_step_end"):
                    hook.on_step_end(step, error=exc)
            raise
        else:
            for hook in _run_steps_hooks:
                if hasattr(hook, "on_step_end"):
                    hook.on_step_end(step, error=None)


def run_pipeline(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    run_config: RunConfig | None = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)
    run_steps(ds, steps, run_config)


def run_changelist(
    ds: DataStore,
    catalog: Catalog,
    pipeline: Pipeline,
    changelist: ChangeList,
    run_config: RunConfig | None = None,
) -> None:
    steps = build_compute(ds, catalog, pipeline)

    return run_steps_changelist(ds, steps, changelist, run_config)


def run_steps_changelist(
    ds: DataStore,
    steps: list[ComputeStep],
    changelist: ChangeList,
    run_config: RunConfig | None = None,
    executor: Executor | None = None,
) -> None:
    # FIXME extract Batch* steps to separate module
    from datapipe.step.batch_transform import BaseBatchTransformStep

    current_changes = changelist
    next_changes = ChangeList()
    iteration = 0

    with tracer.start_as_current_span("Start pipeline for changelist"):
        while not current_changes.empty() and iteration < 100:
            with tracer.start_as_current_span("run_steps"):
                for step in steps:
                    with tracer.start_as_current_span(
                        f"{step.name} {[i.dt.name for i in step.input_dts]} -> {[i.dt.name for i in step.output_dts]}"
                    ):
                        logger.info(
                            f"Running {step.name} "
                            f"{[i.dt.name for i in step.input_dts]} -> {[i.dt.name for i in step.output_dts]}"
                        )

                        if isinstance(step, BaseBatchTransformStep):
                            step_changes = step.run_changelist(
                                ds,
                                current_changes,
                                run_config,
                                executor=executor,
                            )
                            next_changes.extend(step_changes)

            current_changes = next_changes
            next_changes = ChangeList()
            iteration += 1
