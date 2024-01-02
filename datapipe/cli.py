import importlib.metadata as metadata
import os.path
import sys
import time
from typing import Dict, List, Optional, cast

import click
import pandas as pd
import rich
from opentelemetry import trace
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from rich import print as rprint
from tqdm_loggable.auto import tqdm

from datapipe.compute import ComputeStep, DatapipeApp, run_steps, run_steps_changelist
from datapipe.executor import Executor, SingleThreadExecutor
from datapipe.migrations import v013 as migrations_v013
from datapipe.step.batch_transform import BaseBatchTransformStep
from datapipe.types import IndexDF, Labels

tracer = trace.get_tracer("datapipe_app")

rich.reconfigure(highlight=False)


def load_pipeline(pipeline_name: str) -> DatapipeApp:
    pipeline_split = pipeline_name.split(":")

    if len(pipeline_split) == 1:
        module_name = pipeline_split[0]
        app_name = "app"
    elif len(pipeline_split) == 2:
        module_name, app_name = pipeline_split
    else:
        raise Exception(
            f"Expected PIPELINE in format 'module:app' got '{pipeline_name}'"
        )

    from importlib import import_module

    sys.path.append(os.getcwd())

    pipeline_mod = import_module(module_name)
    app = getattr(pipeline_mod, app_name)

    assert isinstance(app, DatapipeApp)

    return app


def parse_labels(labels: Optional[str]) -> Labels:
    if labels is None or labels == "":
        return []

    labels_list = []

    for kv in labels.split(","):
        if "=" not in kv:
            raise Exception(
                f"Expected labels in format 'key=value,key2=value2' got '{labels}'"
            )

        k, v = kv.split("=")
        labels_list.append((k, v))

    return labels_list


def filter_steps_by_labels_and_name(
    app: DatapipeApp,
    labels: Labels = [],
    name_prefix: Optional[str] = None,
) -> List[ComputeStep]:
    res = []

    for step in app.steps:
        for k, v in labels:
            if (k, v) not in step.labels:
                break
        else:
            if name_prefix is None or step.name.startswith(name_prefix):
                res.append(step)

    return res


@click.group()
@click.option("--debug", is_flag=True, help="Log debug output")
@click.option("--debug-sql", is_flag=True, help="Log SQL queries VERY VERBOSE")
@click.option("--trace-stdout", is_flag=True, help="Log traces to console")
@click.option("--trace-jaeger", is_flag=True, help="Enable tracing to Jaeger")
@click.option(
    "--trace-jaeger-host", type=click.STRING, default="localhost", help="Jaeger host"
)
@click.option("--trace-jaeger-port", type=click.INT, default=14268, help="Jaeger port")
@click.option("--trace-gcp", is_flag=True, help="Enable tracing to Google Cloud Trace")
@click.option("--pipeline", type=click.STRING, default="app")
@click.option("--executor", type=click.STRING, default="SingleThreadExecutor")
@click.pass_context
def cli(
    ctx: click.Context,
    debug: bool,
    debug_sql: bool,
    trace_stdout: bool,
    trace_jaeger: bool,
    trace_jaeger_host: str,
    trace_jaeger_port: int,
    trace_gcp: bool,
    pipeline: str,
    executor: str,
) -> None:
    ctx.ensure_object(dict)

    def setup_logging():
        import logging

        if debug:
            datapipe_logger = logging.getLogger("datapipe")
            datapipe_logger.setLevel(logging.DEBUG)

            datapipe_core_steps_logger = logging.getLogger("datapipe.core_steps")
            datapipe_core_steps_logger.setLevel(logging.DEBUG)

            logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
        else:
            for logger_name in [None, "datapipe", "tqdm_loggable"]:
                logger = logging.getLogger(logger_name)
                logger.setLevel(logging.INFO)
            logging.basicConfig(level=logging.INFO, stream=sys.stderr)

        if debug_sql:
            logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    setup_logging()

    trace.set_tracer_provider(
        TracerProvider(resource=Resource.create({SERVICE_NAME: "datapipe"}))
    )

    SQLAlchemyInstrumentor().instrument()

    if trace_stdout:
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        trace.get_tracer_provider().add_span_processor(processor)  # type: ignore

    if trace_jaeger:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter  # type: ignore

        # create a JaegerExporter
        jaeger_exporter = JaegerExporter(
            # configure agent
            # agent_host_name='localhost',
            # agent_port=6831,
            # optional: configure also collector
            collector_endpoint=f"http://{trace_jaeger_host}:{trace_jaeger_port}/api/traces?format=jaeger.thrift",
            # username=xxxx, # optional
            # password=xxxx, # optional
            # max_tag_value_length=None # optional
        )

        # Create a BatchSpanProcessor and add the exporter to it
        span_processor = BatchSpanProcessor(jaeger_exporter)

        # add to the tracer
        trace.get_tracer_provider().add_span_processor(span_processor)  # type: ignore

    if trace_gcp:
        from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter

        cloud_trace_exporter = CloudTraceSpanExporter(
            resource_regex=r".*",
        )
        trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(cloud_trace_exporter))  # type: ignore

    if executor == "SingleThreadExecutor":
        ctx.obj["executor"] = SingleThreadExecutor()
    elif executor == "RayExecutor":
        import ray

        from datapipe.executor.ray import RayExecutor

        ray_ctx = ray.init()

        ctx.obj["executor"] = RayExecutor()
    else:
        raise ValueError(f"Unknown executor: {executor}")

    with tracer.start_as_current_span("init"):
        ctx.obj["pipeline"] = load_pipeline(pipeline)


@cli.group()
def table():
    pass


@table.command()
@click.pass_context
def list(ctx: click.Context) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    for table in sorted(app.catalog.catalog.keys()):
        print(table)


@cli.command()
@click.pass_context
def run(ctx: click.Context) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    with tracer.start_as_current_span("run"):
        from datapipe.compute import run_steps

        run_steps(app.ds, app.steps)


@cli.group()
def db():
    pass


@db.command()
@click.pass_context
def create_all(ctx: click.Context) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    app.ds.meta_dbconn.sqla_metadata.create_all(app.ds.meta_dbconn.con)


@cli.command()
@click.option("--tables", type=click.STRING, default="*")
@click.option("--fix", is_flag=True, type=click.BOOL, default=False)
@click.pass_context
def lint(ctx: click.Context, tables: str, fix: bool) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    from . import lints

    checks = [
        lints.LintDeleteTSIsNewerThanUpdateOrProcess(),
        lints.LintDataWOMeta(),
    ]

    tables_from_catalog = list(app.catalog.catalog.keys())
    print(f"Pipeline contains {len(tables_from_catalog)} tables")

    if tables == "*":
        tables_to_process = tables_from_catalog
    else:
        tables_to_process = tables.split(",")

    for table_name in sorted(tables_to_process):
        print(f"Checking '{table_name}': ", end="")

        dt = app.catalog.get_datatable(app.ds, table_name)

        errors = []

        for check in checks:
            (status, msg) = check.check(dt)

            if status == lints.LintStatus.OK:
                print(".", end="")
            elif status == lints.LintStatus.SKIP:
                print("S", end="")
            elif status == lints.LintStatus.FAIL:
                rprint("[red]F[/red]", end="")
                errors.append((check, msg))

        if len(errors) == 0:
            rprint("[green] ok[/green]")
        else:
            rprint("[red] FAIL[/red]")
            for check, msg in errors:
                print(f" * {check.desc}: {msg}", end="")

                if fix:
                    try:
                        (fix_status, fix_msg) = check.fix(dt)
                        if fix_status == lints.LintStatus.OK:
                            rprint("[green]... FIXED[/green]", end="")
                        elif fix_status == lints.LintStatus.SKIP:
                            rprint("[yellow]... SKIPPED[/yellow]", end="")
                        else:
                            rprint("[red]... FAILED TO FIX[/red]", end="")

                            if fix_msg:
                                print(fix_msg, end="")
                    except:  # noqa
                        rprint("[red]... FAILED TO FIX[/red]", end="")

                print()
            print()


@cli.group()
@click.option("--labels", type=click.STRING)
@click.option("--name", type=click.STRING)
@click.pass_context
def step(
    ctx: click.Context,
    labels: str,
    name: str,
) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    labels_list = parse_labels(labels)
    steps = filter_steps_by_labels_and_name(app, labels=labels_list, name_prefix=name)

    ctx.obj["steps"] = steps


def to_human_repr(step: ComputeStep, extra_args: Optional[Dict] = None) -> str:
    res = []

    res.append(f"[green][bold]{step.name}[/bold][/green] ({step.__class__.__name__})")

    if step.labels:
        labels = " ".join([f"[magenta]{k}={v}[/magenta]" for (k, v) in step.labels])
        res.append(f"  labels: {labels}")

    if inputs_arr := [i.name for i in step.input_dts]:
        inputs = ", ".join(inputs_arr)
        res.append(f"  inputs: {inputs}")

    if outputs_arr := [i.name for i in step.output_dts]:
        outputs = ", ".join(outputs_arr)
        res.append(f"  outputs: {outputs}")

    if extra_args is not None:
        for k, v in extra_args.items():
            res.append(f"  {k}: {v}")

    return "\n".join(res)


@step.command()  # type: ignore
@click.option("--status", is_flag=True, type=click.BOOL, default=False)
@click.pass_context
def list(ctx: click.Context, status: bool) -> None:  # noqa
    app: DatapipeApp = ctx.obj["pipeline"]
    steps: List[ComputeStep] = ctx.obj["steps"]

    for step in steps:
        extra_args = {}

        if status:
            if len(step.input_dts) > 0:
                try:
                    if isinstance(step, BaseBatchTransformStep):
                        changed_idx_count = step.get_changed_idx_count(ds=app.ds)

                        if changed_idx_count > 0:
                            extra_args[
                                "changed_idx_count"
                            ] = f"[red]{changed_idx_count}[/red]"

                except NotImplementedError:
                    # Currently we do not support empty join_keys
                    extra_args["changed_idx_count"] = "[red]N/A[/red]"

        rprint(to_human_repr(step, extra_args=extra_args))
        rprint("")


@step.command()  # type: ignore
@click.option("--loop", is_flag=True, default=False, help="Run continuosly in a loop")
@click.option(
    "--loop-delay", type=click.INT, default=30, help="Delay between loops in seconds"
)
@click.pass_context
def run(ctx: click.Context, loop: bool, loop_delay: int) -> None:  # noqa
    app: DatapipeApp = ctx.obj["pipeline"]
    steps_to_run: List[ComputeStep] = ctx.obj["steps"]

    executor: Executor = ctx.obj["executor"]

    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]
    print(f"Running following steps: {', '.join(steps_to_run_names)}")

    while True:
        if len(steps_to_run) > 0:
            run_steps(app.ds, steps_to_run, executor=executor)

        if not loop:
            break
        else:
            print(f"Loop ended, sleeping {loop_delay}s...")
            time.sleep(loop_delay)
            print("\n\n")


@step.command()  # type: ignore
@click.argument("idx", type=click.STRING)
@click.pass_context
def run_idx(ctx: click.Context, idx: str) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]
    steps_to_run: List[ComputeStep] = ctx.obj["steps"]
    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]
    print(f"Running following steps: {', '.join(steps_to_run_names)}")

    idx_dict = {k: v for k, v in (i.split("=") for i in idx.split(","))}

    for step in steps_to_run:
        if isinstance(step, BaseBatchTransformStep):
            step.run_idx(ds=app.ds, idx=cast(IndexDF, pd.DataFrame([idx_dict])))


@step.command()  # type: ignore
@click.option("--loop", is_flag=True, default=False, help="Run continuosly in a loop")
@click.option(
    "--loop-delay", type=click.INT, default=1, help="Delay between loops in seconds"
)
@click.option("--chunk-size", type=click.INT, default=None, help="Chunk size")
@click.option("--start-step", type=click.STRING)
@click.pass_context
def run_changelist(
    ctx: click.Context,
    start_step: str,
    loop: bool,
    loop_delay: int,
    chunk_size: Optional[int] = None,
) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    steps_to_run: List[ComputeStep] = ctx.obj["steps"]
    if start_step is not None:
        start_step_objs = filter_steps_by_labels_and_name(
            app, labels=[], name_prefix=start_step
        )
        assert len(start_step_objs) == 1
    else:
        start_step_objs = [steps_to_run[0]]

    start_step_obj = start_step_objs[0]
    assert isinstance(start_step_obj, BaseBatchTransformStep)

    if start_step_obj not in steps_to_run:
        steps_to_run = [start_step_obj] + steps_to_run

    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]

    print(f"Running following steps: {', '.join(steps_to_run_names)}")

    executor: Executor = ctx.obj["executor"]

    idx_count, idx_gen = start_step_obj.get_full_process_ids(
        app.ds,
        chunk_size=chunk_size,
    )
    cnt = 0

    while True:
        for idx in tqdm(idx_gen, total=idx_count):
            changes = start_step_obj.run_idx(
                ds=app.ds,
                idx=idx,
                executor=executor,
            )

            run_steps_changelist(app.ds, steps_to_run, changes, executor=executor)

            rprint(f"Chunk {cnt}/{idx_count} ended")
            cnt += 1

        if not loop:
            break
        else:
            rprint(f"All chunks ended, sleeping {loop_delay}s...")
            time.sleep(loop_delay)
            print("\n\n")


@step.command()
@click.pass_context
def fill_metadata(ctx: click.Context) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]
    steps_to_run: List[ComputeStep] = ctx.obj["steps"]
    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]

    for step in steps_to_run:
        if isinstance(step, BaseBatchTransformStep):
            rprint(
                f"Filling metadata for step: [bold][green]{step.name}[/green][/bold]"
            )
            step.fill_metadata(app.ds)


@step.command()  # type: ignore
@click.pass_context
def reset_metadata(ctx: click.Context) -> None:  # noqa
    app: DatapipeApp = ctx.obj["pipeline"]
    steps_to_run: List[ComputeStep] = ctx.obj["steps"]
    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]
    print(f"Resetting following steps: {', '.join(steps_to_run_names)}")

    for step in steps_to_run:
        if isinstance(step, BaseBatchTransformStep):
            step.reset_metadata(app.ds)


@table.command()
@click.pass_context
@click.option("--name", type=click.STRING)
@click.option("--labels", type=click.STRING)
def migrate_transform_tables(ctx: click.Context, labels: str, name: str) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]
    labels_dict = parse_labels(labels)
    batch_transforms_steps = filter_steps_by_labels_and_name(
        app, labels=labels_dict, name_prefix=name
    )

    return migrations_v013.migrate_transform_tables(app, batch_transforms_steps)


for entry_point in metadata.entry_points().get("datapipe.cli", []):
    register_commands = entry_point.load()
    register_commands(cli)


def main():
    cli(auto_envvar_prefix="DATAPIPE")


if __name__ == "__main__":
    main()
