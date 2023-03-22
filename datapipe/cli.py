from typing import Any, Dict, Iterator, List
from typing import Optional, List

import os.path
import sys
import time
import importlib.metadata as metadata

import click
import rich
from rich import print as rprint

from datapipe.types import Labels
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export import ConsoleSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.sdk.resources import Resource

from datapipe.compute import ComputeStep, run_steps
from datapipe_app import DatapipeApp
from datapipe.compute import ComputeStep


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
        raise Exception(f"Expected PIPELINE in format 'module:app' got '{pipeline_name}'")

    from importlib import import_module

    sys.path.append(os.getcwd())

    pipeline_mod = import_module(module_name)
    app = getattr(pipeline_mod, app_name)

    assert isinstance(app, DatapipeApp)

    return app


def parse_labels(labels: str) -> Labels:
    if labels == "":
        return []

    labels_list = []

    for kv in labels.split(","):
        if "=" not in kv:
            raise Exception(f"Expected labels in format 'key=value,key2=value2' got '{labels}'")

        k, v = kv.split("=")
        labels_list.append((k, v))

    return labels_list


def filter_steps_by_labels_and_name(app: DatapipeApp, labels: Labels = [], name_prefix: str = "") -> List[ComputeStep]:
    res = []

    for step in app.steps:
        for k, v in labels:
            if (k, v) not in step.labels:
                break
        else:
            if step.name.startswith(name_prefix):
                res.append(step)

    return res


@click.group()
@click.option("--debug", is_flag=True, help="Log debug output")
@click.option("--debug-sql", is_flag=True, help="Log SQL queries VERY VERBOSE")
@click.option("--trace-stdout", is_flag=True, help="Log traces to console")
@click.option("--trace-jaeger", is_flag=True, help="Enable tracing to Jaeger")
@click.option("--trace-jaeger-host", type=click.STRING, default="localhost", help="Jaeger host")
@click.option("--trace-jaeger-port", type=click.INT, default=14268, help="Jaeger port")
@click.option("--trace-gcp", is_flag=True, help="Enable tracing to Google Cloud Trace")
@click.option("--pipeline", type=click.STRING, default="app")
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
) -> None:
    import logging

    if debug:
        datapipe_logger = logging.getLogger("datapipe")
        datapipe_logger.setLevel(logging.DEBUG)

        datapipe_core_steps_logger = logging.getLogger("datapipe.core_steps")
        datapipe_core_steps_logger.setLevel(logging.DEBUG)

        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if debug_sql:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    trace.set_tracer_provider(TracerProvider(resource=Resource.create({SERVICE_NAME: "datapipe"})))

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

    ctx.ensure_object(dict)
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


@table.command()
@click.argument("table")
@click.pass_context
def reset_metadata(ctx: click.Context, table: str) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]

    dt = app.catalog.get_datatable(app.ds, table)
    dt.reset_metadata()


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

    tables_from_catalog = app.catalog.catalog.keys()
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
                    except:
                        rprint("[red]... FAILED TO FIX[/red]", end="")

                print()
            print()


@cli.group()
@click.option("--labels", type=click.STRING, default="")
@click.option("--name", type=click.STRING, default="")
@click.pass_context
def step(ctx: click.Context, labels: str, name: str):
    app: DatapipeApp = ctx.obj["pipeline"]

    labels_dict = parse_labels(labels)
    steps = filter_steps_by_labels_and_name(app, labels=labels_dict, name_prefix=name)

    ctx.obj["steps"] = steps


def to_human_repr(step) -> Dict:
    res: Dict[str, Any] = {"name": f"[green][bold]{step.name}[/bold][/green]"}

    if step.labels:
        res["labels"] = " ".join([f"[magenta]{k}={v}[/magenta]" for (k, v) in step.labels])

    if inputs := [i.name for i in step.input_dts]:
        res["inputs"] = ", ".join(inputs)

    if outputs := [i.name for i in step.output_dts]:
        res["outputs"] = ", ".join(outputs)

    return res


@step.command()  # type: ignore
@click.pass_context
def list(ctx: click.Context) -> None:
    from yaml import dump

    app: DatapipeApp = ctx.obj["pipeline"]
    steps: List[ComputeStep] = ctx.obj["steps"]

    out: Dict = {"steps": []}

    for step in steps:
        out["steps"].append(to_human_repr(step))

    rprint(dump(out, sort_keys=False))


@step.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]
    steps: List[ComputeStep] = ctx.obj["steps"]

    if len(steps) > 0:
        for step in steps:
            labels = f"\t{step.labels}" if step.labels else ""
            print(
                f"{step.name} {labels}\t{tuple(i.name for i in step.input_dts)} -> {tuple(i.name for i in step.output_dts)}"
            )

            if len(step.input_dts) > 0:
                changed_idx_count = app.ds.get_changed_idx_count(
                    inputs=step.input_dts,
                    outputs=step.output_dts,
                )

                print(f"Idx to process: {changed_idx_count}")


@step.command()  # type: ignore
@click.option("--loop", is_flag=True, default=False, help="Run continuosly in a loop")
@click.option("--loop-delay", type=click.INT, default=30, help="Delay between loops in seconds")
@click.pass_context
def run(ctx: click.Context, loop: bool, loop_delay: int) -> None:
    app: DatapipeApp = ctx.obj["pipeline"]
    steps_to_run: List[ComputeStep] = ctx.obj["steps"]
    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]
    print(f"Running following steps: {', '.join(steps_to_run_names)}")

    while True:
        if len(steps_to_run) > 0:
            run_steps(app.ds, steps_to_run)

        if not loop:
            break
        else:
            print(f"Loop ended, sleeping {loop_delay}s...")
            time.sleep(loop_delay)
            print("\n\n")


def get_steps_range(steps: List[ComputeStep], first_step: Optional[str], last_step: Optional[str]) -> List[ComputeStep]:
    if first_step is None and last_step is None:
        print("Missing arguments: FIRST_STEP or LAST_STEP, running all graph.")
        steps_to_run = steps
    if first_step is not None:
        first_step_idxs = [idx for idx, i in enumerate(steps) if i.name.startswith(first_step)]
        if len(first_step_idxs) == 0:
            print(f"There's no step with name '{first_step}'")
            return []
        first_index = first_step_idxs[0]
    else:
        first_index = None
    if last_step is not None:
        last_step_idxs = [idx for idx, i in enumerate(steps) if i.name.startswith(last_step)]
        if len(last_step_idxs) == 0:
            print(f"There's no step with name '{last_step}'")
            return []
        last_index = last_step_idxs[-1] + 1
    else:
        last_index = None

    if first_index is not None and last_index is not None and first_index > last_index:
        print(f"Step with name '{last_step}' is located before step with name '{step}'")
        return []

    steps_to_run = steps[first_index:last_index]
    return steps_to_run


@step.command()  # type:ignore
@click.option("--first-step", type=click.STRING, default=None, required=False)
@click.option("--last-step", type=click.STRING, default=None, required=False)
@click.option("--pipeline", type=click.STRING, default="app")
def run_in_range(pipeline: str, first_step: Optional[str] = None, last_step: Optional[str] = None) -> None:

    app = load_pipeline(pipeline)

    steps_to_run = get_steps_range(app.steps, first_step, last_step)
    steps_to_run_names = [f"'{i.name}'" for i in steps_to_run]
    print(f"Running following steps: {', '.join(steps_to_run_names)}")

    run_steps(app.ds, steps_to_run)


for entry_point in metadata.entry_points().get("datapipe.cli", []):
    register_commands = entry_point.load()
    register_commands(cli)


def main():
    cli(auto_envvar_prefix="DATAPIPE")
