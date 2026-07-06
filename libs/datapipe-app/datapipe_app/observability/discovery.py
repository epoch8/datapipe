from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from datapipe.compute import Catalog, ComputeStep, Pipeline
from datapipe.datatable import DataStore

from datapipe_app.pipeline_steps import pipeline_step_labels


def extract_stages(steps: list[ComputeStep]) -> list[str]:
    stages: list[str] = []
    seen: set[str] = set()
    for step in steps:
        for k, v in step.labels:
            if k == "stage" and v not in seen:
                stages.append(v)
                seen.add(v)
    return stages


def stage_status_for_step(
    step: ComputeStep,
    ds: DataStore,
    cache: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    # step.get_status(ds) hits the datastore; the same step is queried by both the
    # stage summary and the label graph (and across overlapping stage labels), so
    # an optional per-request cache keyed by step name avoids redundant DB calls.
    if cache is not None and step.name in cache:
        return cache[step.name]
    try:
        status = step.get_status(ds)
        result = {
            "name": step.name,
            "total_idx_count": status.total_idx_count,
            "changed_idx_count": status.changed_idx_count,
            "has_backlog": status.changed_idx_count > 0,
        }
    except Exception:
        result = {"name": step.name, "total_idx_count": 0, "changed_idx_count": 0, "has_backlog": False}
    if cache is not None:
        cache[step.name] = result
    return result


def build_stage_summary(
    steps: list[ComputeStep],
    ds: DataStore,
    status_cache: Optional[dict[str, dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    stage_steps: dict[str, list[ComputeStep]] = defaultdict(list)
    for step in steps:
        stage_names = [v for k, v in step.labels if k == "stage"]
        if not stage_names:
            continue
        for stage in stage_names:
            stage_steps[stage].append(step)

    result = []
    stage_order = extract_stages(steps)
    for stage in stage_order:
        stage_step_list = stage_steps.get(stage)
        if not stage_step_list:
            continue
        statuses = [stage_status_for_step(s, ds, status_cache) for s in stage_step_list]
        has_backlog = any(s["has_backlog"] for s in statuses)
        state = "pending" if has_backlog else "completed"
        result.append(
            {
                "stage": stage,
                "status": state,
                "steps": statuses,
            }
        )
    return result


def build_stage_edges(steps: list[ComputeStep]) -> list[dict[str, Any]]:
    """Derive stage-to-stage edges from shared tables between compute steps."""
    step_stages: dict[str, set[str]] = {}
    table_producers: dict[str, set[str]] = defaultdict(set)
    table_consumers: dict[str, set[str]] = defaultdict(set)

    for step in steps:
        stages = {v for k, v in step.labels if k == "stage"}
        step_stages[step.name] = stages
        for output_dt in step.output_dts:
            table_producers[output_dt.dt.name].add(step.name)
        for input_dt in step.input_dts:
            table_consumers[input_dt.dt.name].add(step.name)

    edge_counts: dict[tuple[str, str], int] = defaultdict(int)
    for table, producers in table_producers.items():
        consumers = table_consumers.get(table, set())
        for producer in producers:
            for consumer in consumers:
                if producer == consumer:
                    continue
                from_stages = step_stages.get(producer, set())
                to_stages = step_stages.get(consumer, set())
                for src in from_stages:
                    for dst in to_stages:
                        if src != dst:
                            edge_counts[(src, dst)] += 1

    return [
        {"from": src, "to": dst, "count": count}
        for (src, dst), count in edge_counts.items()
    ]


def discover_pipeline_stages(
    pipeline: Optional[Pipeline],
) -> list[str]:
    if pipeline is None:
        return []
    stages: list[str] = []
    seen: set[str] = set()
    for step in pipeline.steps:
        for k, v in pipeline_step_labels(step):
            if k == "stage" and v not in seen:
                stages.append(v)
                seen.add(v)
    return stages
