from __future__ import annotations

from collections import defaultdict
from typing import Any

from datapipe.compute import ComputeStep
from datapipe.datatable import DataStore

from datapipe_app.observability.discovery import stage_status_for_step


def _label_values(step: ComputeStep, label_key: str) -> list[str]:
    return [v for k, v in step.labels if k == label_key]


def _build_segments(label_id: str, steps: list[ComputeStep], label_key: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    for order, step in enumerate(steps):
        has_label = label_id in _label_values(step, label_key)
        if has_label and current is None:
            current = {
                "label_id": label_id,
                "start_order": order,
                "end_order": order,
                "step_ids": [step.name],
            }
        elif has_label and current is not None:
            current["end_order"] = order
            current["step_ids"].append(step.name)
        elif not has_label and current is not None:
            result.append(current)
            current = None

    if current is not None:
        result.append(current)
    return result


def _label_status(step_list: list[ComputeStep], ds: DataStore) -> str:
    statuses = [stage_status_for_step(s, ds) for s in step_list]
    if any(s.get("has_backlog") for s in statuses):
        return "pending"
    return "completed"


def _find_containments(label_step_ids: dict[str, set[str]]) -> list[dict[str, Any]]:
    containments: list[dict[str, Any]] = []
    children_by_parent: dict[str, list[str]] = defaultdict(list)

    labels = list(label_step_ids.keys())
    for parent in labels:
        parent_steps = label_step_ids[parent]
        for child in labels:
            if parent == child:
                continue
            child_steps = label_step_ids[child]
            if child_steps < parent_steps:
                children_by_parent[parent].append(child)

    for parent, children in children_by_parent.items():
        if len(children) >= 2:
            for child in children:
                containments.append(
                    {"parent": parent, "child": child, "kind": "semantic"},
                )
    return containments


def _is_interleaved(segments_a: list[dict[str, Any]], segments_b: list[dict[str, Any]]) -> tuple[bool, int]:
    combined = sorted(segments_a + segments_b, key=lambda s: s["start_order"])
    if len(segments_a) < 2 or len(segments_b) < 2:
        return False, 0
    switches = 0
    for i in range(1, len(combined)):
        if combined[i]["label_id"] != combined[i - 1]["label_id"]:
            switches += 1
    return switches >= 3, switches


def build_label_graph(
    steps: list[ComputeStep],
    ds: DataStore,
    label_key: str = "stage",
) -> dict[str, Any]:
    """Build label-over-transform-order graph payload from pipeline steps."""
    label_steps: dict[str, list[ComputeStep]] = defaultdict(list)
    label_first_order: dict[str, int] = {}

    for order, step in enumerate(steps):
        for label in _label_values(step, label_key):
            if label not in label_first_order:
                label_first_order[label] = order
            label_steps[label].append(step)

    label_step_ids: dict[str, set[str]] = {
        label: {s.name for s in step_list} for label, step_list in label_steps.items()
    }
    step_orders = {step.name: i for i, step in enumerate(steps)}

    containments = _find_containments(label_step_ids)
    children_set = {c["child"] for c in containments}
    parent_children: dict[str, list[str]] = defaultdict(list)
    for c in containments:
        parent_children[c["parent"]].append(c["child"])

    segments_by_label = {
        label: _build_segments(label, steps, label_key) for label in label_steps
    }

    nodes: list[dict[str, Any]] = []
    for label, step_list in label_steps.items():
        segs = segments_by_label[label]
        order_values = [step_orders[s.name] for s in step_list]
        children_ids = sorted(parent_children.get(label, []), key=lambda c: label_first_order.get(c, 0))
        kind = "container" if len(children_ids) >= 2 else "label"
        nodes.append(
            {
                "id": label,
                "label": label,
                "status": _label_status(step_list, ds),
                "kind": kind,
                "step_ids": [s.name for s in step_list],
                "step_count": len(step_list),
                "parent_id": None,
                "children_ids": children_ids,
                "order_min": min(order_values) if order_values else 0,
                "order_max": max(order_values) if order_values else 0,
                "segments": segs,
            }
        )

    for c in containments:
        for node in nodes:
            if node["id"] == c["child"]:
                node["parent_id"] = c["parent"]
                node["kind"] = "label"

    node_by_id = {n["id"]: n for n in nodes}

    shared_relations: list[dict[str, Any]] = []
    containment_pairs = {(c["parent"], c["child"]) for c in containments}
    label_ids = list(label_steps.keys())
    for i, a in enumerate(label_ids):
        for b in label_ids[i + 1 :]:
            if (a, b) in containment_pairs or (b, a) in containment_pairs:
                continue
            shared = label_step_ids[a] & label_step_ids[b]
            if not shared:
                continue
            shared_relations.append(
                {
                    "id": f"shared-{a}-{b}",
                    "a": a,
                    "b": b,
                    "shared_step_ids": sorted(shared, key=lambda sid: step_orders[sid]),
                    "shared_count": len(shared),
                    "visible_by_default": False,
                }
            )

    top_level = [lid for lid in label_ids if lid not in children_set]
    top_level.sort(key=lambda lid: node_by_id[lid]["order_min"])

    interleavings: list[dict[str, Any]] = []
    interleaved_labels: set[str] = set()
    top_level_set = set(top_level)

    for i, a in enumerate(top_level):
        for b in top_level[i + 1 :]:
            if a in interleaved_labels or b in interleaved_labels:
                continue
            interleaved, switch_count = _is_interleaved(segments_by_label[a], segments_by_label[b])
            if not interleaved:
                continue
            group_id = f"interleaved:{a}:{b}"
            interleavings.append(
                {
                    "id": group_id,
                    "labels": [a, b],
                    "segments": sorted(
                        segments_by_label[a] + segments_by_label[b],
                        key=lambda s: s["start_order"],
                    ),
                    "switch_count": switch_count,
                    "visible_by_default": True,
                }
            )
            interleaved_labels.add(a)
            interleaved_labels.add(b)
            nodes.append(
                {
                    "id": group_id,
                    "label": f"{a} ⇄ {b}",
                    "status": "pending"
                    if _label_status(label_steps[a], ds) == "pending"
                    or _label_status(label_steps[b], ds) == "pending"
                    else "completed",
                    "kind": "interleaved-group",
                    "step_ids": sorted(
                        label_step_ids[a] | label_step_ids[b],
                        key=lambda sid: step_orders[sid],
                    ),
                    "step_count": len(label_step_ids[a] | label_step_ids[b]),
                    "parent_id": None,
                    "children_ids": [a, b],
                    "order_min": min(node_by_id[a]["order_min"], node_by_id[b]["order_min"]),
                    "order_max": max(node_by_id[a]["order_max"], node_by_id[b]["order_max"]),
                    "segments": sorted(
                        segments_by_label[a] + segments_by_label[b],
                        key=lambda s: s["start_order"],
                    ),
                }
            )
            node_by_id[group_id] = nodes[-1]

    def _sequence_units() -> list[str]:
        units: list[str] = []
        seen_interleaved: set[str] = set()
        for lid in top_level:
            if lid in interleaved_labels:
                for inter in interleavings:
                    if lid in inter["labels"] and inter["id"] not in seen_interleaved:
                        units.append(inter["id"])
                        seen_interleaved.add(inter["id"])
                        break
            else:
                units.append(lid)
        return units

    sequence_units = _sequence_units()

    edges: list[dict[str, Any]] = []
    for i in range(len(sequence_units) - 1):
        src, tgt = sequence_units[i], sequence_units[i + 1]
        edges.append(
            {
                "id": f"{src}->{tgt}",
                "source": src,
                "target": tgt,
                "kind": "order",
                "visible_by_default": True,
            }
        )

    for parent, children in parent_children.items():
        if parent not in top_level_set:
            continue
        sorted_children = sorted(children, key=lambda c: node_by_id[c]["order_min"])
        for i in range(len(sorted_children) - 1):
            src, tgt = sorted_children[i], sorted_children[i + 1]
            edges.append(
                {
                    "id": f"{src}->{tgt}",
                    "source": src,
                    "target": tgt,
                    "kind": "order",
                    "visible_by_default": True,
                }
            )

    for parent in parent_children:
        if parent not in top_level_set:
            continue
        sorted_children = sorted(parent_children[parent], key=lambda c: node_by_id[c]["order_min"])
        if not sorted_children:
            continue
        first_child = sorted_children[0]
        last_child = sorted_children[-1]

        if parent in sequence_units:
            parent_idx = sequence_units.index(parent)
            if parent_idx > 0:
                prev_unit = sequence_units[parent_idx - 1]
                prev_id = prev_unit if not prev_unit.startswith("interleaved:") else prev_unit
                edges.append(
                    {
                        "id": f"{prev_id}->{first_child}",
                        "source": prev_id if not prev_unit.startswith("interleaved:") else prev_unit,
                        "target": first_child,
                        "kind": "exact-order",
                        "visible_by_default": False,
                        "show_when_selected": [prev_id, first_child, parent],
                        "source_scope": "node",
                        "target_scope": "child",
                    }
                )
            if parent_idx < len(sequence_units) - 1:
                next_unit = sequence_units[parent_idx + 1]
                next_id = next_unit
                edges.append(
                    {
                        "id": f"{last_child}->{next_id}",
                        "source": last_child,
                        "target": next_id,
                        "kind": "exact-order",
                        "visible_by_default": False,
                        "show_when_selected": [last_child, next_id, parent],
                        "source_scope": "child",
                        "target_scope": "node",
                    }
                )

    return {
        "label_key": label_key,
        "nodes": nodes,
        "edges": edges,
        "containments": containments,
        "shared_relations": shared_relations,
        "interleavings": interleavings,
    }
