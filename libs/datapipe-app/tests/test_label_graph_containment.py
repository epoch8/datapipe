from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from datapipe_app.observability.graph.label_graph import (
    _find_containments,
    available_label_keys,
    build_label_graph,
)


@dataclass
class _FakeStep:
    name: str
    labels: list[tuple[str, str]] = field(default_factory=list)


class _FakeDS:
    pass


def test_find_containments_keeps_direct_nesting_only():
    label_step_ids = {
        "train": {"split", "freeze", "yolo", "infer", "metrics"},
        "train-prepare": {"split", "freeze"},
        "train-without-freeze": {"yolo", "infer", "metrics"},
        "inference": {"infer"},
        "count-metrics": {"metrics"},
    }
    containments = _find_containments(label_step_ids)
    pairs = {(c["parent"], c["child"]) for c in containments}
    assert ("train", "train-prepare") in pairs
    assert ("train", "train-without-freeze") in pairs
    assert ("train-without-freeze", "inference") in pairs
    assert ("train-without-freeze", "count-metrics") in pairs
    # Transitive edges must be dropped.
    assert ("train", "inference") not in pairs
    assert ("train", "count-metrics") not in pairs


def test_build_label_graph_preserves_nested_container_kind(monkeypatch):
    steps = [
        _FakeStep("split", [("stage", "train"), ("stage", "train-prepare")]),
        _FakeStep("freeze", [("stage", "train"), ("stage", "train-prepare")]),
        _FakeStep("yolo", [("stage", "train"), ("stage", "train-without-freeze")]),
        _FakeStep("infer", [("stage", "train"), ("stage", "train-without-freeze"), ("stage", "inference")]),
        _FakeStep(
            "metrics",
            [("stage", "train"), ("stage", "train-without-freeze"), ("stage", "count-metrics")],
        ),
    ]

    monkeypatch.setattr(
        "datapipe_app.observability.graph.label_graph.stage_status_for_step",
        lambda *args, **kwargs: {"has_backlog": False},
    )
    payload = build_label_graph(steps, _FakeDS())  # type: ignore[arg-type]
    by_id = {n["id"]: n for n in payload["nodes"]}

    assert by_id["train"]["kind"] == "container"
    assert set(by_id["train"]["children_ids"]) == {"train-prepare", "train-without-freeze"}
    assert by_id["train-without-freeze"]["kind"] == "container"
    assert by_id["train-without-freeze"]["parent_id"] == "train"
    assert set(by_id["train-without-freeze"]["children_ids"]) == {"inference", "count-metrics"}
    assert by_id["inference"]["parent_id"] == "train-without-freeze"
    assert by_id["count-metrics"]["parent_id"] == "train-without-freeze"


def test_shared_relations_skip_ancestor_descendant_pairs(monkeypatch):
    steps = [
        _FakeStep("split", [("stage", "train"), ("stage", "train-prepare")]),
        _FakeStep("freeze", [("stage", "train"), ("stage", "train-prepare")]),
        _FakeStep("yolo", [("stage", "train"), ("stage", "train-without-freeze")]),
        _FakeStep("infer", [("stage", "train"), ("stage", "train-without-freeze"), ("stage", "inference")]),
        _FakeStep(
            "metrics",
            [("stage", "train"), ("stage", "train-without-freeze"), ("stage", "count-metrics")],
        ),
    ]
    monkeypatch.setattr(
        "datapipe_app.observability.graph.label_graph.stage_status_for_step",
        lambda *args, **kwargs: {"has_backlog": False},
    )
    payload = build_label_graph(steps, _FakeDS())  # type: ignore[arg-type]
    pairs = {(r["a"], r["b"]) for r in payload["shared_relations"]} | {
        (r["b"], r["a"]) for r in payload["shared_relations"]
    }
    assert ("train", "inference") not in pairs
    assert ("train", "count-metrics") not in pairs
    assert ("train", "train-without-freeze") not in pairs
    assert ("train-without-freeze", "inference") not in pairs


def test_shared_labels_do_not_mask_real_interleaving(monkeypatch):
    steps = [
        _FakeStep("get_data_model", [("stage", "extract"), ("stage", "data-model")]),
        _FakeStep("get_grist_data", [("stage", "extract"), ("stage", "grist")]),
        *[_FakeStep(f"extract_{order}", [("stage", "extract")]) for order in range(2, 6)],
        *[_FakeStep(f"transform_{order}", [("stage", "transform")]) for order in range(6, 14)],
        _FakeStep("fetch_legal_topics", [("stage", "extract"), ("stage", "grist")]),
        _FakeStep("classify_chunk_topics", [("stage", "transform")]),
        _FakeStep("prepare_nodes", [("stage", "transform"), ("stage", "grist")]),
        _FakeStep("prepare_edges", [("stage", "transform"), ("stage", "grist")]),
        *[_FakeStep(f"load_{order}", [("stage", "load")]) for order in range(18, 24)],
        _FakeStep("get_eval_gds", [("stage", "extract")]),
    ]
    monkeypatch.setattr(
        "datapipe_app.observability.graph.label_graph.stage_status_for_step",
        lambda *args, **kwargs: {"has_backlog": False},
    )

    payload = build_label_graph(steps, _FakeDS())  # type: ignore[arg-type]

    assert [inter["labels"] for inter in payload["interleavings"]] == [["extract", "transform"]]
    assert not any(node["id"] == "interleaved:extract:grist" for node in payload["nodes"])

    shared_pairs = {
        frozenset((relation["a"], relation["b"])) for relation in payload["shared_relations"]
    }
    assert frozenset(("extract", "grist")) in shared_pairs
    assert frozenset(("transform", "grist")) in shared_pairs


def test_available_label_keys_prefers_stage_then_popularity():
    steps = [
        _FakeStep("a", [("source", "API"), ("flow", "regular"), ("stage", "extract")]),
        _FakeStep("b", [("custom", "x"), ("flow", "on-demand"), ("flow", "regular")]),
        _FakeStep("c", [("flow", "regular"), ("custom", "y")]),
    ]
    # stage first; then flow (3), custom (2), source (1)
    assert available_label_keys(steps) == ["stage", "flow", "custom", "source"]  # type: ignore[arg-type]


def test_available_label_keys_without_stage_uses_most_popular():
    steps = [
        _FakeStep("a", [("source", "API"), ("flow", "regular")]),
        _FakeStep("b", [("flow", "on-demand")]),
        _FakeStep("c", [("flow", "regular"), ("custom", "x")]),
    ]
    assert available_label_keys(steps) == ["flow", "custom", "source"]  # type: ignore[arg-type]


def test_build_label_graph_filters_by_label_key(monkeypatch):
    steps = [
        _FakeStep("fetch", [("stage", "extract"), ("flow", "regular"), ("source", "API")]),
        _FakeStep("transform", [("stage", "transform"), ("flow", "regular")]),
        _FakeStep("prepare", [("stage", "transform"), ("flow", "on-demand"), ("source", "Grist")]),
    ]
    monkeypatch.setattr(
        "datapipe_app.observability.graph.label_graph.stage_status_for_step",
        lambda *args, **kwargs: {"has_backlog": False},
    )

    flow_graph = build_label_graph(steps, _FakeDS(), label_key="flow")  # type: ignore[arg-type]
    assert flow_graph["label_key"] == "flow"
    assert {node["id"] for node in flow_graph["nodes"] if node["kind"] == "label"} == {
        "regular",
        "on-demand",
    }

    source_graph = build_label_graph(steps, _FakeDS(), label_key="source")  # type: ignore[arg-type]
    assert source_graph["label_key"] == "source"
    assert {node["id"] for node in source_graph["nodes"] if node["kind"] == "label"} == {
        "API",
        "Grist",
    }
