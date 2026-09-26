from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from datapipe_app.observability.graph.label_graph import _find_containments, build_label_graph


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
