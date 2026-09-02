import type { LabelGraphNode, LabelGraphPayload, LabelSegment } from "../../../types/ops";
import { edgePath, layoutLabelGraph, normalizeLabelGraphHierarchy } from "./labelGraph";

const segment = (labelId: string, order: number): LabelSegment => ({
    label_id: labelId,
    start_order: order,
    end_order: order,
    step_ids: [`${labelId}-${order}`],
});

const labelNode = (id: string, order: number): LabelGraphNode => ({
    id,
    label: id,
    status: "completed",
    kind: "label",
    step_ids: [`${id}-${order}`],
    step_count: 1,
    parent_id: null,
    children_ids: [],
    order_min: order,
    order_max: order,
    segments: [segment(id, order)],
});

function corbeyLabelGraphFixture(): LabelGraphPayload {
    const interleavedId = "interleaved:extract:transform";
    const extract = labelNode("extract", 0);
    const transform = labelNode("transform", 6);
    const interleavedSegments = [
        segment("extract", 0),
        segment("transform", 6),
        segment("extract", 14),
        segment("transform", 15),
        segment("extract", 24),
    ];

    return {
        label_key: "stage",
        nodes: [
            extract,
            labelNode("data-model", 0),
            labelNode("grist", 1),
            transform,
            labelNode("load", 18),
            {
                id: interleavedId,
                label: "extract ⇄ transform",
                status: "completed",
                kind: "interleaved-group",
                step_ids: [...extract.step_ids, ...transform.step_ids],
                step_count: 2,
                parent_id: null,
                children_ids: ["extract", "transform"],
                order_min: 0,
                order_max: 24,
                segments: interleavedSegments,
            },
        ],
        edges: [
            {
                id: `${interleavedId}->data-model`,
                source: interleavedId,
                target: "data-model",
                kind: "order",
                visible_by_default: true,
            },
            {
                id: "data-model->grist",
                source: "data-model",
                target: "grist",
                kind: "order",
                visible_by_default: true,
            },
            {
                id: "grist->load",
                source: "grist",
                target: "load",
                kind: "order",
                visible_by_default: true,
            },
        ],
        containments: [],
        shared_relations: [],
        interleavings: [
            {
                id: interleavedId,
                labels: ["extract", "transform"],
                segments: interleavedSegments,
                switch_count: 4,
                visible_by_default: true,
            },
        ],
    };
}

test("preserves interleaved lanes and places their group before tied labels", () => {
    const normalized = normalizeLabelGraphHierarchy(corbeyLabelGraphFixture());
    const group = normalized.nodes.find(
        (node) => node.id === "interleaved:extract:transform",
    );

    expect(group?.children_ids).toEqual(["extract", "transform"]);

    const layout = layoutLabelGraph(normalized, "compact");
    const interleaved = layout.nodes.find(
        (node) => node.nodeId === "interleaved:extract:transform",
    );
    const dataModel = layout.nodes.find((node) => node.nodeId === "data-model");

    expect(interleaved?.interleavedLabelIds).toEqual(["extract", "transform"]);
    expect(interleaved?.x).toBeLessThan(dataModel?.x ?? Number.POSITIVE_INFINITY);
});

test("backward label edges use wrap-around routing", () => {
    const payload: LabelGraphPayload = {
        label_key: "flow",
        nodes: [
            labelNode("on-demand", 0),
            labelNode("regular", 1),
            labelNode("eval", 2),
        ],
        edges: [
            {
                id: "regular->on-demand",
                source: "regular",
                target: "on-demand",
                kind: "order",
                visible_by_default: true,
            },
            {
                id: "regular->eval",
                source: "regular",
                target: "eval",
                kind: "order",
                visible_by_default: true,
            },
            {
                id: "eval->on-demand",
                source: "eval",
                target: "on-demand",
                kind: "order",
                visible_by_default: true,
            },
        ],
        containments: [],
        shared_relations: [],
        interleavings: [],
    };

    const layout = layoutLabelGraph(payload, "compact");
    const back = layout.orderEdges.find((e) => e.id === "eval->on-demand");
    const forward = layout.orderEdges.find((e) => e.id === "regular->eval");
    const leftward = layout.orderEdges.find((e) => e.id === "regular->on-demand");

    expect(back?.wrapAround).toBe(true);
    expect(leftward?.wrapAround).toBe(true);
    expect(forward?.wrapAround).toBeFalsy();

    const wrap = edgePath(back!.x1, back!.y1, back!.x2, back!.y2, true);
    expect(wrap).toContain("Q");
    // Wrap arc must sit inside the canvas (not flush/clipped on top or left).
    const loopTop = Math.min(back!.y1, back!.y2) - 48;
    expect(loopTop).toBeGreaterThanOrEqual(28);
    const leftStub = back!.x2 - 24;
    expect(leftStub).toBeGreaterThanOrEqual(12);
    expect(wrap).toMatch(/M \d/);
});
