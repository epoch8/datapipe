import type { LabelGraphNode, LabelGraphPayload, LabelSegment } from "../../../types/ops";
import { layoutLabelGraph, normalizeLabelGraphHierarchy } from "./labelGraph";

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
