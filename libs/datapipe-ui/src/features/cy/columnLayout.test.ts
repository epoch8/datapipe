import Cytoscape from "cytoscape";
import {
    buildLabelColumnLayout,
    buildLabelColumnMap,
    topLevelLabelOrder,
} from "./columnLayout";

function center(layout: ReturnType<typeof buildLabelColumnLayout>, id: string) {
    const bbox = layout.get(id)!.bbox;
    return { x: bbox.x + bbox.w / 2, y: bbox.y + bbox.h / 2 };
}

test("horizontal layout follows dependency order left-to-right", () => {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
        [
            "extract-step",
            {
                type: "transform",
                name: "extract-step",
                labels: [["stage", "extract"]],
                pipelineOrderKey: "0000",
            },
        ],
        ["raw", { type: "table", name: "raw", pipelineOrderKey: "0000.out.0000" }],
        [
            "transform-step",
            {
                type: "transform",
                name: "transform-step",
                labels: [["stage", "transform"]],
                pipelineOrderKey: "0001",
            },
        ],
        ["clean", { type: "table", name: "clean", pipelineOrderKey: "0001.out.0000" }],
    ]);
    const edges: Cytoscape.EdgeDataDefinition[] = [
        { source: "extract-step", target: "raw" },
        { source: "raw", target: "transform-step" },
        { source: "transform-step", target: "clean" },
    ];

    const horizontal = buildLabelColumnLayout(
        nodes,
        edges,
        new Set(),
        "LR",
        "stage",
        ["extract", "transform"],
    );
    expect(center(horizontal, "extract-step").x).toBeLessThan(center(horizontal, "raw").x);
    expect(center(horizontal, "raw").x).toBeLessThan(center(horizontal, "transform-step").x);
    expect(center(horizontal, "transform-step").x).toBeLessThan(center(horizontal, "clean").x);

    const vertical = buildLabelColumnLayout(
        nodes,
        edges,
        new Set(),
        "TB",
        "stage",
        ["extract", "transform"],
    );
    expect(center(vertical, "extract-step").y).toBeLessThan(center(vertical, "raw").y);
    expect(center(vertical, "raw").y).toBeLessThan(center(vertical, "transform-step").y);
});

test("maps nested labels into top-level columns", () => {
    const nodes = [
        { id: "annotation", kind: "label", parent_id: null, order_min: 0 },
        { id: "train", kind: "container", parent_id: null, order_min: 1 },
        { id: "train-yolo", kind: "label", parent_id: "train", order_min: 2 },
        { id: "fiftyone", kind: "label", parent_id: null, order_min: 3 },
    ];
    expect(topLevelLabelOrder(nodes)).toEqual(["annotation", "train", "fiftyone"]);
    const map = buildLabelColumnMap(nodes);
    expect(map.get("train-yolo")).toBe("train");
    expect(map.get("annotation")).toBe("annotation");
});

test("expanded meta keeps children near the group frame", () => {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
        [
            "G",
            {
                type: "group-expanded",
                name: "G",
                labels: [["stage", "extract"]],
                child_count: 2,
                pipelineOrderKey: "0000",
            },
        ],
        [
            "t1",
            {
                type: "transform",
                name: "t1",
                metaGroup: "G",
                labels: [["stage", "extract"]],
                pipelineOrderKey: "0000.0000",
            },
        ],
        [
            "t2",
            {
                type: "transform",
                name: "t2",
                metaGroup: "G",
                labels: [["stage", "extract"]],
                pipelineOrderKey: "0000.0001",
            },
        ],
        ["out", { type: "table", name: "out", pipelineOrderKey: "0000.out.0000" }],
    ]);
    const edges: Cytoscape.EdgeDataDefinition[] = [
        { source: "t1", target: "t2" },
        { source: "t2", target: "out" },
        { source: "G", target: "out" },
    ];

    const layout = buildLabelColumnLayout(
        nodes,
        edges,
        new Set(["G"]),
        "LR",
        "stage",
        ["extract"],
    );
    const group = layout.get("G")!;
    expect(group).toBeTruthy();
    ["t1", "t2"].forEach((id) => {
        const entry = layout.get(id)!;
        const cx = entry.bbox.x + entry.bbox.w / 2;
        const cy = entry.bbox.y + entry.bbox.h / 2;
        expect(cx).toBeGreaterThan(group.bbox.x);
        expect(cx).toBeLessThan(group.bbox.x + group.bbox.w);
        expect(cy).toBeGreaterThan(group.bbox.y);
        expect(cy).toBeLessThan(group.bbox.y + group.bbox.h);
    });
});
