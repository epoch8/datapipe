import Cytoscape from "cytoscape";
import { buildLabelColumnLayout } from "./columnLayout";

function center(layout: ReturnType<typeof buildLabelColumnLayout>, id: string) {
    const bbox = layout.get(id)!.bbox;
    return { x: bbox.x + bbox.w / 2, y: bbox.y + bbox.h / 2 };
}

function bbox(layout: ReturnType<typeof buildLabelColumnLayout>, id: string) {
    return layout.get(id)!.bbox;
}

test("packs primary label groups left-to-right and transposes centers", () => {
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
    expect(center(horizontal, "extract-step").x).toBeLessThan(
        center(horizontal, "transform-step").x,
    );

    const vertical = buildLabelColumnLayout(
        nodes,
        edges,
        new Set(),
        "TB",
        "stage",
        ["extract", "transform"],
    );
    expect(center(vertical, "extract-step").y).toBeCloseTo(
        center(horizontal, "extract-step").x,
    );
    expect(center(vertical, "transform-step").y).toBeCloseTo(
        center(horizontal, "transform-step").x,
    );
});

test("compresses empty ranks between occupied ranks without reordering inside a label", () => {
    // Single chain so transform-early and transform-late sit on different DAG
    // ranks with an extract-only bridge between them (the empty ranks to remove).
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
        [
            "extract-early",
            {
                type: "transform",
                name: "extract-early",
                labels: [["stage", "extract"]],
                pipelineOrderKey: "0000",
            },
        ],
        ["a", { type: "table", name: "a", pipelineOrderKey: "0000.out.0000" }],
        [
            "transform-early",
            {
                type: "transform",
                name: "transform-early",
                labels: [["stage", "transform"]],
                pipelineOrderKey: "0001",
            },
        ],
        ["b", { type: "table", name: "b", pipelineOrderKey: "0001.out.0000" }],
        [
            "extract-bridge",
            {
                type: "transform",
                name: "extract-bridge",
                labels: [["stage", "extract"]],
                pipelineOrderKey: "0002",
            },
        ],
        ["c", { type: "table", name: "c", pipelineOrderKey: "0002.out.0000" }],
        [
            "transform-late",
            {
                type: "transform",
                name: "transform-late",
                labels: [["stage", "transform"]],
                pipelineOrderKey: "0003",
            },
        ],
        ["d", { type: "table", name: "d", pipelineOrderKey: "0003.out.0000" }],
        [
            "load-step",
            {
                type: "transform",
                name: "load-step",
                labels: [["stage", "load"]],
                pipelineOrderKey: "0004",
            },
        ],
        ["out", { type: "table", name: "out", pipelineOrderKey: "0004.out.0000" }],
    ]);
    const edges: Cytoscape.EdgeDataDefinition[] = [
        { source: "extract-early", target: "a" },
        { source: "a", target: "transform-early" },
        { source: "transform-early", target: "b" },
        { source: "b", target: "extract-bridge" },
        { source: "extract-bridge", target: "c" },
        { source: "c", target: "transform-late" },
        { source: "transform-late", target: "d" },
        { source: "d", target: "load-step" },
        { source: "load-step", target: "out" },
    ];

    const layout = buildLabelColumnLayout(
        nodes,
        edges,
        new Set(),
        "LR",
        "stage",
        ["extract", "transform", "load"],
    );

    // Order inside transform is preserved (early still left of late).
    expect(center(layout, "transform-early").x).toBeLessThan(
        center(layout, "transform-late").x,
    );

    // Empty ranks between transform-early/b and transform-late are compressed:
    // gap should be roughly RANK_GAP, not the extract-bridge hole from the DAG.
    const transformGap =
        bbox(layout, "transform-late").x -
        (bbox(layout, "b").x + bbox(layout, "b").w);
    expect(transformGap).toBeLessThan(120);

    expect(center(layout, "extract-early").x).toBeLessThan(
        center(layout, "transform-early").x,
    );
    expect(center(layout, "transform-late").x).toBeLessThan(
        center(layout, "load-step").x,
    );
});
