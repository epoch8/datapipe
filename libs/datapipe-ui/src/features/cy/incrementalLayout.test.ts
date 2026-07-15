import {
    buildCollapsedLayout,
    expandGroupInLayout,
    GROUP_PADDING,
    measureNode,
} from "./incrementalLayout";
import { graphNodeDimensions } from "./graphNodeLayout";

function makeGroupGraph() {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
        ["group_a", { id: "group_a", name: "group_a", type: "group", child_count: 2 }],
        ["group_b", { id: "group_b", name: "group_b", type: "group", child_count: 2 }],
        ["mid", { id: "mid", name: "mid", type: "transform", pipelineIndex: 1 }],
        ["a1", { id: "a1", name: "a1", type: "transform", metaGroup: "group_a", pipelineIndex: 0 }],
        ["a2", { id: "a2", name: "a2", type: "transform", metaGroup: "group_a", pipelineIndex: 1 }],
        ["b1", { id: "b1", name: "b1", type: "transform", metaGroup: "group_b", pipelineIndex: 0 }],
        ["b2", { id: "b2", name: "b2", type: "transform", metaGroup: "group_b", pipelineIndex: 1 }],
    ]);
    const edges: Cytoscape.EdgeDataDefinition[] = [
        { source: "group_a", target: "mid" },
        { source: "mid", target: "group_b" },
        { source: "a1", target: "a2" },
        { source: "b1", target: "b2" },
    ];
    return { nodes, edges };
}

function assertInnersInsideFrame(
    layout: Map<string, { bbox: { x: number; y: number; w: number; h: number }; node: { metaGroup?: string } }>,
    groupId: string,
) {
    const frame = layout.get(groupId);
    expect(frame).toBeTruthy();
    const pad = 1;
    layout.forEach((entry, id) => {
        if (entry.node.metaGroup !== groupId) return;
        expect(entry.bbox.x).toBeGreaterThanOrEqual(frame!.bbox.x - pad);
        expect(entry.bbox.y).toBeGreaterThanOrEqual(frame!.bbox.y - pad);
        expect(entry.bbox.x + entry.bbox.w).toBeLessThanOrEqual(frame!.bbox.x + frame!.bbox.w + pad);
        expect(entry.bbox.y + entry.bbox.h).toBeLessThanOrEqual(frame!.bbox.y + frame!.bbox.h + pad);
        void id;
    });
}

describe("measureNode", () => {
    it("uses full card size for metaGroup (inner) nodes", () => {
        const inner = measureNode({
            id: "step",
            name: "step",
            type: "transform",
            metaGroup: "group_a",
        });
        const top = measureNode({ id: "step2", name: "step2", type: "transform" });
        expect(inner.w).toBe(graphNodeDimensions.transform.width);
        expect(inner.h).toBe(graphNodeDimensions.transform.height);
        expect(inner.w).toBe(top.w);
        expect(inner.h).toBe(top.h);
    });
});

describe("expandGroupInLayout multi-expand", () => {
    it("keeps earlier group inners inside the blue frame after a later expand", () => {
        const { nodes, edges } = makeGroupGraph();
        let layout = buildCollapsedLayout(nodes, edges, new Set(), "TB");

        layout = expandGroupInLayout(layout, "group_a", nodes, edges, "TB", ["a1", "a2"]);
        assertInnersInsideFrame(layout, "group_a");

        layout = expandGroupInLayout(layout, "group_b", nodes, edges, "TB", ["b1", "b2"]);
        assertInnersInsideFrame(layout, "group_a");
        assertInnersInsideFrame(layout, "group_b");
    });

    it("keeps both groups coherent when lower group is expanded first", () => {
        const { nodes, edges } = makeGroupGraph();
        let layout = buildCollapsedLayout(nodes, edges, new Set(), "TB");

        layout = expandGroupInLayout(layout, "group_b", nodes, edges, "TB", ["b1", "b2"]);
        layout = expandGroupInLayout(layout, "group_a", nodes, edges, "TB", ["a1", "a2"]);
        assertInnersInsideFrame(layout, "group_a");
        assertInnersInsideFrame(layout, "group_b");
    });

    it("buildCollapsedLayout with both expanded keeps inners inside frames", () => {
        const { nodes, edges } = makeGroupGraph();
        const layout = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a", "group_b"]),
            "TB",
            new Map([
                ["group_a", ["a1", "a2"]],
                ["group_b", ["b1", "b2"]],
            ]),
        );
        assertInnersInsideFrame(layout, "group_a");
        assertInnersInsideFrame(layout, "group_b");

        const aFrame = layout.get("group_a")!;
        const a1 = layout.get("a1")!;
        expect(aFrame.bbox.h).toBeGreaterThanOrEqual(
            a1.bbox.h * 2 + GROUP_PADDING.top + GROUP_PADDING.bottom,
        );
    });
});
