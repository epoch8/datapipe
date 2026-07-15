import {
    buildCollapsedLayout,
    countBipartiteCrossings,
    expandGroupInLayout,
    GROUP_PADDING,
    layoutLayeredDag,
    measureNode,
    minimizeLayerCrossings,
} from "./incrementalLayout";
import { graphNodeDimensions } from "./graphNodeLayout";
import type { LayoutEdge, MeasuredNode } from "./incrementalLayout";

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

    it("seeds group-expanded nodes so restored expand gets a full frame", () => {
        // Mimic reprocessData: expanded metas arrive as type group-expanded.
        const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
            [
                "group_a",
                {
                    id: "group_a",
                    name: "group_a",
                    type: "group-expanded",
                    child_count: 2,
                    frameLabel: "group_a · 2 steps",
                },
            ],
            ["a1", { id: "a1", name: "a1", type: "transform", metaGroup: "group_a" }],
            ["a2", { id: "a2", name: "a2", type: "transform", metaGroup: "group_a" }],
            ["out", { id: "out", name: "out", type: "table" }],
        ]);
        const edges: Cytoscape.EdgeDataDefinition[] = [
            { source: "a1", target: "a2" },
            { source: "a2", target: "out" },
        ];
        const layout = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "TB",
            new Map([["group_a", ["a1", "a2"]]]),
        );
        const frame = layout.get("group_a");
        expect(frame).toBeTruthy();
        expect(frame!.node.type).toBe("group-expanded");
        expect(frame!.bbox.w).toBeGreaterThan(graphNodeDimensions.groupCollapsed.width);
        expect(frame!.bbox.h).toBeGreaterThan(graphNodeDimensions.groupCollapsed.height);
        assertInnersInsideFrame(layout, "group_a");
    });
});

describe("minimizeLayerCrossings (barycenter)", () => {
    function stubNode(id: string): MeasuredNode {
        return { id, type: "transform", name: id, w: 100, h: 40 };
    }

    it("removes a classic 2×2 bipartite crossing", () => {
        // A B
        // C D   with A→D, B→C  (crosses) → reorder lower to D C
        const ranks = new Map<number, string[]>([
            [0, ["A", "B"]],
            [1, ["C", "D"]],
        ]);
        const edges: LayoutEdge[] = [
            { source: "A", target: "D" },
            { source: "B", target: "C" },
        ];
        const nodes = new Map<string, MeasuredNode>([
            ["A", stubNode("A")],
            ["B", stubNode("B")],
            ["C", stubNode("C")],
            ["D", stubNode("D")],
        ]);

        expect(countBipartiteCrossings(["A", "B"], ["C", "D"], edges)).toBe(1);

        const ordered = minimizeLayerCrossings(ranks, edges, nodes);
        const upper = ordered.get(0)!;
        const lower = ordered.get(1)!;
        expect(countBipartiteCrossings(upper, lower, edges)).toBe(0);
    });

    it("layoutLayeredDag places uncrossed endpoints left-to-right consistently", () => {
        const nodes = new Map<string, MeasuredNode>([
            ["A", stubNode("A")],
            ["B", stubNode("B")],
            ["C", stubNode("C")],
            ["D", stubNode("D")],
        ]);
        const edges: LayoutEdge[] = [
            { source: "A", target: "D" },
            { source: "B", target: "C" },
        ];
        // Seed ranks so A,B stay on layer 0 and C,D on layer 1.
        const rankEdges: LayoutEdge[] = [
            { source: "A", target: "C" },
            { source: "A", target: "D" },
            { source: "B", target: "C" },
            { source: "B", target: "D" },
        ];
        const positions = layoutLayeredDag(nodes, edges, "TB", rankEdges);
        // After minimization, the left↔right pairing should not cross:
        // whichever of C/D is under A should be on the same side as A.
        const ax = positions.get("A")!.x + positions.get("A")!.w / 2;
        const bx = positions.get("B")!.x + positions.get("B")!.w / 2;
        const cx = positions.get("C")!.x + positions.get("C")!.w / 2;
        const dx = positions.get("D")!.x + positions.get("D")!.w / 2;
        // Edges A→D, B→C: after uncross, order should be A,B on top and D,C
        // (or equivalent so (ax-bx)*(dx-cx) >= 0).
        expect((ax - bx) * (dx - cx)).toBeGreaterThanOrEqual(0);
    });
});
