import {
    buildCollapsedLayout,
    countBipartiteCrossings,
    countLayerCutCrossings,
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

    it("buildCollapsedLayout LR multi-expand keeps dependency order without push cascade", () => {
        const { nodes, edges } = makeGroupGraph();
        const collapsed = buildCollapsedLayout(nodes, edges, new Set(), "LR");
        const both = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a", "group_b"]),
            "LR",
            new Map([
                ["group_a", ["a1", "a2"]],
                ["group_b", ["b1", "b2"]],
            ]),
        );
        assertInnersInsideFrame(both, "group_a");
        assertInnersInsideFrame(both, "group_b");

        const a0 = collapsed.get("group_a")!;
        const b0 = collapsed.get("group_b")!;
        const mid0 = collapsed.get("mid")!;
        expect(a0.bbox.x).toBeLessThan(mid0.bbox.x);
        expect(mid0.bbox.x).toBeLessThan(b0.bbox.x);

        const a1 = both.get("group_a")!;
        const b1 = both.get("group_b")!;
        const mid1 = both.get("mid")!;
        expect(a1.bbox.x).toBeLessThan(mid1.bbox.x);
        expect(mid1.bbox.x).toBeLessThan(b1.bbox.x);
        // Expanded frames are wider than collapsed, but mid stays between them.
        expect(a1.bbox.w).toBeGreaterThan(a0.bbox.w);
        expect(b1.bbox.w).toBeGreaterThan(b0.bbox.w);
    });

    it("projects inner endpoints onto meta so expanded groups keep outer ranks", () => {
        // Real expanded graphs wire edges to inners, not the blue frame id.
        const nodes = new Map<string, Cytoscape.NodeDataDefinition>([
            ["group_a", { id: "group_a", name: "group_a", type: "group-expanded", child_count: 2 }],
            ["group_b", { id: "group_b", name: "group_b", type: "group-expanded", child_count: 2 }],
            ["mid", { id: "mid", name: "mid", type: "transform", pipelineIndex: 1 }],
            ["a1", { id: "a1", name: "a1", type: "transform", metaGroup: "group_a", pipelineIndex: 0 }],
            ["a2", { id: "a2", name: "a2", type: "transform", metaGroup: "group_a", pipelineIndex: 1 }],
            ["b1", { id: "b1", name: "b1", type: "transform", metaGroup: "group_b", pipelineIndex: 0 }],
            ["b2", { id: "b2", name: "b2", type: "transform", metaGroup: "group_b", pipelineIndex: 1 }],
        ]);
        const edges: Cytoscape.EdgeDataDefinition[] = [
            { source: "a2", target: "mid" },
            { source: "mid", target: "b1" },
            { source: "a1", target: "a2" },
            { source: "b1", target: "b2" },
        ];
        const layout = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a", "group_b"]),
            "LR",
            new Map([
                ["group_a", ["a1", "a2"]],
                ["group_b", ["b1", "b2"]],
            ]),
        );
        expect(layout.get("group_a")!.bbox.x).toBeLessThan(layout.get("mid")!.bbox.x);
        expect(layout.get("mid")!.bbox.x).toBeLessThan(layout.get("group_b")!.bbox.x);
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

    it("layoutLayeredDag prefers pipeline chronology over reverse dependencies", () => {
        // Early annotation step depends on a late "best model" table — classic
        // longest-path would put fiftyone-like nodes left of annotation. Chronology
        // must keep pipeline order left→right.
        const nodes = new Map<string, MeasuredNode>([
            [
                "early",
                {
                    ...stubNode("early"),
                    pipelineIndex: 1,
                    pipelineOrderKey: "0001",
                },
            ],
            [
                "fiftyone",
                {
                    ...stubNode("fiftyone"),
                    pipelineIndex: 18,
                    pipelineOrderKey: "0018",
                },
            ],
            [
                "best",
                {
                    ...stubNode("best"),
                    pipelineIndex: 15,
                    pipelineOrderKey: "0015",
                },
            ],
            [
                "late_consumer",
                {
                    ...stubNode("late_consumer"),
                    pipelineIndex: 8,
                    pipelineOrderKey: "0008",
                },
            ],
        ]);
        const edges: LayoutEdge[] = [
            { source: "best", target: "late_consumer" },
            { source: "early", target: "fiftyone" },
        ];
        const positions = layoutLayeredDag(nodes, edges, "LR");
        expect(positions.get("early")!.x).toBeLessThan(positions.get("late_consumer")!.x);
        expect(positions.get("late_consumer")!.x).toBeLessThan(positions.get("best")!.x);
        expect(positions.get("best")!.x).toBeLessThan(positions.get("fiftyone")!.x);
    });

    it("layoutLayeredDag stacks same pipeline-step nodes in one LR column", () => {
        const nodes = new Map<string, MeasuredNode>([
            [
                "step",
                {
                    ...stubNode("step"),
                    pipelineIndex: 3,
                    pipelineOrderKey: "0003",
                },
            ],
            [
                "out_a",
                {
                    ...stubNode("out_a"),
                    type: "table",
                    pipelineIndex: 3,
                    pipelineOrderKey: "0003.out.0000",
                },
            ],
            [
                "out_b",
                {
                    ...stubNode("out_b"),
                    type: "table",
                    pipelineIndex: 3,
                    pipelineOrderKey: "0003.out.0001",
                },
            ],
            [
                "next",
                {
                    ...stubNode("next"),
                    pipelineIndex: 4,
                    pipelineOrderKey: "0004",
                },
            ],
        ]);
        const positions = layoutLayeredDag(nodes, [], "LR");
        expect(positions.get("step")!.x).toBe(positions.get("out_a")!.x);
        expect(positions.get("out_a")!.x).toBe(positions.get("out_b")!.x);
        expect(positions.get("step")!.x).toBeLessThan(positions.get("next")!.x);
        // Vertical stack, not a horizontal ribbon.
        const ys = ["step", "out_a", "out_b"].map((id) => positions.get(id)!.y);
        expect(new Set(ys).size).toBe(3);
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

    it("counts long-span sequential edges on multi-layer cuts", () => {
        const ordered = new Map<number, string[]>([
            [0, ["A", "B"]],
            [1, ["M"]],
            [2, ["C", "D"]],
        ]);
        const nodeRank = new Map([
            ["A", 0],
            ["B", 0],
            ["M", 1],
            ["C", 2],
            ["D", 2],
        ]);
        const edges: LayoutEdge[] = [
            { source: "A", target: "D", sequential: true, synthetic: true },
            { source: "B", target: "C", sequential: true, synthetic: true },
        ];
        // A left→D right and B left→C left-of-D? With bottom C,D: A→D and B→C cross.
        expect(countLayerCutCrossings(ordered, nodeRank, 0, 2, edges)).toBe(1);
        // Same endpoints uncrossed when bottom is D,C.
        const uncrossed = new Map<number, string[]>([
            [0, ["A", "B"]],
            [1, ["M"]],
            [2, ["D", "C"]],
        ]);
        expect(countLayerCutCrossings(uncrossed, nodeRank, 0, 2, edges)).toBe(0);
    });

    it("uncrosses a sequential long hop against a solid mid path", () => {
        // A B
        // M
        // C D
        // solid A→M→C, B→M→D plus sequential A→D (wants A above D / B above C → swap bottom)
        const ranks = new Map<number, string[]>([
            [0, ["A", "B"]],
            [1, ["M"]],
            [2, ["C", "D"]],
        ]);
        const edges: LayoutEdge[] = [
            { source: "A", target: "M" },
            { source: "B", target: "M" },
            { source: "M", target: "C" },
            { source: "M", target: "D" },
            { source: "A", target: "D", sequential: true, synthetic: true },
            { source: "B", target: "C", sequential: true, synthetic: true },
        ];
        const nodes = new Map<string, MeasuredNode>([
            ["A", stubNode("A")],
            ["B", stubNode("B")],
            ["M", stubNode("M")],
            ["C", stubNode("C")],
            ["D", stubNode("D")],
        ]);
        const ordered = minimizeLayerCrossings(ranks, edges, nodes);
        const top = ordered.get(0)!;
        const bottom = ordered.get(2)!;
        const a = top.indexOf("A");
        const b = top.indexOf("B");
        const c = bottom.indexOf("C");
        const d = bottom.indexOf("D");
        // Sequential A→D and B→C are uncrossed when (a-b)*(d-c) >= 0.
        expect((a - b) * (d - c)).toBeGreaterThanOrEqual(0);
    });
});
