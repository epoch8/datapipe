import {
    buildCollapsedLayout,
    computeSnakeBoundaryPolygon,
    countBipartiteCrossings,
    countLayerCutCrossings,
    expandGroupInLayout,
    GROUP_PADDING,
    layoutLayeredDag,
    measureNode,
    minimizeLayerCrossings,
    pinLayoutAnchorCenter,
} from "./incrementalLayout";
import { computeSnakeSpine } from "./snakeRowOverlay";
import { graphNodeDimensions } from "./graphNodeLayout";
import type { LayoutEdge, MeasuredNode, SnakeRowGuide } from "./incrementalLayout";

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

describe("pinLayoutAnchorCenter", () => {
    it("translates the whole layout so the anchor keeps its center", () => {
        const { nodes, edges } = makeGroupGraph();
        const collapsed = buildCollapsedLayout(nodes, edges, new Set(), "LR");
        const before = collapsed.get("group_a")!;
        const pin = {
            x: before.bbox.x + before.bbox.w / 2,
            y: before.bbox.y + before.bbox.h / 2,
        };

        const expanded = buildCollapsedLayout(nodes, edges, new Set(["group_a"]), "LR");
        pinLayoutAnchorCenter(expanded, "group_a", pin);
        const after = expanded.get("group_a")!;
        expect(after.bbox.x + after.bbox.w / 2).toBeCloseTo(pin.x, 5);
        expect(after.bbox.y + after.bbox.h / 2).toBeCloseTo(pin.y, 5);
    });
});

describe("expandGroupInLayout grows from center", () => {
    it("keeps the group center fixed when expanding", () => {
        const { nodes, edges } = makeGroupGraph();
        let layout = buildCollapsedLayout(nodes, edges, new Set(), "LR");
        const before = layout.get("group_a")!;
        const cx = before.bbox.x + before.bbox.w / 2;
        const cy = before.bbox.y + before.bbox.h / 2;

        layout = expandGroupInLayout(layout, "group_a", nodes, edges, "LR", ["a1", "a2"]);
        const after = layout.get("group_a")!;
        expect(after.bbox.x + after.bbox.w / 2).toBeCloseTo(cx, 5);
        expect(after.bbox.y + after.bbox.h / 2).toBeCloseTo(cy, 5);
        expect(after.bbox.w).toBeGreaterThan(before.bbox.w);
        expect(after.bbox.h).toBeGreaterThan(before.bbox.h);
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
        // must keep pipeline order left→right (snake / wrapRows=true).
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

    it("layoutLayeredDag flat LR/TB compact ranks by dependency depth, not chronology", () => {
        // Same graph as chronology test: compact (preferChronology=false) uses
        // longest-path so `best → late_consumer` shares layers with early→fiftyone.
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
        const lr = layoutLayeredDag(nodes, edges, "LR", undefined, false, false);
        expect(lr.get("best")!.x).toBeLessThan(lr.get("late_consumer")!.x);
        expect(lr.get("early")!.x).toBe(lr.get("best")!.x);
        expect(lr.get("fiftyone")!.x).toBe(lr.get("late_consumer")!.x);

        const tb = layoutLayeredDag(nodes, edges, "TB", undefined, false, false);
        expect(tb.get("best")!.y).toBeLessThan(tb.get("late_consumer")!.y);
        expect(tb.get("early")!.y).toBe(tb.get("best")!.y);
        expect(tb.get("fiftyone")!.y).toBe(tb.get("late_consumer")!.y);
    });

    it("layoutLayeredDag flat horizontal keeps chronology ribbon (non-compact)", () => {
        const nodes = new Map<string, MeasuredNode>();
        for (let i = 0; i < 9; i += 1) {
            const id = `s${i}`;
            nodes.set(id, {
                ...stubNode(id),
                pipelineOrderKey: String(i).padStart(4, "0"),
            });
        }
        // preferChronology defaults true — order keys alone make an L→R ribbon.
        const positions = layoutLayeredDag(nodes, [], "LR", undefined, false);
        expect(positions.get("s0")!.y).toBe(positions.get("s8")!.y);
        expect(positions.get("s0")!.x).toBeLessThan(positions.get("s4")!.x);
        expect(positions.get("s4")!.x).toBeLessThan(positions.get("s8")!.x);
    });

    it("layoutLayeredDag flat horizontal compact stacks parallel sources in one column", () => {
        const nodes = new Map<string, MeasuredNode>([
            ["a", { ...stubNode("a"), pipelineOrderKey: "0001" }],
            ["b", { ...stubNode("b"), pipelineOrderKey: "0002" }],
            ["c", { ...stubNode("c"), pipelineOrderKey: "0003" }],
            ["d", { ...stubNode("d"), pipelineOrderKey: "0004" }],
        ]);
        const edges: LayoutEdge[] = [
            { source: "a", target: "c" },
            { source: "b", target: "d" },
        ];
        const positions = layoutLayeredDag(nodes, edges, "LR", undefined, false, false);
        expect(positions.get("a")!.x).toBe(positions.get("b")!.x);
        expect(positions.get("c")!.x).toBe(positions.get("d")!.x);
        expect(positions.get("a")!.x).toBeLessThan(positions.get("c")!.x);
        expect(positions.get("a")!.y).not.toBe(positions.get("b")!.y);
    });

    it("layoutLayeredDag places step then stacked outs then next step L→R", () => {
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
        expect(positions.get("step")!.x).toBeLessThan(positions.get("out_a")!.x);
        expect(positions.get("out_a")!.x).toBe(positions.get("out_b")!.x);
        expect(positions.get("out_a")!.x).toBeLessThan(positions.get("next")!.x);
        expect(positions.get("out_a")!.y).toBeLessThan(positions.get("out_b")!.y);
    });

    it("layoutLayeredDag places input → step → output left-to-right", () => {
        const nodes = new Map<string, MeasuredNode>([
            [
                "in_t",
                {
                    ...stubNode("in_t"),
                    type: "table",
                    pipelineIndex: 2,
                    pipelineOrderKey: "0002.in.0000",
                },
            ],
            [
                "step",
                {
                    ...stubNode("step"),
                    pipelineIndex: 2,
                    pipelineOrderKey: "0002",
                },
            ],
            [
                "out_t",
                {
                    ...stubNode("out_t"),
                    type: "table",
                    pipelineIndex: 2,
                    pipelineOrderKey: "0002.out.0000",
                },
            ],
        ]);
        const positions = layoutLayeredDag(nodes, [], "LR");
        expect(positions.get("in_t")!.x).toBeLessThan(positions.get("step")!.x);
        expect(positions.get("step")!.x).toBeLessThan(positions.get("out_t")!.x);
    });

    it("layoutLayeredDag stacks parallel chains with empty mid slots", () => {
        const nodes = new Map<string, MeasuredNode>([
            [
                "t1",
                {
                    ...stubNode("t1"),
                    type: "table",
                    pipelineOrderKey: "0001.in.0000",
                },
            ],
            [
                "t2",
                {
                    ...stubNode("t2"),
                    type: "table",
                    pipelineOrderKey: "0001.in.0001",
                },
            ],
            [
                "step",
                {
                    ...stubNode("step"),
                    pipelineOrderKey: "0001",
                },
            ],
            [
                "t3",
                {
                    ...stubNode("t3"),
                    type: "table",
                    pipelineOrderKey: "0001.out.0000",
                },
            ],
            [
                "t4",
                {
                    ...stubNode("t4"),
                    type: "table",
                    pipelineOrderKey: "0001.out.0001",
                },
            ],
        ]);
        const edges: LayoutEdge[] = [
            { source: "t1", target: "step" },
            { source: "step", target: "t3" },
            { source: "t2", target: "t4" },
        ];
        const positions = layoutLayeredDag(nodes, edges, "LR");
        expect(positions.get("t1")!.x).toBe(positions.get("t2")!.x);
        expect(positions.get("t1")!.x).toBeLessThan(positions.get("step")!.x);
        expect(positions.get("step")!.x).toBeLessThan(positions.get("t3")!.x);
        expect(positions.get("t3")!.x).toBe(positions.get("t4")!.x);
        // Parallel inputs / outputs stack; mid column has only the transform.
        expect(positions.get("t1")!.y).not.toBe(positions.get("t2")!.y);
        expect(positions.get("t3")!.y).not.toBe(positions.get("t4")!.y);
    });

    it("layoutLayeredDag wraps long pipelines into a snake grid", () => {
        const nodes = new Map<string, MeasuredNode>();
        for (let i = 0; i < 9; i += 1) {
            const id = `s${i}`;
            nodes.set(id, {
                ...stubNode(id),
                pipelineOrderKey: String(i).padStart(4, "0"),
            });
        }
        const positions = layoutLayeredDag(nodes, [], "LR");
        // 9 steps → ~3×3 snake: row0 L→R, row1 R→L under the previous exit.
        expect(positions.get("s0")!.y).toBe(positions.get("s1")!.y);
        expect(positions.get("s0")!.y).toBe(positions.get("s2")!.y);
        expect(positions.get("s0")!.x).toBeLessThan(positions.get("s1")!.x);
        expect(positions.get("s1")!.x).toBeLessThan(positions.get("s2")!.x);
        expect(positions.get("s0")!.y).toBeLessThan(positions.get("s3")!.y);
        // Continuation stays on the right: s2 above s3, then leftward s3→s4→s5.
        expect(positions.get("s3")!.x).toBe(positions.get("s2")!.x);
        expect(positions.get("s5")!.x).toBeLessThan(positions.get("s4")!.x);
        expect(positions.get("s4")!.x).toBeLessThan(positions.get("s3")!.x);
        expect(positions.get("s5")!.x).toBe(positions.get("s0")!.x);
        // L→R row (s8): in → step → out left-to-right.
        nodes.set("in8", {
            ...stubNode("in8"),
            type: "table",
            pipelineOrderKey: "0008.in.0000",
        });
        nodes.set("out8", {
            ...stubNode("out8"),
            type: "table",
            pipelineOrderKey: "0008.out.0000",
        });
        // RTL row (s4): mirror so snake R→L still reads in → step → out
        // (outputs sit left of the step on screen).
        nodes.set("in4", {
            ...stubNode("in4"),
            type: "table",
            pipelineOrderKey: "0004.in.0000",
        });
        nodes.set("out4", {
            ...stubNode("out4"),
            type: "table",
            pipelineOrderKey: "0004.out.0000",
        });
        const withIo = layoutLayeredDag(nodes, [], "LR");
        expect(withIo.get("in8")!.x).toBeLessThan(withIo.get("s8")!.x);
        expect(withIo.get("s8")!.x).toBeLessThan(withIo.get("out8")!.x);
        expect(withIo.get("in8")!.y).toBe(withIo.get("s8")!.y);
        expect(withIo.get("s8")!.y).toBe(withIo.get("out8")!.y);
        expect(withIo.get("out4")!.x).toBeLessThan(withIo.get("s4")!.x);
        expect(withIo.get("s4")!.x).toBeLessThan(withIo.get("in4")!.x);
        expect(withIo.get("out4")!.y).toBe(withIo.get("s4")!.y);
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

    it("computeSnakeBoundaryPolygon makes an S-corridor with turn notches", () => {
        const rows: SnakeRowGuide[] = [
            {
                rtl: false,
                x1: 0,
                y1: 0,
                x2: 100,
                y2: 20,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
            {
                rtl: true,
                x1: 0,
                y1: 40,
                x2: 100,
                y2: 60,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
            {
                rtl: false,
                x1: 0,
                y1: 80,
                x2: 100,
                y2: 100,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
        ];
        const poly = computeSnakeBoundaryPolygon(rows);
        // Not a plain bounding rect — notches add vertices.
        expect(poly.length).toBeGreaterThan(4);

        // Right-turn outer vertical spans the gap at x=100 (row0.y2=20 → row1.y1=40).
        const rightGap = poly.some(
            (p, i) =>
                i > 0 &&
                Math.abs(p.x - 100) < 0.5 &&
                Math.abs(poly[i - 1].x - 100) < 0.5 &&
                Math.min(poly[i - 1].y, p.y) <= 20.5 &&
                Math.max(poly[i - 1].y, p.y) >= 39.5,
        );
        expect(rightGap).toBe(true);
        // Left-turn outer vertical spans the gap at x=0 (row1.y2=60 → row2.y1=80).
        const leftGap = poly.some(
            (p, i) =>
                i > 0 &&
                Math.abs(p.x) < 0.5 &&
                Math.abs(poly[i - 1].x) < 0.5 &&
                Math.min(poly[i - 1].y, p.y) <= 60.5 &&
                Math.max(poly[i - 1].y, p.y) >= 79.5,
        );
        expect(leftGap).toBe(true);

        // End-column width is 30, floored to 72 → notch inners at 28 and 72.
        expect(poly.some((p) => Math.abs(p.x - 28) < 0.5 && Math.abs(p.y - 40) < 0.5)).toBe(true);
        expect(poly.some((p) => Math.abs(p.x - 28) < 0.5 && Math.abs(p.y - 20) < 0.5)).toBe(true);
        expect(poly.some((p) => Math.abs(p.x - 72) < 0.5 && Math.abs(p.y - 60) < 0.5)).toBe(true);
        expect(poly.some((p) => Math.abs(p.x - 72) < 0.5 && Math.abs(p.y - 80) < 0.5)).toBe(true);
    });

    it("computeSnakeSpine drops vertically at left and right ends", () => {
        const rows: SnakeRowGuide[] = [
            {
                rtl: false,
                x1: 0,
                y1: 0,
                x2: 100,
                y2: 20,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
            {
                rtl: true,
                x1: 0,
                y1: 40,
                x2: 100,
                y2: 60,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
            {
                rtl: false,
                x1: 0,
                y1: 80,
                x2: 100,
                y2: 100,
                leftX1: 0,
                leftX2: 30,
                rightX1: 70,
                rightX2: 100,
            },
        ];
        const spine = computeSnakeSpine(rows);
        // L→R on row0 through node centers, vertical on right col, R→L, vertical on left.
        expect(spine[0]).toEqual({ x: 15, y: 10 });
        expect(spine.some((p, i) => i > 0 && p.x === 85 && spine[i - 1].x === 85)).toBe(true);
        expect(spine.some((p, i) => i > 0 && p.x === 15 && spine[i - 1].x === 15)).toBe(true);
        expect(spine[spine.length - 1]).toEqual({ x: 85, y: 90 });
    });
});

/** Long meta with sequential inners that would nest-wrap if wrapRows leaked in. */
function makeWideMetaPipeline(innerCount = 8, outerAfter = 8) {
    const nodes = new Map<string, Cytoscape.NodeDataDefinition>();
    const edges: Cytoscape.EdgeDataDefinition[] = [];
    const pipelineOrders = new Map<string, string[]>();

    nodes.set("group_a", {
        id: "group_a",
        name: "group_a",
        type: "group",
        child_count: innerCount,
        pipelineIndex: 0,
        pipelineOrderKey: "0000",
    });
    const innerIds: string[] = [];
    for (let i = 0; i < innerCount; i += 1) {
        const id = `a${i}`;
        innerIds.push(id);
        nodes.set(id, {
            id,
            name: id,
            type: "transform",
            metaGroup: "group_a",
            pipelineIndex: i,
            pipelineOrderKey: `0000.${String(i).padStart(4, "0")}`,
        });
        if (i > 0) edges.push({ source: `a${i - 1}`, target: id });
    }
    pipelineOrders.set("group_a", innerIds);

    nodes.set("group_b", {
        id: "group_b",
        name: "group_b",
        type: "group",
        child_count: 2,
        pipelineIndex: 1,
        pipelineOrderKey: "0001",
    });
    nodes.set("b0", {
        id: "b0",
        name: "b0",
        type: "transform",
        metaGroup: "group_b",
        pipelineIndex: 0,
        pipelineOrderKey: "0001.0000",
    });
    nodes.set("b1", {
        id: "b1",
        name: "b1",
        type: "transform",
        metaGroup: "group_b",
        pipelineIndex: 1,
        pipelineOrderKey: "0001.0001",
    });
    edges.push({ source: "b0", target: "b1" });
    pipelineOrders.set("group_b", ["b0", "b1"]);

    // Wire outer chronology through the metas (collapsed) / first-last inners.
    edges.push({ source: "group_a", target: "group_b" });
    edges.push({ source: `a${innerCount - 1}`, target: "b0" });

    for (let i = 0; i < outerAfter; i += 1) {
        const id = `s${i}`;
        nodes.set(id, {
            id,
            name: id,
            type: "transform",
            pipelineIndex: 2 + i,
            pipelineOrderKey: String(2 + i).padStart(4, "0"),
        });
        if (i === 0) {
            edges.push({ source: "group_b", target: id });
            edges.push({ source: "b1", target: id });
        } else {
            edges.push({ source: `s${i - 1}`, target: id });
        }
    }

    return { nodes, edges, pipelineOrders, innerIds };
}

function frameCenterY(layout: ReturnType<typeof buildCollapsedLayout>, id: string): number {
    const e = layout.get(id)!;
    return e.bbox.y + e.bbox.h / 2;
}

describe("expanded meta stays in flow (snake / horizontal / vertical)", () => {
    it("snake: expanded inners form one LR strip (no nested mini-snake tower)", () => {
        const { nodes, edges, pipelineOrders, innerIds } = makeWideMetaPipeline(9, 6);
        const layout = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "LR",
            pipelineOrders,
            true,
        );
        assertInnersInsideFrame(layout, "group_a");

        const frame = layout.get("group_a")!;
        const nodeH = graphNodeDimensions.transform.height;
        // Single strip + padding — not a 3-row nested snake (~3*h + 2*ROW_SEP).
        expect(frame.bbox.h).toBeLessThan(nodeH * 2 + GROUP_PADDING.top + GROUP_PADDING.bottom);

        const ys = innerIds.map((id) => {
            const e = layout.get(id)!;
            return e.bbox.y + e.bbox.h / 2;
        });
        const yMin = Math.min(...ys);
        const yMax = Math.max(...ys);
        expect(yMax - yMin).toBeLessThan(nodeH * 0.6);

        const xs = innerIds.map((id) => layout.get(id)!.bbox.x);
        for (let i = 1; i < xs.length; i += 1) {
            expect(xs[i]).toBeGreaterThan(xs[i - 1]);
        }
    });

    it("snake: expanding a mid meta pushes later steps further along the wrap", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(8, 9);
        const collapsed = buildCollapsedLayout(
            nodes,
            edges,
            new Set(),
            "LR",
            pipelineOrders,
            true,
        );
        const expanded = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "LR",
            pipelineOrders,
            true,
        );
        assertInnersInsideFrame(expanded, "group_a");

        const lastId = "s8";
        const cLast = collapsed.get(lastId)!;
        const eLast = expanded.get(lastId)!;
        const cA = collapsed.get("group_a")!;
        const eA = expanded.get("group_a")!;
        expect(eA.bbox.w).toBeGreaterThan(cA.bbox.w);

        // Downstream card moves: either later snake row or farther from the meta.
        const movedDown = eLast.snakeRow !== undefined && cLast.snakeRow !== undefined
            ? eLast.snakeRow! >= cLast.snakeRow!
            : true;
        const distCollapsed = Math.hypot(
            cLast.bbox.x - cA.bbox.x,
            cLast.bbox.y - cA.bbox.y,
        );
        const distExpanded = Math.hypot(
            eLast.bbox.x - eA.bbox.x,
            eLast.bbox.y - eA.bbox.y,
        );
        expect(movedDown || distExpanded > distCollapsed * 0.9).toBe(true);
        expect(eA.bbox.w + eLast.bbox.w).toBeGreaterThan(cA.bbox.w);
    });

    it("snake: expand/collapse sequences keep frames coherent and restore size", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(6, 6);
        const orders = pipelineOrders;

        const both = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a", "group_b"]),
            "LR",
            orders,
            true,
        );
        assertInnersInsideFrame(both, "group_a");
        assertInnersInsideFrame(both, "group_b");
        expect(both.get("group_a")!.bbox.x).toBeLessThan(both.get("group_b")!.bbox.x);

        const onlyB = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_b"]),
            "LR",
            orders,
            true,
        );
        assertInnersInsideFrame(onlyB, "group_b");
        expect(onlyB.get("group_a")!.node.type).not.toBe("group-expanded");
        expect(onlyB.get("a0")).toBeUndefined();

        const onlyA = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "LR",
            orders,
            true,
        );
        assertInnersInsideFrame(onlyA, "group_a");

        const none = buildCollapsedLayout(nodes, edges, new Set(), "LR", orders, true);
        expect(none.get("a0")).toBeUndefined();
        expect(none.get("b0")).toBeUndefined();
        expect(none.get("group_a")!.bbox.w).toBeLessThan(onlyA.get("group_a")!.bbox.w);
        expect(none.get("group_b")!.bbox.w).toBeLessThan(onlyB.get("group_b")!.bbox.w);

        // Reverse expand order: B then A then collapse B.
        const bThenA = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_b", "group_a"]),
            "LR",
            orders,
            true,
        );
        assertInnersInsideFrame(bThenA, "group_a");
        assertInnersInsideFrame(bThenA, "group_b");
        expect(bThenA.get("group_a")!.bbox.x).toBeLessThan(bThenA.get("group_b")!.bbox.x);
    });

    it("horizontal: expand widens the meta and shifts later nodes right", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(5, 3);
        const collapsed = buildCollapsedLayout(
            nodes,
            edges,
            new Set(),
            "LR",
            pipelineOrders,
            false,
        );
        const expanded = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "LR",
            pipelineOrders,
            false,
        );
        assertInnersInsideFrame(expanded, "group_a");

        expect(expanded.get("group_a")!.bbox.w).toBeGreaterThan(
            collapsed.get("group_a")!.bbox.w,
        );
        expect(expanded.get("group_b")!.bbox.x).toBeGreaterThan(
            collapsed.get("group_b")!.bbox.x,
        );
        expect(expanded.get("s0")!.bbox.x).toBeGreaterThan(collapsed.get("s0")!.bbox.x);
        // Same ribbon Y for top-level cards.
        expect(frameCenterY(expanded, "group_a")).toBeCloseTo(
            frameCenterY(expanded, "group_b"),
            0,
        );
    });

    it("horizontal: multi expand/collapse order stays left-to-right", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(4, 2);
        for (const expanded of [
            new Set<string>(["group_a"]),
            new Set<string>(["group_b"]),
            new Set<string>(["group_a", "group_b"]),
            new Set<string>(["group_b", "group_a"]),
            new Set<string>(),
        ]) {
            const layout = buildCollapsedLayout(
                nodes,
                edges,
                expanded,
                "LR",
                pipelineOrders,
                false,
            );
            expect(layout.get("group_a")!.bbox.x).toBeLessThan(layout.get("group_b")!.bbox.x);
            expect(layout.get("group_b")!.bbox.x).toBeLessThan(layout.get("s0")!.bbox.x);
            if (expanded.has("group_a")) assertInnersInsideFrame(layout, "group_a");
            if (expanded.has("group_b")) assertInnersInsideFrame(layout, "group_b");
        }
    });

    it("vertical: expand grows the meta and shifts later nodes down", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(5, 3);
        const collapsed = buildCollapsedLayout(
            nodes,
            edges,
            new Set(),
            "TB",
            pipelineOrders,
            false,
        );
        const expanded = buildCollapsedLayout(
            nodes,
            edges,
            new Set(["group_a"]),
            "TB",
            pipelineOrders,
            false,
        );
        assertInnersInsideFrame(expanded, "group_a");

        expect(expanded.get("group_a")!.bbox.h).toBeGreaterThan(
            collapsed.get("group_a")!.bbox.h,
        );
        expect(expanded.get("group_b")!.bbox.y).toBeGreaterThan(
            collapsed.get("group_b")!.bbox.y,
        );
        expect(expanded.get("s0")!.bbox.y).toBeGreaterThan(collapsed.get("s0")!.bbox.y);
    });

    it("vertical: multi expand/collapse order stays top-to-bottom", () => {
        const { nodes, edges, pipelineOrders } = makeWideMetaPipeline(4, 2);
        for (const expanded of [
            new Set<string>(["group_a"]),
            new Set<string>(["group_b"]),
            new Set<string>(["group_a", "group_b"]),
            new Set<string>(),
        ]) {
            const layout = buildCollapsedLayout(
                nodes,
                edges,
                expanded,
                "TB",
                pipelineOrders,
                false,
            );
            expect(layout.get("group_a")!.bbox.y).toBeLessThan(layout.get("group_b")!.bbox.y);
            expect(layout.get("group_b")!.bbox.y).toBeLessThan(layout.get("s0")!.bbox.y);
            if (expanded.has("group_a")) assertInnersInsideFrame(layout, "group_a");
            if (expanded.has("group_b")) assertInnersInsideFrame(layout, "group_b");
        }
    });

    it("snake: expanded frame without __orderKey suffix stays on the same row", () => {
        // Mirrors LabelStudioUploadTasks: unique meta name → id has no __0007 suffix.
        const collapsedNodes = new Map<string, Cytoscape.NodeDataDefinition>([
            [
                "early",
                {
                    id: "early",
                    name: "early",
                    type: "transform",
                    pipelineOrderKey: "0000",
                    pipelineIndex: 0,
                },
            ],
            [
                "LabelStudioUploadTasks",
                {
                    id: "LabelStudioUploadTasks",
                    name: "LabelStudioUploadTasks",
                    type: "group",
                    pipelineOrderKey: "0001",
                    pipelineIndex: 1,
                    child_count: 2,
                },
            ],
            [
                "late",
                {
                    id: "late",
                    name: "late",
                    type: "transform",
                    pipelineOrderKey: "0002",
                    pipelineIndex: 2,
                },
            ],
        ]);
        const expandedNodes = new Map<string, Cytoscape.NodeDataDefinition>([
            ...collapsedNodes,
            [
                "LabelStudioUploadTasks",
                {
                    ...collapsedNodes.get("LabelStudioUploadTasks")!,
                    type: "group-expanded",
                },
            ],
            [
                "t1",
                {
                    id: "t1",
                    name: "t1",
                    type: "transform",
                    metaGroup: "LabelStudioUploadTasks",
                    pipelineOrderKey: "0001.0000",
                    pipelineIndex: 0,
                },
            ],
            [
                "t2",
                {
                    id: "t2",
                    name: "t2",
                    type: "transform",
                    metaGroup: "LabelStudioUploadTasks",
                    pipelineOrderKey: "0001.0001",
                    pipelineIndex: 1,
                },
            ],
        ]);
        const edges: Cytoscape.EdgeDataDefinition[] = [
            { source: "early", target: "LabelStudioUploadTasks" },
            { source: "LabelStudioUploadTasks", target: "late" },
            { source: "t1", target: "t2" },
            { source: "early", target: "t1" },
            { source: "t2", target: "late" },
        ];
        const collapsedLayout = buildCollapsedLayout(
            collapsedNodes,
            edges,
            new Set(),
            "LR",
            new Map(),
            true,
        );
        const expandedLayout = buildCollapsedLayout(
            expandedNodes,
            edges,
            new Set(["LabelStudioUploadTasks"]),
            "LR",
            new Map([["LabelStudioUploadTasks", ["t1", "t2"]]]),
            true,
        );
        const c = collapsedLayout.get("LabelStudioUploadTasks")!;
        const e = expandedLayout.get("LabelStudioUploadTasks")!;
        expect(e.snakeRow).toBe(c.snakeRow);
        expect(e.bbox.x).toBeGreaterThan(collapsedLayout.get("early")!.bbox.x);
        expect(expandedLayout.get("late")!.bbox.x).toBeGreaterThan(e.bbox.x);
        assertInnersInsideFrame(expandedLayout, "LabelStudioUploadTasks");
    });
});
