import Cytoscape from "cytoscape";
import { edgeColors, graphColors } from "./graphColors";
import { getTransformPrimaryKeys } from "./nodeKeyChips";
import { groupBoxSize, stepNodeSize, tableNodeSize } from "./graphNodeLayout";

function nodeName(node: Cytoscape.NodeSingular): string {
    return (node.data("name") as string) || (node.data("label") as string) || "";
}

function nodeWidth(node: Cytoscape.NodeSingular): number {
    const name = nodeName(node);
    if (node.data("type") === "group") {
        return (node.data("boxW") as number) ?? groupBoxSize(name, node.data("child_count") ?? 1, getTransformPrimaryKeys(node.data())).w;
    }
    if (node.data("type") === "table") {
        return tableNodeSize(name, node.data("indexes") || [], false).w;
    }
    return stepNodeSize(name, false, getTransformPrimaryKeys(node.data())).w;
}

function nodeHeight(node: Cytoscape.NodeSingular): number {
    const name = nodeName(node);
    if (node.data("type") === "group") {
        return (node.data("boxH") as number) ?? groupBoxSize(name, node.data("child_count") ?? 1, getTransformPrimaryKeys(node.data())).h;
    }
    if (node.data("type") === "table") {
        return tableNodeSize(name, node.data("indexes") || [], false).h;
    }
    return stepNodeSize(name, false, getTransformPrimaryKeys(node.data())).h;
}

export const stylesheet: Cytoscape.Stylesheet[] = [
    {
        selector: "node",
        style: {
            shape: "round-rectangle",
            width: nodeWidth,
            height: nodeHeight,
            "background-opacity": 0,
            "border-width": 0,
            "overlay-opacity": 0,
            "z-index": 10,
            ghost: "no",
        },
    },
    {
        selector: ":active",
        style: {
            "overlay-opacity": 0,
        },
    },
    {
        selector: 'node[type = "group"]',
        style: {
            width: (node: Cytoscape.NodeSingular) =>
                (node.data("boxW") as number) ?? groupBoxSize(nodeName(node), node.data("child_count") ?? 1).w,
            height: (node: Cytoscape.NodeSingular) =>
                (node.data("boxH") as number) ?? groupBoxSize(nodeName(node), node.data("child_count") ?? 1).h,
        },
    },
    {
        selector: 'node[type = "group-expanded"]',
        style: {
            shape: "round-rectangle",
            backgroundColor: graphColors.group.expandedBg,
            "background-opacity": 1,
            "border-width": 2,
            "border-color": graphColors.group.expandedBorder,
            "border-style": "dashed",
            width: (node: Cytoscape.NodeSingular) =>
                (node.data("boxW") as number) ??
                groupBoxSize(nodeName(node), node.data("child_count") ?? 1).w,
            height: (node: Cytoscape.NodeSingular) =>
                (node.data("boxH") as number) ??
                groupBoxSize(nodeName(node), node.data("child_count") ?? 1).h,
            label: (node: Cytoscape.NodeSingular) =>
                (node.data("frameLabel") as string) || nodeName(node),
            "text-valign": "top",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "600px",
            "font-size": 13,
            "font-weight": 800,
            color: graphColors.group.text,
            "text-margin-y": -12,
            "z-index": 0,
            ghost: "no",
            // Decorative frame only: must not steal pointer hits, otherwise LMB-drag
            // inside the blue area cannot pan the camera. Inner step/table HTML
            // labels still receive clicks; empty-frame gestures are handled in
            // graphInteractions via hit-testing.
            events: "no",
        } as Cytoscape.Css.Node,
    },
    {
        selector: "node.focused",
        style: {
            "z-index": 50,
        },
    },
    {
        selector: "node.related",
        style: {
            "background-color": edgeColors.related,
            "background-opacity": 0.04,
            "border-width": 2,
            "border-color": edgeColors.related,
            "overlay-opacity": 0,
            "z-index": 35,
        },
    },
    {
        selector: "node.dimmed",
        style: {
            opacity: 0.35,
        },
    },
    {
        // Selection chrome is app-owned (see graphSelection.ts) and painted via
        // HTML label classes / `node.focused` — cytoscape's native `:selected`
        // state is never set, but this rule stays as a belt-and-suspenders no-op.
        selector: "node:selected",
        style: {},
    },
    {
        selector: "edge[internalMeta]",
        style: {
            opacity: 0,
            "z-index": 0,
            events: "no",
        },
    },
    {
        selector: "edge",
        style: {
            "curve-style": "taxi",
            "taxi-direction": "auto",
            "taxi-turn": "50%",
            "taxi-turn-min-distance": 22,
            width: 2.15,
            "line-color": edgeColors.default,
            "target-arrow-color": edgeColors.default,
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.08,
            "line-style": "solid",
            opacity: 0.78,
            "z-index": 1,
        },
    },
    {
        // Chronology spine: next pipeline step (not a data dependency).
        selector: "edge[sequential]",
        style: {
            width: 2.4,
            "line-style": "solid",
            "line-color": edgeColors.sequential,
            "target-arrow-color": edgeColors.sequential,
            "target-arrow-shape": "chevron",
            "arrow-scale": 1.25,
            opacity: 0.92,
            label: "next",
            "font-size": 11,
            "font-weight": 700,
            color: edgeColors.sequential,
            "text-background-color": graphColors.canvas.bg,
            "text-background-opacity": 0.92,
            "text-background-padding": "3px",
            "text-border-width": 1,
            "text-border-color": edgeColors.sequential,
            "text-border-opacity": 0.35,
            "text-rotation": "autorotate",
            "z-index": 2,
        },
    },
    {
        // The static column view mirrors the prototype: flowing connectors with
        // clearly visible arrowheads. Keep taxi routing for the classic DAG.
        selector: 'edge[layoutMode = "columns"]',
        style: {
            "curve-style": "bezier",
            width: 1.8,
            opacity: 0.72,
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.35,
            "source-distance-from-node": 2,
            "target-distance-from-node": 4,
        },
    },
    {
        selector: 'edge[sequential][layoutMode = "columns"]',
        style: {
            "target-arrow-shape": "chevron",
            "arrow-scale": 1.4,
            width: 2.2,
            opacity: 0.9,
        },
    },
    {
        selector: "edge.focused",
        style: {
            width: 3.2,
            opacity: 1,
            "line-color": edgeColors.active,
            "target-arrow-color": edgeColors.active,
            "z-index": 20,
        },
    },
    {
        selector: "edge.related",
        style: {
            width: 2.6,
            opacity: 0.9,
            "line-color": edgeColors.related,
            "target-arrow-color": edgeColors.related,
            "z-index": 20,
        },
    },
    {
        selector: "edge.muted",
        style: {
            opacity: 0.12,
        },
    },
    {
        selector: "edge.failed",
        style: {
            width: 2.8,
            opacity: 1,
            "line-color": edgeColors.error,
            "target-arrow-color": edgeColors.error,
            "z-index": 25,
        },
    },
    {
        selector: "node.label-hidden",
        style: {
            opacity: 0,
            display: "none",
            events: "no",
        },
    },
    {
        selector: "edge.label-hidden",
        style: {
            opacity: 0,
            display: "none",
            events: "no",
        },
    },
    {
        // Selection neighborhood: hide everything outside the focused subgraph.
        selector: "node.focus-hidden",
        style: {
            opacity: 0,
            display: "none",
            events: "no",
        },
    },
    {
        selector: "edge.focus-hidden",
        style: {
            opacity: 0,
            display: "none",
            events: "no",
        },
    },
];
