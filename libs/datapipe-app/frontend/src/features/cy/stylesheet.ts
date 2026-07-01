import Cytoscape from "cytoscape";
import { edgeColors, graphColors } from "./graphColors";
import { groupBoxSize, stepNodeSize, tableNodeSize } from "./graphNodeLayout";

function nodeName(node: Cytoscape.NodeSingular): string {
    return (node.data("name") as string) || (node.data("label") as string) || "";
}

function nodeWidth(node: Cytoscape.NodeSingular): number {
    const name = nodeName(node);
    const compact = Boolean(node.data("metaGroup"));
    if (node.data("type") === "group") {
        return (node.data("boxW") as number) ?? groupBoxSize(name, node.data("child_count") ?? 1).w;
    }
    if (node.data("type") === "table") {
        return tableNodeSize(name, node.data("indexes") || [], compact).w;
    }
    return stepNodeSize(name, compact).w;
}

function nodeHeight(node: Cytoscape.NodeSingular): number {
    const name = nodeName(node);
    const compact = Boolean(node.data("metaGroup"));
    if (node.data("type") === "group") {
        return (node.data("boxH") as number) ?? groupBoxSize(name, node.data("child_count") ?? 1).h;
    }
    if (node.data("type") === "table") {
        return tableNodeSize(name, node.data("indexes") || [], compact).h;
    }
    return stepNodeSize(name, compact).h;
}

export const stylesheet: Cytoscape.Stylesheet[] = [
    {
        selector: "node",
        style: {
            shape: "round-rectangle",
            "text-wrap": "wrap",
            "text-valign": "center",
            "text-halign": "center",
            width: nodeWidth,
            height: nodeHeight,
            "background-opacity": 0,
            "border-width": 0,
            "overlay-opacity": 0,
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
        selector: 'node[type = "transform"], node[type = "table"], node[type = "group"]',
        style: {
            "z-index": 10,
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
            "font-size": 22,
            "font-weight": 700,
            color: graphColors.group.text,
            "text-margin-y": -14,
            "z-index": 0,
            ghost: "no",
            events: "yes",
        } as Cytoscape.Css.Node,
    },
    {
        selector: "node.focused",
        style: {
            "z-index": 30,
        },
    },
    {
        selector: "node.dimmed",
        style: {
            opacity: 0.35,
        },
    },
    {
        selector: "node:selected",
        style: {
            "border-width": 4,
            "border-color": edgeColors.active,
            "background-color": edgeColors.active,
            "background-opacity": 0.08,
            "overlay-opacity": 0,
            "z-index": 40,
        },
    },
    {
        selector: "edge",
        style: {
            "curve-style": "taxi",
            "taxi-direction": "vertical",
            "taxi-turn": "50%",
            "taxi-turn-min-distance": 20,
            width: 1.8,
            "line-color": edgeColors.default,
            "target-arrow-color": edgeColors.default,
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.05,
            opacity: 0.72,
            "z-index": 1,
        },
    },
    {
        selector: "edge.focused",
        style: {
            width: 3,
            opacity: 1,
            "line-color": edgeColors.active,
            "target-arrow-color": edgeColors.active,
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
            width: 2.5,
            opacity: 1,
            "line-color": edgeColors.error,
            "target-arrow-color": edgeColors.error,
        },
    },
];
