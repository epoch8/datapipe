import Cytoscape from "cytoscape";
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
            ghost: "yes",
            "ghost-opacity": 0.12,
            "ghost-offset-x": 4,
            "ghost-offset-y": 4,
            height: nodeHeight,
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
            "background-opacity": 0,
            "border-width": 0,
            ghost: "no",
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
            backgroundColor: "#eef6ff",
            "background-opacity": 0.45,
            "border-width": 2,
            "border-color": "#91caff",
            "border-style": "dashed",
            width: (node: Cytoscape.NodeSingular) =>
                (node.data("boxW") as number) ??
                groupBoxSize(nodeName(node), node.data("child_count") ?? 1).w,
            height: (node: Cytoscape.NodeSingular) =>
                (node.data("boxH") as number) ??
                groupBoxSize(nodeName(node), node.data("child_count") ?? 1).h,
            label: "data(name)",
            "text-valign": "top",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "600px",
            "font-size": 22,
            "font-weight": 700,
            color: "#0958d9",
            "text-margin-y": -14,
            "z-index": 0,
            ghost: "no",
            events: "yes",
        } as Cytoscape.Css.Node,
    },
    {
        selector: "edge",
        style: {
            "curve-style": "bezier",
            "taxi-direction": "horizontal",
            "line-color": "#b0b0b0",
            "target-arrow-shape": "triangle",
            "target-arrow-color": "#b0b0b0",
            "arrow-scale": 1.6,
            width: 2.5,
        },
    },
    {
        selector: ":selected",
        style: {
            "border-width": 1,
            "line-color": "#000",
            "target-arrow-color": "#000",
        },
    },
];
