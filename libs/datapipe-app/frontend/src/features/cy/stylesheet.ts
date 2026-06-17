import Cytoscape from "cytoscape";
import {
    NODE_STEP_HEIGHT,
    SUBGRAPH_STEP_HEIGHT,
    SUBGRAPH_STEP_WIDTH,
    SUBGRAPH_TABLE_HEIGHT,
    SUBGRAPH_TABLE_WIDTH,
    groupNodeHeight,
    groupNodeWidth,
    nodeWidthFromLabel,
    tableNodeHeight,
    tableNodeWidth,
} from "./graphNodeLayout";

export const stylesheet: Cytoscape.Stylesheet[] = [
    {
        selector: "node",
        style: {
            shape: "round-rectangle",
            "text-wrap": "wrap",
            "text-valign": "center",
            "text-halign": "center",
            width: (node: Cytoscape.NodeSingular) => {
                const label = (node.data("label") as string) || "";
                if (node.data("metaGroup")) {
                    return node.data("type") === "table" ? SUBGRAPH_TABLE_WIDTH : SUBGRAPH_STEP_WIDTH;
                }
                if (node.data("type") === "table") {
                    const label = (node.data("label") as string) || "";
                    return tableNodeWidth(label, node.data("indexes") || [], Boolean(node.data("metaGroup")));
                }
                if (node.data("type") === "group") {
                    return groupNodeWidth(node.data("child_count") ?? 1);
                }
                return nodeWidthFromLabel(label);
            },
            ghost: "yes",
            "ghost-opacity": 0.12,
            "ghost-offset-x": 4,
            "ghost-offset-y": 4,
            height: (node: Cytoscape.NodeSingular) => {
                if (node.data("metaGroup")) {
                    return node.data("type") === "table" ? SUBGRAPH_TABLE_HEIGHT : SUBGRAPH_STEP_HEIGHT;
                }
                if (node.data("type") === "table") {
                    const label = (node.data("label") as string) || "";
                    return tableNodeHeight(label, Boolean(node.data("metaGroup")));
                }
                if (node.data("type") === "group") {
                    return groupNodeHeight(node.data("child_count") ?? 1);
                }
                return NODE_STEP_HEIGHT;
            },
        },
    },
    {
        selector: ":active",
        style: {
            "overlay-opacity": 0,
        },
    },
    {
        selector: 'node[type = "table"]',
        style: {
            backgroundColor: "#FFC18E",
        },
    },
    {
        selector: 'node[type = "transform"]',
        style: {
            backgroundColor: "#90C8AC",
        },
    },
    {
        selector: 'node[type = "group"]',
        style: {
            backgroundColor: "#e6f4ff",
            "border-width": 2,
            "border-color": "#69b1ff",
            "border-style": "dashed",
        },
    },
    {
        selector: 'node[?metaGroup]',
        style: {
            "border-width": 1.5,
            "border-color": "#69b1ff",
            "border-style": "dashed",
            "background-opacity": 0.95,
        },
    },
    {
        selector: "edge",
        style: {
            "curve-style": "bezier",
            "taxi-direction": "horizontal",
            "line-color": "#b0b0b0",
            "target-arrow-shape": "triangle",
            "target-arrow-color": "#b0b0b0",
            width: 1.5,
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
