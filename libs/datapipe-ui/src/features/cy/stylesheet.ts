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
            events: "yes",
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
            "background-opacity": 0.08,
            "border-width": 3,
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
        selector: "node:selected",
        style: {
            "background-color": edgeColors.active,
            "background-opacity": 0.08,
            "border-width": 4,
            "border-color": edgeColors.active,
            "overlay-opacity": 0,
            "z-index": 40,
        },
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
            "taxi-direction": "vertical",
            "taxi-turn": "50%",
            "taxi-turn-min-distance": 22,
            width: 2.15,
            "line-color": edgeColors.default,
            "target-arrow-color": edgeColors.default,
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.08,
            opacity: 0.78,
            "z-index": 1,
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
            width: 3.2,
            opacity: 1,
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
];
