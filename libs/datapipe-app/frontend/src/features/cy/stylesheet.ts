import Cytoscape from "cytoscape";

export const stylesheet: Cytoscape.Stylesheet[] = [
    {
        selector: "node",
        style: {
            shape: "barrel",
            "text-wrap": "wrap",
            "text-valign": "center",
            "text-halign": "center",
            width: (node: any) =>
                Math.max(
                    node.data("label").length * 10,
                    (node.data("indexes") || []).join(", ").length * 6,
                ),
            ghost: "yes",
            "ghost-opacity": 0.2,
            "ghost-offset-x": 10,
            "ghost-offset-y": 10,
            height: 70,
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
        selector: ":parent",
        style: {
            "text-valign": "top",
            "text-halign": "center",
            height: 200,
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
            width: 2,
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
