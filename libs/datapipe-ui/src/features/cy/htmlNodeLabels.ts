import Cytoscape from "cytoscape";
import { nodeUsesHtmlLabel } from "./htmlLabelOpacity";

export type HtmlLabelAlign = "top" | "left" | "center" | "right" | "bottom";

export type HtmlNodeLabelConfig = {
    query: string;
    tpl: (data: Cytoscape.NodeDataDefinition) => string;
    halign?: HtmlLabelAlign;
    valign?: HtmlLabelAlign;
    halignBox?: HtmlLabelAlign;
    valignBox?: HtmlLabelAlign;
    cssClass?: string;
};

type LabelPosition = { w: number; h: number; x: number; y: number };

type LabelEntry = {
    root: HTMLDivElement;
    content: HTMLDivElement;
    config: HtmlNodeLabelConfig;
    dataKey: string;
    align: [number, number, number, number];
};

const layerStore = new WeakMap<Cytoscape.Core, HTMLDivElement>();
const initStore = new WeakMap<Cytoscape.Core, true>();

const ALIGN: Record<HtmlLabelAlign, number> = {
    top: -0.5,
    left: -0.5,
    center: 0,
    right: 0.5,
    bottom: 0.5,
};

function buildAlign(config: HtmlNodeLabelConfig): [number, number, number, number] {
    const halign = config.halign ?? "center";
    const valign = config.valign ?? "center";
    const halignBox = config.halignBox ?? "center";
    const valignBox = config.valignBox ?? "center";
    return [
        ALIGN[halign],
        ALIGN[valign],
        100 * (ALIGN[halignBox] - 0.5),
        100 * (ALIGN[valignBox] - 0.5),
    ];
}

/** Stable key for label HTML — ignores transient UI/focus/size/opacity fields. */
export function htmlLabelDataKey(data: Cytoscape.NodeDataDefinition): string {
    const copy: Record<string, unknown> = { ...data };
    delete copy.uiFocused;
    delete copy.uiSelected;
    delete copy.uiRelated;
    delete copy.uiDimmed;
    // Size/opacity animate every frame during expand/collapse; rebuild only on content.
    delete copy.boxW;
    delete copy.boxH;
    delete copy.htmlLabelOpacity;
    return JSON.stringify(copy);
}

function getNodePosition(node: Cytoscape.NodeSingular): LabelPosition {
    return {
        w: node.width(),
        h: node.height(),
        x: node.position("x"),
        y: node.position("y"),
    };
}

function renderLabelHtml(content: HTMLDivElement, html: string): void {
    while (content.firstChild) {
        content.removeChild(content.firstChild);
    }
    if (!html) return;
    const parsed = new DOMParser().parseFromString(html, "text/html");
    Array.from(parsed.body.children).forEach((child) => {
        content.appendChild(child);
    });
}

function applyLabelTransform(entry: LabelEntry, position: LabelPosition): void {
    // Keep the painted card size in sync with cytoscape box (morph updates
    // boxW/boxH without rebuilding label HTML). Size lives on the inner
    // `.node-compound-label`, not the absolute positioning root.
    const card = entry.content.firstElementChild as HTMLElement | null;
    if (card) {
        if (position.w > 0) card.style.width = `${position.w}px`;
        if (position.h > 0) card.style.height = `${position.h}px`;
    }
    const x = position.x + entry.align[0] * position.w;
    const y = position.y + entry.align[1] * position.h;
    const valRel = `translate(${entry.align[2]}%,${entry.align[3]}%) `;
    const valAbs = `translate(${x.toFixed(2)}px,${y.toFixed(2)}px)`;
    entry.root.style.transform = valRel + valAbs;
}

function ensureLayer(cy: Cytoscape.Core): HTMLDivElement | null {
    const container = cy.container();
    if (!container) return null;

    let layer = layerStore.get(cy);
    if (layer && container.contains(layer)) return layer;

    const canvas = container.querySelector("canvas");
    if (!canvas?.parentNode) return null;

    layer = document.createElement("div");
    layer.className = "cy-node-html-labels";
    layer.style.position = "absolute";
    layer.style.zIndex = "10";
    layer.style.width = "500px";
    layer.style.margin = "0";
    layer.style.padding = "0";
    layer.style.border = "0";
    layer.style.outline = "0";
    layer.style.pointerEvents = "none";
    canvas.parentNode.appendChild(layer);
    layerStore.set(cy, layer);
    return layer;
}

function updateLayerPanZoom(cy: Cytoscape.Core): void {
    const layer = layerStore.get(cy);
    if (!layer) return;
    const pan = cy.pan();
    const zoom = cy.zoom();
    layer.style.transform = `translate(${pan.x}px,${pan.y}px) scale(${zoom})`;
    layer.style.transformOrigin = "top left";
}

function matchingConfig(
    configs: HtmlNodeLabelConfig[],
    node: Cytoscape.NodeSingular,
): HtmlNodeLabelConfig | undefined {
    for (let i = configs.length - 1; i >= 0; i -= 1) {
        if (node.is(configs[i].query)) return configs[i];
    }
    return undefined;
}

/**
 * Custom HTML label layer for Cytoscape nodes.
 *
 * Replaces cytoscape-node-html-label which rebuilds label DOM on every `style`
 * event (including transient `:active`), causing selection flicker and camera jumps.
 * We only rebuild when node *data* content changes (labelRefresh, run status, etc.).
 */
export function initHtmlNodeLabels(cy: Cytoscape.Core, configs: HtmlNodeLabelConfig[]): void {
    if (initStore.has(cy)) return;
    initStore.set(cy, true);

    const entries = new Map<string, LabelEntry>();

    const removeNode = (nodeId: string) => {
        const entry = entries.get(nodeId);
        if (!entry) return;
        entry.root.remove();
        entries.delete(nodeId);
    };

    const upsertNode = (node: Cytoscape.NodeSingular) => {
        if (!nodeUsesHtmlLabel(node)) {
            removeNode(node.id());
            return;
        }

        const config = matchingConfig(configs, node);
        if (!config) {
            removeNode(node.id());
            return;
        }

        const layer = ensureLayer(cy);
        if (!layer) return;

        const html = config.tpl(node.data());
        const dataKey = htmlLabelDataKey(node.data());
        const position = getNodePosition(node);
        let entry = entries.get(node.id());

        if (!entry) {
            const root = document.createElement("div");
            root.style.position = "absolute";
            if (config.cssClass) root.classList.add(config.cssClass);
            const content = document.createElement("div");
            root.appendChild(content);
            layer.appendChild(root);
            entry = {
                root,
                content,
                config,
                dataKey: "",
                align: buildAlign(config),
            };
            entries.set(node.id(), entry);
        }

        entry.config = config;
        entry.align = buildAlign(config);

        if (entry.dataKey !== dataKey) {
            entry.dataKey = dataKey;
            renderLabelHtml(entry.content, html);
        }

        applyLabelTransform(entry, position);
    };

    const syncAll = () => {
        if (cy.destroyed()) return;
        configs.forEach((cfg) => {
            cy.elements(cfg.query).forEach((ele) => {
                if (ele.isNode()) upsertNode(ele as Cytoscape.NodeSingular);
            });
        });
    };

    const onData = (event: Cytoscape.EventObject) => {
        const target = event.target;
        if (!target.isNode()) return;
        upsertNode(target as Cytoscape.NodeSingular);
    };

    const onPosition = (event: Cytoscape.EventObject) => {
        const target = event.target;
        if (!target.isNode()) return;
        const entry = entries.get(target.id());
        if (!entry) return;
        applyLabelTransform(entry, getNodePosition(target as Cytoscape.NodeSingular));
    };

    cy.one("render", () => {
        ensureLayer(cy);
        syncAll();
        updateLayerPanZoom(cy);
    });

    cy.on("add", onData);
    cy.on("remove", (event) => removeNode(event.target.id()));
    cy.on("data", onData);
    cy.on("position bounds", onPosition);
    cy.on("pan zoom", () => updateLayerPanZoom(cy));

    cy.one("destroy", () => {
        entries.forEach((entry) => entry.root.remove());
        entries.clear();
        layerStore.delete(cy);
        initStore.delete(cy);
    });
}
