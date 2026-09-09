import Cytoscape from "cytoscape";

export type KeyKind = "pk" | "tpk" | "label";

export type VisibleKeyChips = {
    visible: string[];
    hidden: string[];
    hiddenCount: number;
    mode: "none" | "one-row" | "two-rows" | "overflow";
};

export function getTransformPrimaryKeys(data: Cytoscape.NodeDataDefinition): string[] {
    return (
        (data.transform_primary_keys as string[] | undefined) ??
        (data.tpk as string[] | undefined) ??
        (data.indexes as string[] | undefined) ??
        (data.primary_keys as string[] | undefined) ??
        []
    );
}

export function getVisibleKeyChips(keys: string[]): VisibleKeyChips {
    if (!keys.length) {
        return { visible: [], hidden: [], hiddenCount: 0, mode: "none" };
    }

    if (keys.length <= 3) {
        return {
            visible: keys,
            hidden: [],
            hiddenCount: 0,
            mode: "one-row",
        };
    }

    if (keys.length <= 6) {
        return {
            visible: keys,
            hidden: [],
            hiddenCount: 0,
            mode: "two-rows",
        };
    }

    const visible = keys.slice(0, 3);
    const hidden = keys.slice(3);

    return {
        visible,
        hidden,
        hiddenCount: hidden.length,
        mode: "overflow",
    };
}

export function escapeHtml(value: string): string {
    return value
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

export function formatNodeLabels(labels: string[][] | undefined): string[] {
    if (!labels?.length) return [];
    return labels.map(([k, v]) => `${k}=${v}`);
}

export function renderKeyChipList(nodeId: string, kind: KeyKind, keys: string[]): string {
    if (!keys.length) return "";

    const label = kind === "pk" ? "PK" : kind === "tpk" ? "TPK" : "Labels";

    const chips = keys
        .map(
            (key) => `
        <span class="node-key-chip ${kind}" title="${escapeHtml(key)}">
          ${escapeHtml(key)}
        </span>
      `,
        )
        .join("");

    // All keys render in a single horizontally-scrollable row so long/overflowing
    // keys can be revealed by scrolling instead of being clipped.
    return `
    <div class="node-key-row ${kind}">
      <span class="node-key-label">${label}</span>
      <span
        class="node-key-chips scrollable"
        data-key-scroll="true"
        data-cy-node-id="${escapeHtml(nodeId)}"
        data-key-kind="${kind}"
      >
        ${chips}
      </span>
    </div>
  `;
}

export function renderLabelChipList(nodeId: string, labels: string[][] | undefined): string {
    return renderKeyChipList(nodeId, "label", formatNodeLabels(labels));
}
