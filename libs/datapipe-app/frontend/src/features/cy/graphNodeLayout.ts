export const TABLE_NAME_MAX_LEN = 64;
export const TABLE_CHARS_PER_LINE = 28;

export const NODE_MIN_WIDTH = 110;
export const NODE_MAX_WIDTH = 180;
export const NODE_STEP_HEIGHT = 64;
export const NODE_TABLE_HEIGHT = 58;
export const TABLE_NODE_MAX_WIDTH = 300;

export const GROUP_MIN_WIDTH = 150;
export const GROUP_MAX_WIDTH = 340;
export const GROUP_MIN_HEIGHT = 76;
export const GROUP_MAX_HEIGHT = 200;

export const SUBGRAPH_STEP_WIDTH = 78;
export const SUBGRAPH_STEP_HEIGHT = 42;
export const SUBGRAPH_TABLE_WIDTH = 72;
export const SUBGRAPH_TABLE_HEIGHT = 38;
export const SUBGRAPH_GAP = 6;

export function displayNodeName(name: string, maxLen = 26): string {
    if (name.length <= maxLen) return name;
    return `${name.slice(0, maxLen - 1)}…`;
}

/** Full table name (DB limit 64); subgraph nodes stay compact. */
export function tableDisplayName(name: string, compact: boolean): string {
    const trimmed = name.slice(0, TABLE_NAME_MAX_LEN);
    return compact ? displayNodeName(trimmed, 14) : trimmed;
}

export function tableNameLineCount(name: string): number {
    const len = Math.min(name.length, TABLE_NAME_MAX_LEN);
    return Math.max(1, Math.ceil(len / TABLE_CHARS_PER_LINE));
}

export function nodeWidthFromLabel(label: string, charWidth = 7): number {
    const estimated = label.length * charWidth;
    return Math.min(NODE_MAX_WIDTH, Math.max(NODE_MIN_WIDTH, estimated));
}

export function tableNodeWidth(name: string, indexes: string[] = [], compact = false): number {
    if (compact) {
        const estimated = Math.min(name.length, 14) * 5;
        return Math.min(SUBGRAPH_TABLE_WIDTH, Math.max(72, estimated));
    }
    const displayLen = Math.min(name.length, TABLE_NAME_MAX_LEN);
    const charsOnLongestLine = Math.min(displayLen, TABLE_CHARS_PER_LINE);
    const indexesText = indexes.join(", ");
    const estimated = Math.max(charsOnLongestLine * 6.5, indexesText.length * 5);
    return Math.min(TABLE_NODE_MAX_WIDTH, Math.max(NODE_MIN_WIDTH, estimated));
}

export function tableNodeHeight(name: string, compact = false): number {
    if (compact) return SUBGRAPH_TABLE_HEIGHT;
    const lines = tableNameLineCount(name);
    return NODE_TABLE_HEIGHT + (lines - 1) * 13;
}

/** Collapsed pipeline-step group — large, reserves space proportional to child count. */
export function groupNodeWidth(childCount: number): number {
    const count = Math.max(1, childCount);
    return Math.min(GROUP_MAX_WIDTH, Math.max(GROUP_MIN_WIDTH, 120 + count * 24));
}

export function groupNodeHeight(childCount: number): number {
    const count = Math.max(1, childCount);
    return Math.min(GROUP_MAX_HEIGHT, Math.max(GROUP_MIN_HEIGHT, 56 + count * 14));
}

export function subgraphNameMaxLen(compact: boolean): number {
    return compact ? 14 : 26;
}
