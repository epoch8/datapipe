export const TABLE_NAME_MAX_LEN = 64;

export const NODE_MAX_LINES = 3;

// Card geometry.
//  - Tables (orange) and transforms (green) share one size so the graph reads
//    as a clean grid.
//  - Collapsed groups (blue) are slightly larger to signal they contain steps.
//  - Height fits title + subtitle + TPK/PK row + Labels row without clipping.
const NODE_WIDTH = 420;
const NODE_HEIGHT = 156;
const GROUP_WIDTH = 450;
const GROUP_HEIGHT = 168;
const COMPACT_HEIGHT = 72;

// Mirrors `.node-content`: 16px side padding + 28px icon + 12px column gap.
const CARD_TEXT_GUTTER_PX = 72;
// Average glyph width for 13px/800 snake_case in Inter (measured ~6.5–6.8px).
const CHAR_WIDTH_PX = 6.6;
const LINE_FILL_RATIO = 0.9;

function charsPerLine(cardWidth: number): number {
    const textPx = Math.max(80, cardWidth - CARD_TEXT_GUTTER_PX);
    return Math.max(12, Math.floor((textPx * LINE_FILL_RATIO) / CHAR_WIDTH_PX));
}

export const graphNodeDimensions = {
    table: { width: NODE_WIDTH, height: NODE_HEIGHT, heightTwoRows: NODE_HEIGHT },
    transform: { width: NODE_WIDTH, height: NODE_HEIGHT, heightTwoRows: NODE_HEIGHT },
    groupCollapsed: { width: GROUP_WIDTH, height: GROUP_HEIGHT, heightTwoRows: GROUP_HEIGHT },
    compactTable: { width: 280, height: COMPACT_HEIGHT },
    compactTransform: { width: 280, height: COMPACT_HEIGHT },
    compactGroup: { width: 300, height: COMPACT_HEIGHT },
    horizontalGap: 58,
    verticalGap: 68,
} as const;

export function displayNodeName(name: string, maxLen = 26): string {
    if (name.length <= maxLen) return name;
    return `${name.slice(0, maxLen - 1)}…`;
}

function splitTokens(name: string): string[] {
    const parts = name.split("_");
    return parts
        .map((part, index) => (index < parts.length - 1 ? `${part}_` : part))
        .filter((token) => token.length > 0);
}

export type WrappedName = { lines: string[]; maxLen: number; truncated: boolean };

export function wrapName(name: string, maxChars: number, maxLines = NODE_MAX_LINES): WrappedName {
    const tokens = splitTokens(name);
    const lines: string[] = [];
    let current = "";

    const flushHard = (chunk: string): string => {
        let rest = chunk;
        while (rest.length > maxChars) {
            lines.push(rest.slice(0, maxChars));
            rest = rest.slice(maxChars);
        }
        return rest;
    };

    tokens.forEach((token) => {
        if (token.length > maxChars) {
            if (current) {
                lines.push(current);
                current = "";
            }
            current = flushHard(token);
            return;
        }
        if (current && current.length + token.length > maxChars) {
            lines.push(current);
            current = token;
        } else {
            current += token;
        }
    });
    if (current) lines.push(current);

    let truncated = false;
    if (lines.length > maxLines) {
        lines.length = maxLines;
        truncated = true;
        const last = lines[maxLines - 1];
        lines[maxLines - 1] = `${last.length > maxChars - 1 ? last.slice(0, maxChars - 1) : last}…`;
    }

    const maxLen = lines.reduce((acc, line) => Math.max(acc, line.length), 1);
    return { lines, maxLen, truncated };
}

export type NodeSize = { w: number; h: number; lines: string[] };

export function stepNodeSize(name: string, compact: boolean, _tpk: string[] = []): NodeSize {
    const dim = compact ? graphNodeDimensions.compactTransform : graphNodeDimensions.transform;
    const { lines } = wrapName(name, charsPerLine(dim.width));
    return { w: dim.width, h: dim.height, lines };
}

export function tableNodeSize(name: string, _indexes: string[] = [], compact: boolean): NodeSize {
    const trimmed = name.slice(0, TABLE_NAME_MAX_LEN);
    const dim = compact ? graphNodeDimensions.compactTable : graphNodeDimensions.table;
    const { lines } = wrapName(trimmed, charsPerLine(dim.width));
    return { w: dim.width, h: dim.height, lines };
}

export const GROUP_MIN_WIDTH = graphNodeDimensions.groupCollapsed.width;
export const GROUP_MAX_WIDTH = graphNodeDimensions.groupCollapsed.width;
export const GROUP_MIN_HEIGHT = graphNodeDimensions.groupCollapsed.height;
export const GROUP_MAX_HEIGHT = graphNodeDimensions.groupCollapsed.height;

export function groupBoxSize(name: string, _childCount: number, _tpk: string[] = []): NodeSize {
    const dim = graphNodeDimensions.groupCollapsed;
    const { lines } = wrapName(name, charsPerLine(dim.width), 2);
    return {
        w: dim.width,
        h: dim.height,
        lines,
    };
}
