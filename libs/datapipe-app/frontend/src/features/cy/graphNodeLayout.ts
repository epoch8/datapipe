export const TABLE_NAME_MAX_LEN = 64;

export const NODE_MAX_LINES = 3;

export const graphNodeDimensions = {
    table: { width: 230, height: 72 },
    transform: { width: 250, height: 92 },
    groupCollapsed: { width: 250, height: 82 },
    compactTable: { width: 190, height: 58 },
    compactTransform: { width: 210, height: 72 },
    horizontalGap: 58,
    verticalGap: 68,
} as const;

type SizeSpec = {
    charW: number;
    lineH: number;
    maxChars: number;
    minW: number;
    maxW: number;
    padX: number;
    padTop: number;
    padBottom: number;
    extraH: number;
};

const STEP_NORMAL: SizeSpec = {
    charW: 11, lineH: 14, maxChars: 18, minW: 250, maxW: 300, padX: 56, padTop: 11, padBottom: 11, extraH: 28,
};
const STEP_COMPACT: SizeSpec = {
    charW: 10, lineH: 13, maxChars: 16, minW: 210, maxW: 260, padX: 48, padTop: 8, padBottom: 8, extraH: 20,
};
const TABLE_NORMAL: SizeSpec = {
    charW: 11, lineH: 14, maxChars: 18, minW: 230, maxW: 290, padX: 56, padTop: 11, padBottom: 11, extraH: 0,
};
const TABLE_COMPACT: SizeSpec = {
    charW: 10, lineH: 13, maxChars: 16, minW: 190, maxW: 240, padX: 48, padTop: 8, padBottom: 8, extraH: 0,
};

function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

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

function sizeFor(name: string, spec: SizeSpec, extraLines = 0): NodeSize {
    const { lines, maxLen } = wrapName(name, spec.maxChars);
    const w = clamp(Math.round(maxLen * spec.charW) + spec.padX, spec.minW, spec.maxW);
    const h = clamp(
        spec.padTop + lines.length * spec.lineH + extraLines * spec.lineH + spec.extraH + spec.padBottom,
        spec.minW === STEP_NORMAL.minW ? graphNodeDimensions.transform.height : graphNodeDimensions.table.height,
        spec.minW === STEP_NORMAL.minW ? 120 : 96,
    );
    return { w, h, lines };
}

export function stepNodeSize(name: string, compact: boolean): NodeSize {
    const base = sizeFor(name, compact ? STEP_COMPACT : STEP_NORMAL);
    const target = compact ? graphNodeDimensions.compactTransform : graphNodeDimensions.transform;
    return { ...base, w: Math.max(base.w, target.width), h: Math.max(base.h, target.height) };
}

export function tableNodeSize(name: string, indexes: string[] = [], compact: boolean): NodeSize {
    const trimmed = name.slice(0, TABLE_NAME_MAX_LEN);
    const hasIndexes = !compact && indexes.length > 0;
    const base = sizeFor(trimmed, compact ? TABLE_COMPACT : TABLE_NORMAL, hasIndexes ? 1 : 0);
    const target = compact ? graphNodeDimensions.compactTable : graphNodeDimensions.table;
    const h = Math.max(base.h, target.height + (hasIndexes ? 10 : 0));
    return { ...base, w: Math.max(base.w, target.width), h };
}

export const GROUP_MIN_WIDTH = graphNodeDimensions.groupCollapsed.width;
export const GROUP_MAX_WIDTH = 320;
export const GROUP_MIN_HEIGHT = graphNodeDimensions.groupCollapsed.height;
export const GROUP_MAX_HEIGHT = 140;

export function groupBoxSize(name: string, childCount: number): NodeSize {
    const { lines, maxLen } = wrapName(name, 22, 2);
    const w = clamp(Math.max(GROUP_MIN_WIDTH, maxLen * 11 + 56), GROUP_MIN_WIDTH, GROUP_MAX_WIDTH);
    const h = clamp(Math.max(GROUP_MIN_HEIGHT, lines.length * 14 + 52), GROUP_MIN_HEIGHT, GROUP_MAX_HEIGHT);
    return { w, h, lines };
}
