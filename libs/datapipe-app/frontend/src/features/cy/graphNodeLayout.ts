export const TABLE_NAME_MAX_LEN = 64;

export const NODE_MAX_LINES = 3;

export const graphNodeDimensions = {
    table: { width: 260, height: 98, heightTwoRows: 122 },
    transform: { width: 300, height: 112, heightTwoRows: 134 },
    groupCollapsed: { width: 300, height: 104, heightTwoRows: 126 },
    compactTable: { width: 210, height: 68 },
    compactTransform: { width: 230, height: 72 },
    compactGroup: { width: 230, height: 72 },
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
        spec.minW === STEP_NORMAL.minW ? 140 : 130,
    );
    return { w, h, lines };
}

export function keyRowCount(keys: string[]): 0 | 1 | 2 {
    if (!keys.length) return 0;
    if (keys.length <= 3) return 1;
    if (keys.length <= 6) return 2;
    return 1;
}

export function stepNodeSize(name: string, compact: boolean, tpk: string[] = []): NodeSize {
    if (compact) {
        return {
            ...sizeFor(name, STEP_COMPACT),
            w: graphNodeDimensions.compactTransform.width,
            h: graphNodeDimensions.compactTransform.height,
        };
    }

    const rows = keyRowCount(tpk);
    const base = sizeFor(name, STEP_NORMAL, rows === 2 ? 1 : 0);
    const targetHeight =
        rows === 2
            ? graphNodeDimensions.transform.heightTwoRows
            : graphNodeDimensions.transform.height;

    return {
        ...base,
        w: Math.max(base.w, graphNodeDimensions.transform.width),
        h: Math.max(base.h, targetHeight + (rows === 0 ? 0 : 0)),
    };
}

export function tableNodeSize(name: string, indexes: string[] = [], compact: boolean): NodeSize {
    const trimmed = name.slice(0, TABLE_NAME_MAX_LEN);
    if (compact) {
        return {
            ...sizeFor(trimmed, TABLE_COMPACT),
            w: graphNodeDimensions.compactTable.width,
            h: graphNodeDimensions.compactTable.height,
        };
    }

    const rows = keyRowCount(indexes);
    const base = sizeFor(trimmed, TABLE_NORMAL, rows === 2 ? 1 : 0);
    const targetHeight =
        rows === 2
            ? graphNodeDimensions.table.heightTwoRows
            : graphNodeDimensions.table.height;

    return {
        ...base,
        w: Math.max(base.w, graphNodeDimensions.table.width),
        h: Math.max(base.h, targetHeight),
    };
}

export const GROUP_MIN_WIDTH = graphNodeDimensions.groupCollapsed.width;
export const GROUP_MAX_WIDTH = 320;
export const GROUP_MIN_HEIGHT = graphNodeDimensions.groupCollapsed.height;
export const GROUP_MAX_HEIGHT = 150;

export function groupBoxSize(name: string, childCount: number, tpk: string[] = []): NodeSize {
    const { lines, maxLen } = wrapName(name, 22, 2);
    const rows = keyRowCount(tpk);
    const w = clamp(Math.max(GROUP_MIN_WIDTH, maxLen * 11 + 56), GROUP_MIN_WIDTH, GROUP_MAX_WIDTH);
    const baseH = clamp(Math.max(GROUP_MIN_HEIGHT, lines.length * 14 + 52), GROUP_MIN_HEIGHT, GROUP_MAX_HEIGHT);
    const targetHeight =
        rows === 2
            ? graphNodeDimensions.groupCollapsed.heightTwoRows
            : graphNodeDimensions.groupCollapsed.height;
    const h = Math.max(baseH, rows > 0 ? targetHeight : GROUP_MIN_HEIGHT);
    return { w, h, lines };
}
