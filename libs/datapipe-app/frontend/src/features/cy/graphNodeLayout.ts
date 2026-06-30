export const TABLE_NAME_MAX_LEN = 64;

export const NODE_MAX_LINES = 4;

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
    charW: 18, lineH: 30, maxChars: 22, minW: 380, maxW: 680, padX: 52, padTop: 20, padBottom: 20, extraH: 44,
};
const STEP_COMPACT: SizeSpec = {
    charW: 16, lineH: 28, maxChars: 18, minW: 300, maxW: 520, padX: 38, padTop: 16, padBottom: 16, extraH: 40,
};
const TABLE_NORMAL: SizeSpec = {
    charW: 17, lineH: 30, maxChars: 22, minW: 380, maxW: 720, padX: 52, padTop: 20, padBottom: 20, extraH: 0,
};
const TABLE_COMPACT: SizeSpec = {
    charW: 16, lineH: 28, maxChars: 18, minW: 300, maxW: 520, padX: 38, padTop: 16, padBottom: 16, extraH: 0,
};

function clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
}

export function displayNodeName(name: string, maxLen = 26): string {
    if (name.length <= maxLen) return name;
    return `${name.slice(0, maxLen - 1)}…`;
}

/** Split a snake_case name into wrap-friendly tokens, keeping the underscore at the break. */
function splitTokens(name: string): string[] {
    const parts = name.split("_");
    return parts
        .map((part, index) => (index < parts.length - 1 ? `${part}_` : part))
        .filter((token) => token.length > 0);
}

export type WrappedName = { lines: string[]; maxLen: number; truncated: boolean };

/** Wrap the full name across lines at token boundaries so it stays readable without truncation. */
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
    const h = spec.padTop + lines.length * spec.lineH + extraLines * spec.lineH + spec.extraH + spec.padBottom;
    return { w, h, lines };
}

export function stepNodeSize(name: string, compact: boolean): NodeSize {
    return sizeFor(name, compact ? STEP_COMPACT : STEP_NORMAL);
}

export function tableNodeSize(name: string, indexes: string[] = [], compact: boolean): NodeSize {
    const trimmed = name.slice(0, TABLE_NAME_MAX_LEN);
    const hasIndexes = !compact && indexes.length > 0;
    return sizeFor(trimmed, compact ? TABLE_COMPACT : TABLE_NORMAL, hasIndexes ? 1 : 0);
}

export const GROUP_MIN_WIDTH = 300;
export const GROUP_MAX_WIDTH = 560;
export const GROUP_MIN_HEIGHT = 150;
export const GROUP_MAX_HEIGHT = 380;

/** Collapsed pipeline-step group — fits both the step-count hint and the full (wrapped) name. */
export function groupBoxSize(name: string, childCount: number): NodeSize {
    const { lines, maxLen } = wrapName(name, 26, 3);
    const count = Math.max(1, childCount);
    const w = clamp(Math.max(260 + count * 30, maxLen * 13 + 48), GROUP_MIN_WIDTH, GROUP_MAX_WIDTH);
    const h = clamp(Math.max(120 + count * 16, lines.length * 28 + 70), GROUP_MIN_HEIGHT, GROUP_MAX_HEIGHT);
    return { w, h, lines };
}
