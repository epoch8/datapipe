/** Strip CSI / OSC escape sequences (ANSI colors, cursor moves, etc.). */
const ANSI_RE =
    // eslint-disable-next-line no-control-regex
    /[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><~]/g;

const FG: Record<number, string> = {
    30: "#000000",
    31: "#cd3131",
    32: "#0dbc79",
    33: "#e5e510",
    34: "#2472c8",
    35: "#bc3fbc",
    36: "#11a8cd",
    37: "#e5e5e5",
    90: "#666666",
    91: "#f14c4c",
    92: "#23d18b",
    93: "#f5f543",
    94: "#3b8eea",
    95: "#d670d6",
    96: "#29b8db",
    97: "#e5e5e5",
};

const BG: Record<number, string> = {
    40: "#000000",
    41: "#cd3131",
    42: "#0dbc79",
    43: "#e5e510",
    44: "#2472c8",
    45: "#bc3fbc",
    46: "#11a8cd",
    47: "#e5e5e5",
    100: "#666666",
    101: "#f14c4c",
    102: "#23d18b",
    103: "#f5f543",
    104: "#3b8eea",
    105: "#d670d6",
    106: "#29b8db",
    107: "#e5e5e5",
};

type Style = {
    color?: string;
    background?: string;
    bold?: boolean;
    dim?: boolean;
    italic?: boolean;
    underline?: boolean;
};

function escapeHtml(text: string): string {
    return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function styleToCss(style: Style): string {
    const parts: string[] = [];
    if (style.color) parts.push(`color:${style.color}`);
    if (style.background) parts.push(`background-color:${style.background}`);
    if (style.bold) parts.push("font-weight:700");
    if (style.dim) parts.push("opacity:0.7");
    if (style.italic) parts.push("font-style:italic");
    if (style.underline) parts.push("text-decoration:underline");
    return parts.join(";");
}

function applySgr(style: Style, codes: number[]): Style {
    const next = { ...style };
    let i = 0;
    while (i < codes.length) {
        const code = codes[i];
        if (code === 0) {
            return {};
        }
        if (code === 1) next.bold = true;
        else if (code === 2) next.dim = true;
        else if (code === 3) next.italic = true;
        else if (code === 4) next.underline = true;
        else if (code === 22) {
            next.bold = false;
            next.dim = false;
        } else if (code === 23) next.italic = false;
        else if (code === 24) next.underline = false;
        else if (code === 39) delete next.color;
        else if (code === 49) delete next.background;
        else if (FG[code]) next.color = FG[code];
        else if (BG[code]) next.background = BG[code];
        else if (code === 38 || code === 48) {
            const isFg = code === 38;
            const mode = codes[i + 1];
            if (mode === 5 && codes[i + 2] != null) {
                const color = xterm256(codes[i + 2]);
                if (isFg) next.color = color;
                else next.background = color;
                i += 2;
            } else if (mode === 2 && codes[i + 4] != null) {
                const color = `rgb(${codes[i + 2]},${codes[i + 3]},${codes[i + 4]})`;
                if (isFg) next.color = color;
                else next.background = color;
                i += 4;
            }
        }
        i += 1;
    }
    return next;
}

/** Map xterm 256-color index to CSS rgb (approximation of standard palette). */
function xterm256(n: number): string {
    if (n < 0) return "#000000";
    if (n < 16) {
        const basic = [
            "#000000",
            "#cd3131",
            "#0dbc79",
            "#e5e510",
            "#2472c8",
            "#bc3fbc",
            "#11a8cd",
            "#e5e5e5",
            "#666666",
            "#f14c4c",
            "#23d18b",
            "#f5f543",
            "#3b8eea",
            "#d670d6",
            "#29b8db",
            "#e5e5e5",
        ];
        return basic[n] ?? "#000000";
    }
    if (n < 232) {
        const idx = n - 16;
        const r = Math.floor(idx / 36);
        const g = Math.floor((idx % 36) / 6);
        const b = idx % 6;
        const levels = [0, 95, 135, 175, 215, 255];
        return `rgb(${levels[r]},${levels[g]},${levels[b]})`;
    }
    const gray = 8 + (n - 232) * 10;
    return `rgb(${gray},${gray},${gray})`;
}

export function stripAnsi(text: string): string {
    return text.replace(ANSI_RE, "");
}

/**
 * Convert ANSI-colored text to safe HTML spans for a dark terminal panel.
 * Non-SGR CSI sequences are dropped; text is HTML-escaped.
 */
export function ansiToHtml(text: string): string {
    if (!text) return "";
    // eslint-disable-next-line no-control-regex
    const tokenRe = /\u001b\[([0-9;]*)m|\u001b\[[0-9;?]*[A-Za-z]|\u001b\][^\u0007]*(?:\u0007|\u001b\\)/g;

    let html = "";
    let style: Style = {};
    let last = 0;
    let match: RegExpExecArray | null;

    const pushText = (chunk: string) => {
        if (!chunk) return;
        const escaped = escapeHtml(chunk);
        const css = styleToCss(style);
        html += css ? `<span style="${css}">${escaped}</span>` : escaped;
    };

    while ((match = tokenRe.exec(text)) !== null) {
        pushText(text.slice(last, match.index));
        last = match.index + match[0].length;
        if (match[1] != null) {
            const codes =
                match[1] === ""
                    ? [0]
                    : match[1].split(";").map((part) => Number(part) || 0);
            style = applySgr(style, codes);
        }
        // non-SGR escapes: drop
    }
    pushText(text.slice(last));
    return html;
}

export function hasAnsi(text: string): boolean {
    return ANSI_RE.test(text);
}
