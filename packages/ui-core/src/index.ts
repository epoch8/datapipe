/**
 * @datapipe/ui-core — shared design tokens and chrome helpers.
 *
 * CSS is not bundled by this module. Import once in the host app:
 *
 *   import "@datapipe/ui-core/tokens.css";
 *
 * Then toggle theme with `data-theme` / `data-ui-theme` = `"light"` | `"dark"`.
 */

export const UI_CORE_TOKENS_PATH = "@datapipe/ui-core/tokens.css";

export type ThemeMode = "light" | "dark";
/** @deprecated Prefer ThemeMode */
export type UiTheme = ThemeMode;

/** Apply theme attributes on a root element (defaults to documentElement). */
export function setThemeMode(mode: ThemeMode, root?: HTMLElement): void {
    const el = root ?? (typeof document !== "undefined" ? document.documentElement : undefined);
    if (!el) return;
    el.setAttribute("data-theme", mode);
    el.setAttribute("data-ui-theme", mode);
    el.setAttribute("data-bs-theme", mode);
}

export function applyUiTheme(theme: ThemeMode, root?: HTMLElement): void {
    setThemeMode(theme, root);
    try {
        localStorage.setItem("ui-theme", theme);
    } catch {
        // ignore
    }
}

export function readStoredUiTheme(fallback: ThemeMode = "light"): ThemeMode {
    try {
        const stored = localStorage.getItem("ui-theme");
        if (stored === "light" || stored === "dark") return stored;
    } catch {
        // ignore
    }
    return fallback;
}
