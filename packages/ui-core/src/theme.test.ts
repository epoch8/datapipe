/**
 * Theme helper smoke tests for @datapipe/ui-core (jsdom).
 */
import { applyUiTheme, readStoredUiTheme } from "./index";

describe("@datapipe/ui-core theme", () => {
    beforeEach(() => {
        localStorage.clear();
        document.documentElement.removeAttribute("data-ui-theme");
    });

    it("applies light and dark themes to documentElement", () => {
        applyUiTheme("light");
        expect(document.documentElement.getAttribute("data-ui-theme")).toBe("light");
        applyUiTheme("dark");
        expect(document.documentElement.getAttribute("data-ui-theme")).toBe("dark");
        expect(localStorage.getItem("ui-theme")).toBe("dark");
    });

    it("reads stored theme with fallback", () => {
        expect(readStoredUiTheme("light")).toBe("light");
        localStorage.setItem("ui-theme", "dark");
        expect(readStoredUiTheme("light")).toBe("dark");
    });
});
