import {
    DEFAULT_LABEL_KEY,
    graphHref,
    labelKeyFromRunLabels,
    normalizeLabelKey,
} from "./labelKey";

describe("labelKey helpers", () => {
    test("normalizeLabelKey defaults to first available key", () => {
        expect(normalizeLabelKey(null, ["stage", "flow"])).toBe("stage");
        expect(normalizeLabelKey(null, ["flow", "source"])).toBe("flow");
        expect(normalizeLabelKey("flow", ["stage", "flow"])).toBe("flow");
        expect(normalizeLabelKey("missing", ["flow", "source"])).toBe("flow");
    });

    test("graphHref keeps stage param and optional label_key", () => {
        expect(graphHref("extract")).toBe("/graph?stage=extract");
        expect(graphHref("regular", "flow")).toBe("/graph?label_key=flow&stage=regular");
        expect(graphHref("API", DEFAULT_LABEL_KEY)).toBe("/graph?stage=API");
    });

    test("labelKeyFromRunLabels prefers stage then first key", () => {
        expect(labelKeyFromRunLabels([["flow", "regular"], ["stage", "extract"]])).toBe(
            "stage",
        );
        expect(labelKeyFromRunLabels([["flow", "regular"]])).toBe("flow");
        expect(labelKeyFromRunLabels([])).toBe("stage");
    });
});
