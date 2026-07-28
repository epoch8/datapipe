import { normalizeMetricKey, readMetricNumber } from "./metricsFormat";

describe("readMetricNumber aliases", () => {
    test("resolves Ops short ids to canonical Detailed keys", () => {
        const metrics = {
            weighted_f1: 0.64,
            macro_f1: 0.63,
            weighted_precision: 0.59,
            accuracy: 0.46,
        };
        expect(readMetricNumber(metrics, "weighted_f1_score")).toBe(0.64);
        expect(readMetricNumber(metrics, "macro_f1_score")).toBe(0.63);
        expect(readMetricNumber(metrics, "weighted_precision")).toBe(0.59);
        expect(readMetricNumber(metrics, "accuracy")).toBe(0.46);
    });

    test("normalizeMetricKey maps short f1 ids", () => {
        expect(normalizeMetricKey("weighted_f1")).toBe("weighted_f1_score");
        expect(normalizeMetricKey("calc__weighted_f1_score")).toBe("weighted_f1_score");
    });
});
