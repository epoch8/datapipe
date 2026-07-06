import { ApiError, toApiError } from "./http";

describe("toApiError", () => {
    const originalNavigator = globalThis.navigator;

    afterEach(() => {
        Object.defineProperty(globalThis, "navigator", {
            configurable: true,
            value: originalNavigator,
        });
    });

    it("maps Failed to fetch to a network ApiError", () => {
        Object.defineProperty(globalThis, "navigator", {
            configurable: true,
            value: { onLine: true },
        });

        const error = toApiError(new TypeError("Failed to fetch"), "/api/v1alpha3/overview");

        expect(error).toBeInstanceOf(ApiError);
        expect(error.kind).toBe("network");
        expect(error.message).toContain("Cannot reach the Datapipe API");
        expect(String(error)).toBe(error.message);
    });

    it("maps offline navigator to an offline ApiError", () => {
        const previous = Object.getOwnPropertyDescriptor(globalThis.navigator, "onLine");
        Object.defineProperty(globalThis.navigator, "onLine", {
            configurable: true,
            value: false,
        });

        const error = toApiError(new TypeError("Failed to fetch"));

        if (previous) {
            Object.defineProperty(globalThis.navigator, "onLine", previous);
        }

        expect(error.kind).toBe("offline");
        expect(error.message).toContain("offline");
    });
});
