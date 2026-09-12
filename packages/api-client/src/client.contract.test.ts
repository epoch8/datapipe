/**
 * Lightweight contract checks for PipelineApiClient capability defaults
 * and LocalPipelineApiClient construction (no live server required).
 */
import { createLocalPipelineApiClient } from "./client";

describe("@datapipe/api-client contract", () => {
    it("creates a local client with custom apiBase", () => {
        const client = createLocalPipelineApiClient({ apiBase: "/api/v1alpha3/" });
        expect(typeof client.getCapabilities).toBe("function");
        expect(typeof client.getRuns).toBe("function");
        expect(typeof client.startRun).toBe("function");
        expect(typeof client.createWsUrl).toBe("function");
        const ws = client.createWsUrl("/ws/transform/x/run-status");
        expect(ws).toContain("/ws/transform/x/run-status");
    });

    it("exposes createWsUrl that does not require window for absolute paths", () => {
        const client = createLocalPipelineApiClient({
            apiBase: "/api/v1alpha3",
            createWsUrl: (path) => `ws://example.test${path}`,
        });
        expect(client.createWsUrl("/ws/x")).toBe("ws://example.test/ws/x");
    });
});
