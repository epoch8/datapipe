import { coreOpsApi, setActiveOpsApi } from "./ops";
import { mergeOpsApiExtensions } from "../plugins/registry";
import type { PipelineApiClient } from "../context/types";

function buildOpsApi(): PipelineApiClient {
    return { ...coreOpsApi, ...mergeOpsApiExtensions() };
}

/**
 * Live Ops API facade. Always reads the current {@link coreOpsApi}
 * (updated via {@link setActiveOpsApi}) so hosts can inject adapters.
 */
export const opsApi: PipelineApiClient = new Proxy({} as PipelineApiClient, {
    get(_target, prop, _receiver) {
        const live = buildOpsApi() as unknown as Record<string | symbol, unknown>;
        const value = live[prop];
        return typeof value === "function" ? value.bind(live) : value;
    },
});

export type OpsApi = PipelineApiClient;

export { getRefreshIntervalMs, exportCsv, setActiveOpsApi } from "./ops";
