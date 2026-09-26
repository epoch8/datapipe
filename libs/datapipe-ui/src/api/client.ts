import { coreOpsApi } from "./ops";
import { mergeOpsApiExtensions } from "../plugins/registry";

export const opsApi = { ...coreOpsApi, ...mergeOpsApiExtensions() };

export type OpsApi = typeof coreOpsApi;

export { getRefreshIntervalMs, exportCsv } from "./ops";
