import { coreOpsApi, exportCsv, getRefreshIntervalMs } from "@datapipe/ui/api/ops";
import { mlOpsApi } from "./mlOps";

export const opsApi = { ...coreOpsApi, ...mlOpsApi };

export type OpsApi = typeof opsApi;

export { getRefreshIntervalMs, exportCsv };
