import { coreOpsApi } from "./ops";
import { mlOpsApi } from "@datapipe/ui-ml/api/mlOps";

export const opsApi = { ...coreOpsApi, ...mlOpsApi };

export { getRefreshIntervalMs, exportCsv } from "./ops";
