/** Library surface for cloud / hosts that mount datapipe-ui screens. */

export { DatapipeUiProvider, useDatapipeUiConfig, usePipelineApi, useDatapipeLinkTo } from "../context/DatapipeUiContext";
export type { DatapipeUiConfig, DatapipeUiMode, Permissions } from "../context/DatapipeUiContext";
export { Overview } from "../features/ops/Overview";
export { GraphPage } from "../features/ops/GraphPage";
export { RunsPage } from "../features/ops/runs/RunsPage";
export { RunDetail } from "../features/ops/RunDetail";
export { TableDetail } from "../features/ops/TableDetail";
export { TransformDetail } from "../features/ops/TransformDetail";
export { MetaStepDetail } from "../features/ops/MetaStepDetail";
export { Help } from "../features/ops/Help";
export { createOpsApi, setActiveOpsApi, coreOpsApi } from "../api/ops";
export { LocalShell, bootstrapLocalTheme } from "../local/LocalShell";
export {
    CloudPipelineApp,
    createOpsApiFromBase,
    readCloudMountConfig,
} from "../cloud/CloudPipelineApp";
export type { CloudMountConfig } from "../cloud/CloudPipelineApp";
