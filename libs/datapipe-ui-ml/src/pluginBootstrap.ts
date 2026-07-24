import { registerUiPlugin } from "@datapipe/ui/plugins/registry";
import { EntityLink } from "./features/ops/shared/EntityLink";
import { mlOpsApi } from "./api/mlOps";
import { mlUiPlugin } from "./plugin";

registerUiPlugin({
    ...mlUiPlugin,
    opsApiExtensions: mlOpsApi,
    EntityLink,
});
