import type { UiPlugin } from "@datapipe/ui/plugins/registry";
import { mlOpsRouteElements } from "./routes";
import { MlPluginSection } from "./plugins/PluginSection";
import { renderMlNavSections } from "./nav";

export const mlUiPlugin: UiPlugin = {
    id: "datapipe-ui-ml",
    routes: () => mlOpsRouteElements(),
    renderNavSections: renderMlNavSections,
    renderLegacyNavItems: ({ hasExplicitSpecs, capabilities }) => {
        if (hasExplicitSpecs || !capabilities?.ml_metrics) return [];
        return [
            { key: "/training", href: "/training", label: "Training" },
            { key: "/metrics", href: "/metrics", label: "Metrics" },
            { key: "/classes", href: "/classes", label: "Class Metrics" },
        ];
    },
    PluginSection: MlPluginSection,
    obsPagePrefixes: ["/image", "/frozen-datasets", "/metrics", "/classes", "/class-metrics", "/training"],
};

export { mlOpsRouteElements, MlOpsRoutes } from "./routes";
export { mlOpsApi } from "./api/mlOps";
