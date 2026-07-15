import React from "react";
import type { Capabilities } from "@datapipe/ui/types/ops";
import type { OpsSpecSummary } from "./types/opsSpecs";
import { mlOpsRouteElements } from "./routes";
import { MlPluginSection } from "./plugins/PluginSection";
import { renderMlNavSections } from "./nav";

export type UiMlPlugin = {
    id: string;
    routes: () => React.ReactNode;
    renderNavSections: (ctx: {
        specs: OpsSpecSummary[];
        collapsed: boolean;
        pathname: string;
        capabilities: Capabilities | null;
    }) => React.ReactNode;
    renderLegacyNavItems: (ctx: { hasExplicitSpecs: boolean; capabilities: Capabilities | null }) => Array<{
        key: string;
        href: string;
        label: string;
    }>;
    PluginSection: typeof MlPluginSection;
    obsPagePrefixes: string[];
};

export const mlUiPlugin: UiMlPlugin = {
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
