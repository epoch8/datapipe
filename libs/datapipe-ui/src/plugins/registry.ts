import React from "react";
import type { Capabilities } from "../types/ops";
import type { OpsSpecSummary } from "@datapipe/ui-ml/types/opsSpecs";

export type UiMlPlugin = {
    id: string;
    routes: () => React.ReactNode;
    renderNavSections: (ctx: {
        specs: OpsSpecSummary[];
        collapsed: boolean;
        pathname: string;
        capabilities: Capabilities | null;
    }) => React.ReactNode;
    renderLegacyNavItems: (ctx: {
        hasExplicitSpecs: boolean;
        capabilities: Capabilities | null;
    }) => Array<{ key: string; href: string; label: string }>;
    PluginSection: React.ComponentType<{ enrichments?: import("../types/ops").Enrichment[] }>;
    obsPagePrefixes: string[];
};

const emptyPlugin: UiMlPlugin = {
    id: "ml-stub",
    routes: () => null,
    renderNavSections: () => null,
    renderLegacyNavItems: () => [],
    PluginSection: () => null,
    obsPagePrefixes: [],
};

let activePlugin: UiMlPlugin = emptyPlugin;

export function registerUiMlPlugin(plugin: UiMlPlugin): void {
    activePlugin = plugin;
}

export function getUiMlPlugin(): UiMlPlugin {
    return activePlugin;
}
