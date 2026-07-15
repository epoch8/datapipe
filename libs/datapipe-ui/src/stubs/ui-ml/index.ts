import type { UiMlPlugin } from "../../plugins/registry";

export const mlUiPlugin: UiMlPlugin = {
    id: "ml-stub",
    routes: () => null,
    renderNavSections: () => null,
    renderLegacyNavItems: () => [],
    PluginSection: () => null,
    obsPagePrefixes: [],
};
