import React from "react";
import type { PipelineApiClient, Capabilities, UiTheme } from "./types";

export type DatapipeUiMode = "local" | "cloud";

export type Permissions = {
    canStartRun?: boolean;
    canStopRun?: boolean;
    canResetMetadata?: boolean;
    canRunTransform?: boolean;
};

export type DatapipeUiConfig = {
    apiClient: PipelineApiClient;
    pipelineName: string;
    mode: DatapipeUiMode;
    capabilities: Capabilities;
    permissions?: Permissions;
    theme: "light" | "dark";
    /** Build a host-relative path for in-app navigation (no leading slash required). */
    linkTo: (path: string) => string;
    createWsUrl?: (path: string) => string;
};

const DatapipeUiContext = React.createContext<DatapipeUiConfig | null>(null);

export function DatapipeUiProvider({
    value,
    children,
}: {
    value: DatapipeUiConfig;
    children: React.ReactNode;
}) {
    React.useEffect(() => {
        // CloudShell: Django owns <html> theme; only annotate the island mount.
        if (value.mode === "cloud") {
            const mount = document.getElementById("datapipe-ui-root");
            if (mount) {
                mount.setAttribute("data-ui-theme", value.theme);
                mount.setAttribute("data-theme", value.theme);
                mount.classList.add("datapipe-cloud-island");
            }
            return;
        }
        const root = document.documentElement;
        root.setAttribute("data-ui-theme", value.theme);
        root.setAttribute("data-theme", value.theme);
        root.setAttribute("data-bs-theme", value.theme);
    }, [value.theme, value.mode]);

    return <DatapipeUiContext.Provider value={value}>{children}</DatapipeUiContext.Provider>;
}

export function useDatapipeUiConfig(): DatapipeUiConfig {
    const ctx = React.useContext(DatapipeUiContext);
    if (!ctx) {
        throw new Error("useDatapipeUiConfig must be used within DatapipeUiProvider");
    }
    return ctx;
}

export function usePipelineApi(): PipelineApiClient {
    return useDatapipeUiConfig().apiClient;
}

export function useDatapipeLinkTo(): (path: string) => string {
    return useDatapipeUiConfig().linkTo;
}

export function useDatapipeCapabilities(): Capabilities {
    return useDatapipeUiConfig().capabilities;
}

/** Safe helper when provider may not yet be mounted (legacy call sites). */
export function useOptionalDatapipeUiConfig(): DatapipeUiConfig | null {
    return React.useContext(DatapipeUiContext);
}

// Re-export theme type name used by ui-core consumers without hard dependency.
export type { UiTheme };
