import React from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { Overview } from "../features/ops/Overview";
import { RunsPage } from "../features/ops/runs/RunsPage";
import { RunDetail } from "../features/ops/RunDetail";
import { Help } from "../features/ops/Help";
import { GraphPage } from "../features/ops/GraphPage";
import { TableDetail } from "../features/ops/TableDetail";
import { TransformDetail } from "../features/ops/TransformDetail";
import { MetaStepDetail } from "../features/ops/MetaStepDetail";
import { renderPluginRoutes } from "../plugins/registry";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { DatapipeUiProvider } from "../context/DatapipeUiContext";
import { createOpsApi, setActiveOpsApi } from "../api/ops";
import type { Capabilities } from "../types/ops";
import "../App.css";
import "../operatorLight.css";
import "../opsPages.css";
import "../themeDark.css";
import "../cloud/cloudIsland.css";
import "antd/dist/antd.css";
import "@datapipe/ui-core/tokens.css";

export type CloudMountConfig = {
    mode: "cloud" | "local";
    pipelineName: string;
    apiBase: string;
    basename: string;
    section?: string;
    runId?: string;
    theme?: string;
};

const defaultCapabilities: Capabilities = {
    graph: true,
    table_data: true,
    table_meta: true,
    transform_meta: true,
    run_history: true,
    run_start: true,
    run_stop: true,
    run_logs: true,
    transform_run: true,
    transform_reset: true,
    addons: [],
};

function linkTo(path: string): string {
    const cleaned = path.replace(/^\//, "");
    return cleaned ? `/${cleaned}` : "/";
}

/** Configure Ops API base before mounting {@link CloudPipelineApp}. */
export function createOpsApiFromBase(apiBase: string) {
    const api = createOpsApi({ apiBase: apiBase.replace(/\/$/, "") });
    setActiveOpsApi(api);
    return api;
}

export function readCloudMountConfig(): CloudMountConfig | null {
    const raw = (window as unknown as { __DP_PIPELINE__?: CloudMountConfig }).__DP_PIPELINE__;
    if (!raw || raw.mode !== "cloud") return null;
    return raw;
}

/**
 * CloudShell island — Django owns chrome/tabs/agent selector; this mounts shared
 * pipeline screens from the same source as LocalShell.
 */
export function CloudPipelineApp({ config }: { config: CloudMountConfig }) {
    const basename = config.basename.replace(/\/$/, "") || "/";
    const api = React.useMemo(
        () => createOpsApi({ apiBase: config.apiBase.replace(/\/$/, "") }),
        [config.apiBase],
    );
    const [capabilities, setCapabilities] = React.useState<Capabilities>(defaultCapabilities);
    const [theme, setTheme] = React.useState<"light" | "dark">(() => {
        if (typeof document === "undefined") {
            return config.theme === "light" ? "light" : "dark";
        }
        const live =
            document.documentElement.getAttribute("data-ui-theme") ||
            document.documentElement.getAttribute("data-bs-theme") ||
            config.theme;
        return live === "light" ? "light" : "dark";
    });

    React.useEffect(() => {
        setActiveOpsApi(api);
        api.getCapabilities().then(setCapabilities).catch(() => undefined);
    }, [api]);

    React.useEffect(() => {
        const syncFromHost = () => {
            const live =
                document.documentElement.getAttribute("data-ui-theme") ||
                document.documentElement.getAttribute("data-bs-theme") ||
                "dark";
            const next = live === "light" ? "light" : "dark";
            setTheme(next);
            const mount = document.getElementById("datapipe-ui-root");
            if (mount) {
                mount.setAttribute("data-ui-theme", next);
                mount.setAttribute("data-theme", next);
                mount.classList.add("datapipe-cloud-island");
            }
        };
        syncFromHost();
        const observer = new MutationObserver(syncFromHost);
        observer.observe(document.documentElement, {
            attributes: true,
            attributeFilter: ["data-ui-theme", "data-bs-theme", "data-theme"],
        });
        window.addEventListener("dp-ui-theme", syncFromHost);
        return () => {
            observer.disconnect();
            window.removeEventListener("dp-ui-theme", syncFromHost);
        };
    }, []);

    return (
        <DatapipeUiProvider
            value={{
                apiClient: api,
                pipelineName: config.pipelineName || capabilities.pipeline_id || "pipeline",
                mode: "cloud",
                capabilities,
                theme,
                linkTo,
                createWsUrl: api.createWsUrl,
            }}
        >
            <BrowserRouter basename={basename}>
                <div
                    className="datapipe-main datapipe-cloud-island"
                    data-ui-theme={theme}
                    data-theme={theme}
                    style={{
                        flex: 1,
                        minWidth: 0,
                        minHeight: 0,
                        height: "100%",
                        display: "flex",
                        flexDirection: "column",
                        overflow: "hidden",
                    }}
                >
                    <ErrorBoundary>
                        <Routes>
                            <Route path="/" element={<Overview />} />
                            <Route path="/overview" element={<Navigate to="/" replace />} />
                            <Route path="/runs" element={<RunsPage />} />
                            <Route path="/runs/:runId" element={<RunDetail />} />
                            {renderPluginRoutes()}
                            <Route path="/graph" element={<GraphPage />} />
                            <Route path="/help" element={<Help />} />
                            <Route path="/tables/:tableName" element={<TableDetail />} />
                            <Route path="/transforms/:transformName" element={<TransformDetail />} />
                            <Route path="/meta-steps/:stepName" element={<MetaStepDetail />} />
                            <Route path="*" element={<Navigate to="/" replace />} />
                        </Routes>
                    </ErrorBoundary>
                </div>
            </BrowserRouter>
        </DatapipeUiProvider>
    );
}
