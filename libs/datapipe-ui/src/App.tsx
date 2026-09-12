import React from "react";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { LocalShell, bootstrapLocalTheme } from "./local/LocalShell";
import { Overview } from "./features/ops/Overview";
import { RunsPage } from "./features/ops/runs/RunsPage";
import { RunDetail } from "./features/ops/RunDetail";
import { Help } from "./features/ops/Help";
import { GraphPage } from "./features/ops/GraphPage";
import { TableDetail } from "./features/ops/TableDetail";
import { TransformDetail } from "./features/ops/TransformDetail";
import { MetaStepDetail } from "./features/ops/MetaStepDetail";
import { Settings } from "./features/ops/Settings";
import { renderPluginRoutes } from "./plugins/registry";
import { DatapipeUiProvider } from "./context/DatapipeUiContext";
import { createOpsApi, setActiveOpsApi } from "./api/ops";
import type { Capabilities } from "./types/ops";
import "./App.css";
import "./operatorLight.css";
import "./opsPages.css";
import "./themeDark.css";
import "./local/localShell.css";
import "antd/dist/antd.css";
import "@datapipe/ui-core/tokens.css";

function LegacyDebugRedirect() {
    const { search } = useLocation();
    return <Navigate to={`/graph${search}`} replace />;
}

function linkTo(path: string): string {
    const cleaned = path.replace(/^\//, "");
    return cleaned ? `/${cleaned}` : "/";
}

const localApi = createOpsApi({ apiBase: "/api/v1alpha3" });
setActiveOpsApi(localApi);

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

/** Standalone LocalShell application entry. */
function App() {
    const theme = bootstrapLocalTheme();
    const [capabilities, setCapabilities] = React.useState<Capabilities>(defaultCapabilities);

    React.useEffect(() => {
        localApi.getCapabilities().then(setCapabilities).catch(() => undefined);
    }, []);

    return (
        <DatapipeUiProvider
            value={{
                apiClient: localApi,
                pipelineName: capabilities.pipeline_id || "app:app",
                mode: "local",
                capabilities,
                theme,
                linkTo,
                createWsUrl: localApi.createWsUrl,
            }}
        >
            <BrowserRouter>
                <Routes>
                    <Route element={<LocalShell />}>
                        <Route path="/" element={<Overview />} />
                        <Route path="/runs" element={<RunsPage />} />
                        <Route path="/runs/:runId" element={<RunDetail />} />
                        {renderPluginRoutes()}
                        <Route path="/graph" element={<GraphPage />} />
                        <Route path="/debug" element={<LegacyDebugRedirect />} />
                        <Route path="/help" element={<Help />} />
                        <Route path="/settings" element={<Settings />} />
                        <Route path="/tables/:tableName" element={<TableDetail />} />
                        <Route path="/transforms/:transformName" element={<TransformDetail />} />
                        <Route path="/meta-steps/:stepName" element={<MetaStepDetail />} />
                        <Route path="/pipelines/:id" element={<Navigate to="/" replace />} />
                        <Route path="/pipelines/:id/tables/:tableName" element={<TableDetail />} />
                        <Route
                            path="/pipelines/:id/transforms/:transformName"
                            element={<TransformDetail />}
                        />
                        <Route path="/pipelines/:id/meta-steps/:stepName" element={<MetaStepDetail />} />
                    </Route>
                    <Route path="*" element={<Navigate to="/" replace />} />
                </Routes>
            </BrowserRouter>
        </DatapipeUiProvider>
    );
}

export default App;
