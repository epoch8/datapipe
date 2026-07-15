import React from "react";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { OpsShell } from "./layouts/OpsShell";
import { Overview } from "./features/ops/Overview";
import { RunsPage } from "./features/ops/runs/RunsPage";
import { PipelineDetail } from "./features/ops/PipelineDetail";
import { RunDetail } from "./features/ops/RunDetail";
import { Help } from "./features/ops/Help";
import { GraphPage } from "./features/ops/GraphPage";
import { TableDetail } from "./features/ops/TableDetail";
import { TransformDetail } from "./features/ops/TransformDetail";
import { MetaStepDetail } from "./features/ops/MetaStepDetail";
import { registerUiMlPlugin, getUiMlPlugin } from "./plugins/registry";
import { mlUiPlugin } from "@datapipe/ui-ml";
import "./App.css";
import "./operatorLight.css";
import "./opsPages.css";
import "antd/dist/antd.css";

registerUiMlPlugin(mlUiPlugin);

function LegacyDebugRedirect() {
    const { search } = useLocation();
    return <Navigate to={`/graph${search}`} replace />;
}

function App() {
    const mlPlugin = getUiMlPlugin();
    return (
        <BrowserRouter>
            <Routes>
                <Route element={<OpsShell />}>
                    <Route path="/" element={<Overview />} />
                    <Route path="/runs" element={<RunsPage />} />
                    <Route path="/runs/:runId" element={<RunDetail />} />
                    {mlPlugin.routes()}
                    <Route path="/graph" element={<GraphPage />} />
                    <Route path="/debug" element={<LegacyDebugRedirect />} />
                    <Route path="/help" element={<Help />} />
                    <Route path="/pipelines/:id" element={<PipelineDetail />} />
                    <Route path="/pipelines/:id/tables/:tableName" element={<TableDetail />} />
                    <Route path="/pipelines/:id/transforms/:transformName" element={<TransformDetail />} />
                    <Route path="/pipelines/:id/meta-steps/:stepName" element={<MetaStepDetail />} />
                </Route>
                <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
