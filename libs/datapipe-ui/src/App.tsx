import React from "react";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { OpsShell } from "./layouts/OpsShell";
import { Overview } from "./features/ops/Overview";
import { Help } from "./features/ops/Help";
import { GraphPage } from "./features/ops/GraphPage";
import { TableDetail } from "./features/ops/TableDetail";
import { TransformDetail } from "./features/ops/TransformDetail";
import { MetaStepDetail } from "./features/ops/MetaStepDetail";
import { Settings } from "./features/ops/Settings";
import { renderPluginRoutes } from "./plugins/registry";
import "./App.css";
import "./operatorLight.css";
import "./opsPages.css";
import "antd/dist/antd.css";

function LegacyDebugRedirect() {
    const { search } = useLocation();
    return <Navigate to={`/graph${search}`} replace />;
}

function App() {
    return (
        <BrowserRouter>
            <Routes>
                <Route element={<OpsShell />}>
                    <Route path="/" element={<Overview />} />
                    {renderPluginRoutes()}
                    <Route path="/graph" element={<GraphPage />} />
                    <Route path="/debug" element={<LegacyDebugRedirect />} />
                    <Route path="/help" element={<Help />} />
                    <Route path="/settings" element={<Settings />} />
                    <Route path="/tables/:tableName" element={<TableDetail />} />
                    <Route path="/transforms/:transformName" element={<TransformDetail />} />
                    <Route path="/meta-steps/:stepName" element={<MetaStepDetail />} />
                    {/* Legacy pipeline-scoped URLs from older UI builds */}
                    <Route path="/pipelines/:id" element={<Navigate to="/" replace />} />
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
