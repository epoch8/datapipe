import React from "react";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { OpsShell } from "./layouts/OpsShell";
import { Overview } from "./features/ops/Overview";
import { RunsPage } from "./features/ops/runs/RunsPage";
import { PipelineDetail } from "./features/ops/PipelineDetail";
import { RunDetail } from "./features/ops/RunDetail";
import { TrainingDetail } from "./features/ops/TrainingDetail";
import { TrainingCompare } from "./features/ops/TrainingCompare";
import { Help } from "./features/ops/Help";
import { GraphPage } from "./features/ops/GraphPage";
import { TableDetail } from "./features/ops/TableDetail";
import { TransformDetail } from "./features/ops/TransformDetail";
import { MetaStepDetail } from "./features/ops/MetaStepDetail";
import { Metrics } from "./features/ops/Metrics";
import { TrainingRuns } from "./features/ops/TrainingRuns";
import { MetricsOverviewPage } from "./features/ops/metrics/MetricsOverviewPage";
import { ClassMetricsPage } from "./features/ops/classes/ClassMetricsPage";
import { TrainingRunsPage } from "./features/ops/training/TrainingRunsPage";
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
                    <Route path="/runs" element={<RunsPage />} />
                    <Route path="/runs/:runId" element={<RunDetail />} />
                    <Route path="/metrics" element={<MetricsOverviewPage />} />
                    <Route path="/pipelines/:id/metrics" element={<MetricsOverviewPage />} />
                    <Route path="/classes" element={<ClassMetricsPage />} />
                    <Route path="/pipelines/:id/classes" element={<ClassMetricsPage />} />
                    <Route path="/training" element={<TrainingRunsPage />} />
                    <Route path="/pipelines/:id/training" element={<TrainingRunsPage />} />
                    <Route path="/graph" element={<GraphPage />} />
                    <Route path="/debug" element={<LegacyDebugRedirect />} />
                    <Route path="/help" element={<Help />} />
                    <Route path="/pipelines/:id" element={<PipelineDetail />} />
                    <Route path="/pipelines/:id/tables/:tableName" element={<TableDetail />} />
                    <Route path="/pipelines/:id/transforms/:transformName" element={<TransformDetail />} />
                    <Route path="/pipelines/:id/meta-steps/:stepName" element={<MetaStepDetail />} />
                    <Route path="/training/:runKey" element={<TrainingDetail />} />
                    <Route path="/training/compare" element={<TrainingCompare />} />
                    {/* Legacy wrappers */}
                    <Route path="/metrics-legacy" element={<Metrics />} />
                    <Route path="/training-legacy/:id" element={<TrainingRuns />} />
                </Route>
                <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
