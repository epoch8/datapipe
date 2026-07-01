import React from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { OpsShell } from "./layouts/OpsShell";
import { Overview } from "./features/ops/Overview";
import { Metrics } from "./features/ops/Metrics";
import { PipelineDetail } from "./features/ops/PipelineDetail";
import { RunDetail } from "./features/ops/RunDetail";
import { TrainingDetail } from "./features/ops/TrainingDetail";
import { TrainingRuns } from "./features/ops/TrainingRuns";
import { TrainingCompare } from "./features/ops/TrainingCompare";
import { Help } from "./features/ops/Help";
import { Settings } from "./features/ops/Settings";
import { DebugPage } from "./features/ops/DebugPage";
import { TableDetail } from "./features/ops/TableDetail";
import { TransformDetail } from "./features/ops/TransformDetail";
import { MetaStepDetail } from "./features/ops/MetaStepDetail";
import "./App.css";
import "./operatorLight.css";
import "antd/dist/antd.css";

function App() {
    return (
        <BrowserRouter>
            <Routes>
                <Route element={<OpsShell />}>
                    <Route path="/" element={<Overview />} />
                    <Route path="/metrics" element={<Metrics />} />
                    <Route path="/debug" element={<DebugPage />} />
                    <Route path="/help" element={<Help />} />
                    <Route path="/settings" element={<Settings />} />
                    <Route path="/pipelines/:id" element={<PipelineDetail />} />
                    <Route path="/pipelines/:id/tables/:tableName" element={<TableDetail />} />
                    <Route path="/pipelines/:id/transforms/:transformName" element={<TransformDetail />} />
                    <Route path="/pipelines/:id/meta-steps/:stepName" element={<MetaStepDetail />} />
                    <Route path="/pipelines/:id/training" element={<TrainingRuns />} />
                    <Route path="/training/:runKey" element={<TrainingDetail />} />
                    <Route path="/training/compare" element={<TrainingCompare />} />
                    <Route path="/runs/:runId" element={<RunDetail />} />
                </Route>
                <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
        </BrowserRouter>
    );
}

export default App;
