import React from "react";
import { Route } from "react-router-dom";
import { Metrics } from "./features/ops/metrics/legacyExports";
import { TrainingRuns } from "./features/ops/training/legacyExports";
import { TrainingDetail } from "./features/ops/training/TrainingDetailPage";
import { TrainingCompare } from "./features/ops/training/TrainingCompareRedirect";
import { MetricsOverviewPage } from "./features/ops/metrics/MetricsOverviewPage";
import { ModelDetailPage } from "./features/ops/models/ModelDetailPage";
import { FrozenDatasetDetailPage } from "./features/ops/frozen-datasets/FrozenDatasetDetailPage";
import { ImageOverviewPage } from "./features/ops/images/ImageOverviewPage";
import { ImageSpecPage } from "./features/ops/images/ImageSpecPage";
import { ImageRecordDetailPage } from "./features/ops/images/ImageRecordDetailPage";
import { FrozenDatasetRecordDetailPage } from "./features/ops/frozen-datasets/FrozenDatasetRecordDetailPage";
import { ModelPredictionRecordDetailPage } from "./features/ops/models/ModelPredictionRecordDetailPage";
import { ClassMetricsPage } from "./features/ops/metrics/ClassMetricsPage";
import { TrainingRunsPage } from "./features/ops/training/TrainingRunsPage";
import { OpsEntityDetailPage, OpsOverviewSpecPage, OpsSpecificSpecPage } from "./features/ops/specs/OpsSpecPages";

export function mlOpsRouteElements(): React.ReactNode {
    return (
        <>
            <Route path="/image" element={<ImageOverviewPage />} />
            <Route path="/image/:specId" element={<ImageSpecPage />} />
            <Route path="/image/:specId/records/:recordKey" element={<ImageRecordDetailPage />} />
            <Route path="/frozen-datasets" element={<OpsOverviewSpecPage kind="frozen-datasets" />} />
            <Route
                path="/frozen-datasets/:specId/datasets/:entityId/records/:recordKey"
                element={<FrozenDatasetRecordDetailPage />}
            />
            <Route path="/frozen-datasets/:specId/datasets/:entityId" element={<FrozenDatasetDetailPage />} />
            <Route path="/frozen-datasets/:specId" element={<OpsSpecificSpecPage kind="frozen-datasets" />} />
            <Route path="/training" element={<OpsOverviewSpecPage kind="training" />} />
            <Route path="/training/:specId/runs/:entityId" element={<OpsEntityDetailPage kind="training-run" />} />
            <Route path="/training/:specId" element={<OpsSpecificSpecPage kind="training" />} />
            <Route path="/metrics" element={<OpsOverviewSpecPage kind="metrics" />} />
            <Route
                path="/metrics/:specId/models/:entityId/predictions/:recordKey"
                element={<ModelPredictionRecordDetailPage />}
            />
            <Route path="/metrics/:specId/models/:entityId" element={<ModelDetailPage />} />
            <Route path="/metrics/:specId" element={<OpsSpecificSpecPage kind="metrics" />} />
            <Route path="/class-metrics" element={<OpsOverviewSpecPage kind="class-metrics" />} />
            <Route path="/class-metrics/:specId" element={<OpsSpecificSpecPage kind="class-metrics" />} />
            <Route path="/pipelines/:id/metrics" element={<MetricsOverviewPage />} />
            <Route path="/classes" element={<ClassMetricsPage />} />
            <Route path="/pipelines/:id/classes" element={<ClassMetricsPage />} />
            <Route path="/pipelines/:id/training" element={<TrainingRunsPage />} />
            <Route path="/training-runs/:runKey" element={<TrainingDetail />} />
            <Route path="/training/compare" element={<TrainingCompare />} />
            <Route path="/metrics-legacy" element={<Metrics />} />
            <Route path="/training-legacy/:id" element={<TrainingRuns />} />
        </>
    );
}

/** @deprecated Use mlOpsRouteElements — must be Route children, not a wrapper component. */
export function MlOpsRoutes() {
    return <>{mlOpsRouteElements()}</>;
}
