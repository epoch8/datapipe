import React from "react";
import { Alert, Spin } from "antd";
import { opsApi } from "../../api/ops";
import { PipelineDetail } from "./PipelineDetail";

export function Overview() {
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);
    const [showMetrics, setShowMetrics] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((capabilities) => {
                setPipelineId(capabilities.pipeline_id ?? null);
                setShowMetrics(capabilities.ml_metrics);
            })
            .catch((e) => setError(String(e)));
    }, []);

    if (error) return <Alert type="error" message={error} />;
    if (!pipelineId) return <Spin />;

    return <PipelineDetail pipelineId={pipelineId} embedded includeMetrics={showMetrics} />;
}
