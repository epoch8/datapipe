import React from "react";
import { Spin } from "antd";
import { opsApi } from "../../api/ops";
import { ApiErrorAlert } from "../../components/ApiErrorAlert";
import { PipelineDetail } from "./PipelineDetail";

export function Overview() {
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);
    const [showMetrics, setShowMetrics] = React.useState(false);
    const [error, setError] = React.useState<unknown>(null);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((capabilities) => {
                setPipelineId(capabilities.pipeline_id ?? null);
                setShowMetrics(capabilities.ml_metrics);
            })
            .catch((e) => setError(e));
    }, []);

    if (error) return <ApiErrorAlert error={error} />;
    if (!pipelineId) return <Spin />;

    return <PipelineDetail pipelineId={pipelineId} embedded includeMetrics={showMetrics} />;
}
