import React, { Suspense } from "react";
import { Alert, Spin } from "antd";
import { opsApi } from "../../../api/ops";
import type { PipelineGraphProps } from "../../../types/pipelineGraph";

const CyGraph = React.lazy(() => import("../../cy"));

export type { PipelineGraphProps };

export function PipelineGraph(props: PipelineGraphProps) {
    return (
        <Suspense
            fallback={
                <div style={{ display: "flex", justifyContent: "center", padding: 48 }}>
                    <Spin />
                </div>
            }
        >
            <CyGraph {...props} />
        </Suspense>
    );
}

export function PipelineGraphAgentOnly(props: PipelineGraphProps) {
    const [agentMode, setAgentMode] = React.useState<boolean | null>(null);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((c) => setAgentMode(c.mode === "agent"))
            .catch(() => setAgentMode(false));
    }, []);

    if (agentMode === null) return <Spin />;
    if (!agentMode) {
        return (
            <Alert
                type="info"
                showIcon
                message="Pipeline graph is available in agent mode (run the pipeline API locally)."
            />
        );
    }
    return <PipelineGraph {...props} />;
}
