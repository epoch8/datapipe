import React, { Suspense } from "react";
import { Spin } from "antd";
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

export const PipelineGraphAgentOnly = PipelineGraph;
