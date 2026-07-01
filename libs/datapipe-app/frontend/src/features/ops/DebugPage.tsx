import React from "react";
import { Link, useSearchParams } from "react-router-dom";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { workflowIconSvg } from "../cy/nodeIcons";

export function DebugPage() {
    const [searchParams] = useSearchParams();
    const stage = searchParams.get("stage");

    const title = stage ? `Pipeline graph · ${stage}` : "Pipeline graph";

    return (
        <div>
            <div className="datapipe-breadcrumb">
                <Link to="/">Overview</Link> / Debug
                {stage ? ` / ${stage}` : ""}
            </div>
            <div className="pipeline-card">
                <div className="pipeline-card-header">
                    <div className="pipeline-card-title">
                        <span
                            className="pipeline-card-title-icon"
                            dangerouslySetInnerHTML={{ __html: workflowIconSvg }}
                        />
                        {title}
                    </div>
                </div>
                <div className="pipeline-card-body">
                    <PipelineGraphAgentOnly
                        stageFilter={stage}
                        height="100%"
                        rankDir="TB"
                        refreshIntervalMs={0}
                    />
                </div>
            </div>
        </div>
    );
}
