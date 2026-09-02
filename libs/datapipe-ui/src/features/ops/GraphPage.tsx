import React from "react";
import { Segmented } from "antd";
import { useSearchParams } from "react-router-dom";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";
import { PageHeader } from "./shared";
import { workflowIconSvg } from "../cy/nodeIcons";
import {
    FLOW_LAYOUT_SEGMENTED_OPTIONS,
    parseFlowLayout,
} from "./utils/flowLayout";

/**
 * Cloud Ops graph page: pipeline DAG against `/api/v1alpha3/graph`.
 * Label-graph / runs chrome from the full observability UI is omitted.
 */
export function GraphPage() {
    const [searchParams, setSearchParams] = useSearchParams();
    const flowLayout = parseFlowLayout(searchParams);
    const [graphRefreshToken, setGraphRefreshToken] = React.useState(0);

    const setFlowLayout = (nextLayout: string) => {
        setSearchParams(
            (prev) => {
                const next = new URLSearchParams(prev);
                next.delete("direction");
                if (nextLayout === "snake") next.delete("layout");
                else next.set("layout", nextLayout);
                return next;
            },
            { replace: true },
        );
    };

    const refresh = React.useCallback(() => {
        setGraphRefreshToken((token) => token + 1);
    }, []);

    return (
        <div className="graph-page">
            <PageHeader
                breadcrumbs={[{ label: "Graph" }]}
                title="Pipeline graph"
                onRefresh={refresh}
            />
            <div className="pipeline-card">
                <div className="pipeline-card-header">
                    <div className="pipeline-card-header-left">
                        <div className="pipeline-card-title">
                            <span
                                className="pipeline-card-title-icon"
                                dangerouslySetInnerHTML={{ __html: workflowIconSvg }}
                            />
                            Pipeline graph
                        </div>
                        <Segmented
                            size="small"
                            value={flowLayout}
                            options={[...FLOW_LAYOUT_SEGMENTED_OPTIONS]}
                            onChange={(value) => setFlowLayout(String(value))}
                        />
                    </div>
                </div>
                <div className="pipeline-card-body">
                    <PipelineGraphAgentOnly
                        layoutMode="dag"
                        height="100%"
                        flowLayout={flowLayout}
                        refreshIntervalMs={0}
                        graphRefreshToken={graphRefreshToken}
                    />
                </div>
            </div>
        </div>
    );
}
