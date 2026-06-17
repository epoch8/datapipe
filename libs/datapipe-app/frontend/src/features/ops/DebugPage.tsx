import React from "react";
import { Card, Typography } from "antd";
import { Link, useSearchParams } from "react-router-dom";
import { getRefreshIntervalMs } from "../../api/ops";
import { PipelineGraphAgentOnly } from "./components/PipelineGraph";

const { Text } = Typography;

export function DebugPage() {
    const [searchParams] = useSearchParams();
    const stage = searchParams.get("stage");

    const title = stage ? `Pipeline graph · ${stage}` : "Pipeline graph";

    return (
        <div>
            <Text type="secondary">
                <Link to="/">Overview</Link> / Debug
                {stage ? ` / ${stage}` : ""}
            </Text>
            <Card
                title={title}
                style={{ marginTop: 16 }}
                bodyStyle={{ padding: 0, height: "calc(100vh - 220px)", minHeight: 420 }}
            >
                <PipelineGraphAgentOnly
                    stageFilter={stage}
                    height="100%"
                    rankDir="TB"
                    refreshIntervalMs={getRefreshIntervalMs()}
                />
            </Card>
        </div>
    );
}
