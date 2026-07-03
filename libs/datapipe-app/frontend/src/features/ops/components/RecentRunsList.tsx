import React from "react";
import { Button, Tag, Typography } from "antd";
import { useNavigate } from "react-router-dom";
import { formatRunTime } from "../utils/formatRunTime";
import { formatRunTriggerLabel } from "../utils/recentRuns";

const { Text } = Typography;

export type RecentRunItem = {
    run_id: string;
    status: string;
    started_at?: string;
    finished_at?: string;
    trigger?: string;
};

const statusColor: Record<string, string> = {
    completed: "success",
    failed: "error",
    running: "processing",
};

export function RecentRunsList({
    runs,
    emptyText = "No runs yet",
}: {
    runs: RecentRunItem[];
    emptyText?: string;
}) {
    const navigate = useNavigate();

    if (!runs.length) {
        return <Text type="secondary">{emptyText}</Text>;
    }

    return (
        <div className="recent-runs-list">
            {runs.map((run) => {
                const scopeLabel = formatRunTriggerLabel(run.trigger);
                return (
                    <div key={run.run_id} className="recent-run-row">
                        {run.started_at && (
                            <Text type="secondary" className="recent-run-time">
                                {formatRunTime(run.started_at)}
                            </Text>
                        )}
                        <Button
                            type="link"
                            className="recent-run-link"
                            onClick={() => navigate(`/runs/${run.run_id}`)}
                        >
                            {run.run_id.slice(0, 8)}…
                        </Button>
                        {scopeLabel && (
                            <Tag className="recent-run-scope">{scopeLabel}</Tag>
                        )}
                        <Tag color={statusColor[run.status]}>{run.status}</Tag>
                    </div>
                );
            })}
        </div>
    );
}
