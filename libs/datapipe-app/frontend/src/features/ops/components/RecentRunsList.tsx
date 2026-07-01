import React from "react";
import { Button, Tag, Typography } from "antd";
import { useNavigate } from "react-router-dom";
import { formatRunTime } from "../utils/formatRunTime";

const { Text } = Typography;

export type RecentRunItem = {
    run_id: string;
    status: string;
    started_at?: string;
    finished_at?: string;
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
            {runs.map((run) => (
                <div key={run.run_id} className="recent-run-row">
                    {run.started_at && (
                        <Text type="secondary" className="recent-run-time">
                            {formatRunTime(run.started_at)}
                        </Text>
                    )}
                    <Button type="link" className="recent-run-link" onClick={() => navigate(`/runs/${run.run_id}`)}>
                        {run.run_id.slice(0, 8)}…
                    </Button>
                    <Tag color={statusColor[run.status]}>{run.status}</Tag>
                </div>
            ))}
        </div>
    );
}
