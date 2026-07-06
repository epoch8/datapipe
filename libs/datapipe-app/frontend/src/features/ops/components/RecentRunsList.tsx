import React from "react";
import { Button, Tag, Typography } from "antd";
import { Link, useNavigate } from "react-router-dom";
import { formatRunRelative, formatRunTime } from "../utils/formatRunTime";
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

const statusIcon: Record<string, string> = {
    completed: "✓",
    failed: "✕",
    running: "⟳",
};

export function RecentRunsList({
    runs,
    emptyText = "No runs yet",
    compact = false,
    viewAllHref,
}: {
    runs: RecentRunItem[];
    emptyText?: string;
    compact?: boolean;
    viewAllHref?: string;
}) {
    const navigate = useNavigate();

    if (!runs.length) {
        return <Text type="secondary">{emptyText}</Text>;
    }

    return (
        <div className="recent-runs-list">
            {runs.map((run) => {
                const scopeLabel = formatRunTriggerLabel(run.trigger);
                const timeLabel = compact
                    ? formatRunRelative(run.started_at)
                    : run.started_at
                      ? formatRunTime(run.started_at)
                      : null;
                return (
                    <div key={run.run_id} className={`recent-run-row${compact ? " recent-run-row-compact" : ""}`}>
                        {compact && (
                            <span className={`recent-run-status-icon recent-run-status-${run.status}`}>
                                {statusIcon[run.status] ?? "·"}
                            </span>
                        )}
                        <Button
                            type="link"
                            className="recent-run-link"
                            onClick={() => navigate(`/runs/${run.run_id}`)}
                        >
                            {run.run_id.slice(0, 8)}
                        </Button>
                        <Tag color={statusColor[run.status]}>{run.status}</Tag>
                        {scopeLabel && (
                            <Tag className="recent-run-scope">{scopeLabel}</Tag>
                        )}
                        {timeLabel && (
                            <Text type="secondary" className="recent-run-time">
                                {timeLabel}
                            </Text>
                        )}
                    </div>
                );
            })}
            {viewAllHref && (
                <Link to={viewAllHref} className="recent-runs-view-all">
                    View all runs →
                </Link>
            )}
        </div>
    );
}
