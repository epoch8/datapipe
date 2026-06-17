import React from "react";
import { Badge, Button, Card, Progress, Tag, Typography } from "antd";
import { useNavigate } from "react-router-dom";
import type { Health, OverviewCard } from "../../../types/ops";

const { Text, Title } = Typography;

const healthBorder: Record<Health, string> = {
    healthy: "#52c41a",
    running: "#1890ff",
    training: "#faad14",
    degraded: "#fa8c16",
    failed: "#ff4d4f",
};

const healthLabel: Record<Health, string> = {
    healthy: "Healthy",
    running: "Running",
    training: "Training",
    degraded: "Degraded",
    failed: "Failed",
};

function actionLabel(action: OverviewCard["primary_action"]) {
    switch (action.type) {
        case "view_training":
            return "View training";
        case "view_run":
            return "View run";
        default:
            return "View details";
    }
}

export function PipelineCard({ card }: { card: OverviewCard }) {
    const navigate = useNavigate();
    const borderColor = healthBorder[card.health] || "#d9d9d9";
    const tp = card.training_preview;
    const progress =
        tp?.epoch && tp?.total_epochs
            ? Math.round((tp.epoch / tp.total_epochs) * 100)
            : undefined;

    return (
        <Card
            style={{ borderLeft: `4px solid ${borderColor}`, marginBottom: 16 }}
            bodyStyle={{ padding: 16 }}
        >
            <div style={{ display: "flex", justifyContent: "space-between", gap: 16 }}>
                <div>
                    <div style={{ marginBottom: 8 }}>
                        <Badge color={borderColor} text={healthLabel[card.health]} />
                        {card.task_type && (
                            <Tag style={{ marginLeft: 8 }}>{card.task_type}</Tag>
                        )}
                    </div>
                    <Title level={4} style={{ margin: 0 }}>
                        {card.display_name}
                    </Title>
                    {card.last_run_age && (
                        <Text type="secondary">Last run: {card.last_run_age}</Text>
                    )}
                    {card.next_run_at && (
                        <div>
                            <Text type="secondary">Next run: {card.next_run_at}</Text>
                        </div>
                    )}
                    {tp?.epoch != null && tp?.total_epochs != null && (
                        <div style={{ marginTop: 8 }}>
                            <Text>
                                epoch {tp.epoch}/{tp.total_epochs}
                                {tp.heartbeat_age_s != null && ` · heartbeat ${tp.heartbeat_age_s}s ago`}
                            </Text>
                            {progress != null && (
                                <Progress percent={progress} size="small" showInfo={false} />
                            )}
                        </div>
                    )}
                    {card.error_snippet && (
                        <Text type="danger" style={{ display: "block", marginTop: 8 }}>
                            {card.error_snippet}
                        </Text>
                    )}
                </div>
                <Button type="primary" onClick={() => navigate(card.primary_action.target)}>
                    {actionLabel(card.primary_action)}
                </Button>
            </div>
        </Card>
    );
}
