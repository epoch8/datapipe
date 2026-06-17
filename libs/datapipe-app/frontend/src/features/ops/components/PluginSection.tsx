import React from "react";
import { Card, Descriptions, Table, Tag } from "antd";
import type { Enrichment } from "../../../types/ops";

export function PluginSection({ enrichments }: { enrichments?: Enrichment[] }) {
    if (!enrichments?.length) return null;

    return (
        <>
            {enrichments.map((e, i) => {
                if (e.type === "ml_training" && e.payload?.rows) {
                    const rows = e.payload.rows as Record<string, unknown>[];
                    return (
                        <Card key={i} title="Training runs" style={{ marginTop: 16 }}>
                            <Table
                                size="small"
                                rowKey={(r) => String(r.run_key || r.model_id)}
                                dataSource={rows}
                                pagination={false}
                                columns={[
                                    { title: "Run key", dataIndex: "run_key" },
                                    { title: "Model", dataIndex: "model_id" },
                                    {
                                        title: "Status",
                                        dataIndex: "status",
                                        render: (v) => <Tag>{String(v)}</Tag>,
                                    },
                                    { title: "Attempt", dataIndex: "attempt" },
                                ]}
                            />
                        </Card>
                    );
                }
                if (e.type === "ml_metrics_summary") {
                    const p = e.payload;
                    return (
                        <Card key={i} title="Latest metrics" style={{ marginTop: 16 }}>
                            <Descriptions size="small" column={3}>
                                {Object.entries(p).map(([k, v]) => (
                                    <Descriptions.Item key={k} label={k}>
                                        {String(v)}
                                    </Descriptions.Item>
                                ))}
                            </Descriptions>
                        </Card>
                    );
                }
                if (e.type === "ml_best_model") {
                    return (
                        <Card key={i} title="Best model" style={{ marginTop: 16 }}>
                            <Tag color="gold">★ {String(e.payload.model_id || "—")}</Tag>
                        </Card>
                    );
                }
                return null;
            })}
        </>
    );
}
