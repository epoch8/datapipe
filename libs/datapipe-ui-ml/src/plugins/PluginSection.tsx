import React from "react";
import { Card, Descriptions, Table, Tag } from "antd";
import type { Enrichment } from "@datapipe/ui/types/ops";

export function MlPluginSection({ enrichments }: { enrichments?: Enrichment[] }) {
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
                if (e.type === "ml_metrics_summary" || e.type === "ops_metrics_summary") {
                    const p = e.payload as Record<string, unknown>;
                    const name = p.metric_name ?? "metric";
                    const value = p.metric_value ?? "—";
                    return (
                        <Card key={i} title="Latest metrics" style={{ marginTop: 16 }}>
                            <Descriptions size="small" column={2}>
                                <Descriptions.Item label={String(name)}>{String(value)}</Descriptions.Item>
                                {p.model_id != null ? (
                                    <Descriptions.Item label="Model">{String(p.model_id)}</Descriptions.Item>
                                ) : null}
                                {p.source_table != null ? (
                                    <Descriptions.Item label="Table">{String(p.source_table)}</Descriptions.Item>
                                ) : null}
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
