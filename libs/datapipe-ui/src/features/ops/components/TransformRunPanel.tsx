import React, { useEffect, useState } from "react";
import {
    Alert,
    Button,
    Form,
    Input,
    Progress,
    Space,
    Typography,
} from "antd";
import { MinusCircleOutlined, PlusOutlined } from "@ant-design/icons";
import { Link } from "react-router-dom";
import type { IdxRow } from "../../../types";

const { Text } = Typography;

const TRANSFORM_WS_BASE =
    (process.env["REACT_APP_WEBSOCKET_URL"] as string) || "/api/v1alpha3/ws/transform/";

type Props = {
    transformName: string;
    indexKeys: string[];
};

export function TransformRunPanel({ transformName, indexKeys }: Props) {
    const [form] = Form.useForm();
    const [ws, setWs] = useState<WebSocket | null>(null);
    const [progress, setProgress] = useState<{
        status?: "active" | "success" | "exception";
        processed: number;
        total: number;
    }>({ processed: 0, total: 0 });
    const [runId, setRunId] = useState<string | null>(null);
    const [errorMsg, setErrorMsg] = useState<string | null>(null);

    useEffect(() => {
        const socket = new WebSocket(`${TRANSFORM_WS_BASE}${transformName}/run-status`);
        setWs(socket);
        socket.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            if (msg.status === "not found" || msg.status === "not allowed") {
                setErrorMsg(msg.status);
                return;
            }
            if (msg.status === "running") {
                setProgress({
                    status: "active",
                    processed: msg.processed,
                    total: msg.total,
                });
            } else if (msg.status === "finished") {
                setProgress({
                    status: "success",
                    processed: msg.processed,
                    total: msg.total,
                });
            }
            if (msg.run_id) {
                setRunId(msg.run_id);
            }
        };
        return () => socket.close();
    }, [transformName]);

    const runStep = () => {
        const rows = (form.getFieldValue("indexes") as IdxRow[] | undefined) ?? [];
        const filters = rows
            .map((row) => {
                const entry: IdxRow = {};
                indexKeys.forEach((k) => {
                    const v = row[k];
                    if (v !== undefined && v !== "") entry[k] = v;
                });
                return entry;
            })
            .filter((row) => Object.keys(row).length > 0);

        ws?.send(
            JSON.stringify({
                transform: transformName,
                operation: "run-step",
                filters: filters.length ? filters : null,
            }),
        );
        setRunId(null);
        setErrorMsg(null);
        setProgress({ processed: 0, total: 0 });
    };

    return (
        <div>
            <Typography.Paragraph type="secondary">
                Run this transform for all pending indexes, or specify index values below
                (one row per index). Leave empty to process everything.
            </Typography.Paragraph>
            {indexKeys.length > 0 && (
                <Form form={form} layout="vertical" initialValues={{ indexes: [{}] }}>
                    <Form.List name="indexes">
                        {(fields, { add, remove }) => (
                            <>
                                {fields.map((field) => (
                                    <Space
                                        key={field.key}
                                        align="baseline"
                                        wrap
                                        style={{ marginBottom: 8 }}
                                    >
                                        {indexKeys.map((key) => (
                                            <Form.Item
                                                key={key}
                                                name={[field.name, key]}
                                                label={key}
                                                style={{ marginBottom: 0 }}
                                            >
                                                <Input placeholder={key} style={{ width: 140 }} />
                                            </Form.Item>
                                        ))}
                                        {fields.length > 1 && (
                                            <MinusCircleOutlined onClick={() => remove(field.name)} />
                                        )}
                                    </Space>
                                ))}
                                <Form.Item>
                                    <Button
                                        type="dashed"
                                        onClick={() => add({})}
                                        icon={<PlusOutlined />}
                                        htmlType="button"
                                    >
                                        Add index row
                                    </Button>
                                </Form.Item>
                            </>
                        )}
                    </Form.List>
                </Form>
            )}
            <Button type="primary" htmlType="button" onClick={runStep}>
                Run transform
            </Button>
            {errorMsg && (
                <Alert style={{ marginTop: 12 }} type="warning" message={errorMsg} showIcon />
            )}
            {runId && (
                <Alert
                    style={{ marginTop: 12 }}
                    type="info"
                    showIcon
                    message={
                        <>
                            Run started —{" "}
                            <Link to={`/runs/${runId}`}>view logs and status</Link>
                        </>
                    }
                />
            )}
            {progress.status && progress.total > 0 && (
                <div style={{ marginTop: 12 }}>
                    <Text>
                        {progress.processed} / {progress.total}
                    </Text>
                    <Progress
                        size="small"
                        status={progress.status}
                        percent={Math.round((progress.processed * 100) / progress.total)}
                    />
                </div>
            )}
        </div>
    );
}
