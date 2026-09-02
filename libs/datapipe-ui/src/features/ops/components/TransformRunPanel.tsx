import React, { useEffect, useRef, useState } from "react";
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

function transformRunWsUrl(transformName: string): string {
    const configured = (process.env["REACT_APP_WEBSOCKET_URL"] as string) || "";
    const encoded = encodeURIComponent(transformName);
    if (configured.startsWith("ws://") || configured.startsWith("wss://")) {
        const base = configured.endsWith("/") ? configured : `${configured}/`;
        return `${base}${encoded}/run-status`;
    }
    const pathBase =
        configured || "/api/v1alpha3/ws/transform/";
    const path = `${pathBase.endsWith("/") ? pathBase : `${pathBase}/`}${encoded}/run-status`;
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${window.location.host}${path}`;
}

type Props = {
    transformName: string;
    indexKeys: string[];
};

export function TransformRunPanel({ transformName, indexKeys }: Props) {
    const [form] = Form.useForm();
    const wsRef = useRef<WebSocket | null>(null);
    const [wsReady, setWsReady] = useState(false);
    const [progress, setProgress] = useState<{
        status?: "active" | "success" | "exception";
        processed: number;
        total: number;
    }>({ processed: 0, total: 0 });
    const [runId, setRunId] = useState<string | null>(null);
    const [errorMsg, setErrorMsg] = useState<string | null>(null);
    const [running, setRunning] = useState(false);

    useEffect(() => {
        let cancelled = false;
        let socket: WebSocket | null = null;
        let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

        const connect = () => {
            if (cancelled) return;
            setWsReady(false);
            socket = new WebSocket(transformRunWsUrl(transformName));
            wsRef.current = socket;

            socket.onopen = () => {
                if (!cancelled) setWsReady(true);
            };
            socket.onmessage = (event) => {
                const msg = JSON.parse(event.data);
                if (msg.status === "not found" || msg.status === "not allowed") {
                    setErrorMsg(msg.detail || msg.status);
                    setRunning(false);
                    return;
                }
                if (msg.status === "running" || msg.status === "starting") {
                    setRunning(true);
                    setProgress({
                        status: "active",
                        processed: msg.processed ?? 0,
                        total: msg.total ?? 0,
                    });
                } else if (msg.status === "finished") {
                    setRunning(false);
                    setProgress({
                        status: "success",
                        processed: msg.processed ?? 0,
                        total: msg.total ?? 0,
                    });
                }
                if (msg.run_id) {
                    setRunId(msg.run_id);
                }
            };
            socket.onerror = () => {
                if (!cancelled) {
                    setWsReady(false);
                    setErrorMsg("WebSocket connection error");
                    setRunning(false);
                }
            };
            socket.onclose = () => {
                if (cancelled) return;
                setWsReady(false);
                wsRef.current = null;
                reconnectTimer = setTimeout(connect, 1500);
            };
        };

        connect();
        return () => {
            cancelled = true;
            if (reconnectTimer) clearTimeout(reconnectTimer);
            socket?.close();
            wsRef.current = null;
        };
    }, [transformName]);

    const runStep = () => {
        const socket = wsRef.current;
        if (!socket || socket.readyState !== WebSocket.OPEN) {
            setErrorMsg("WebSocket is not connected yet — wait a moment and try again");
            return;
        }

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

        try {
            socket.send(
                JSON.stringify({
                    transform: transformName,
                    operation: "run-step",
                    filters: filters.length ? filters : null,
                }),
            );
            setRunId(null);
            setErrorMsg(null);
            setRunning(true);
            setProgress({ status: "active", processed: 0, total: 0 });
        } catch (err) {
            setErrorMsg(err instanceof Error ? err.message : String(err));
            setRunning(false);
        }
    };

    return (
        <div>
            <Typography.Paragraph type="secondary">
                Run this transform for all pending indexes, or specify index values below
                (one row per index). Leave empty to process everything.
            </Typography.Paragraph>
            {indexKeys.length > 0 ? (
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
            ) : (
                <Alert
                    style={{ marginBottom: 12 }}
                    type="info"
                    showIcon
                    message="This transform has no index keys (e.g. DatatableTransform) — Run executes the full step. Index filters are only available for batch transforms."
                />
            )}
            <Space>
                <Button
                    type="primary"
                    htmlType="button"
                    onClick={runStep}
                    loading={running}
                    disabled={!wsReady && !running}
                >
                    Run transform
                </Button>
                <Text type="secondary">
                    {wsReady ? "Connected" : "Connecting…"}
                </Text>
            </Space>
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
