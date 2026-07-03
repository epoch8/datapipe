import React, { useCallback, useEffect, useRef, useState } from "react";
import { Button, Card, Typography } from "antd";
import { opsApi } from "../../../api/ops";
import type { RunLogLine } from "../../../types/ops";

const { Text } = Typography;
const MAX_LINES = 10_000;
const POLL_MS = 1000;
const BATCH_LIMIT = 500;

type Props = {
    runId: string;
    status: string;
};

export function RunLogsPanel({ runId, status }: Props) {
    const [lines, setLines] = useState<RunLogLine[]>([]);
    const lastSeqRef = useRef(0);
    const containerRef = useRef<HTMLPreElement>(null);
    const stickToBottomRef = useRef(true);

    const appendLines = useCallback((incoming: RunLogLine[]) => {
        if (!incoming.length) return;
        setLines((prev) => {
            const next = [...prev, ...incoming];
            return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
        });
        lastSeqRef.current = incoming[incoming.length - 1].seq;
    }, []);

    const fetchLogs = useCallback(async () => {
        try {
            let after = lastSeqRef.current;
            while (true) {
                const data = await opsApi.getRunLogs(runId, after, BATCH_LIMIT);
                if (!data.lines.length) break;
                appendLines(data.lines);
                after = data.last_seq;
                if (data.lines.length < BATCH_LIMIT) break;
            }
        } catch {
            /* ignore transient poll errors */
        }
    }, [runId, appendLines]);

    useEffect(() => {
        lastSeqRef.current = 0;
        setLines([]);
        fetchLogs();
    }, [runId, fetchLogs]);

    useEffect(() => {
        if (status !== "running") return;
        const timer = setInterval(fetchLogs, POLL_MS);
        return () => clearInterval(timer);
    }, [status, fetchLogs]);

    useEffect(() => {
        if (status === "running") return;
        fetchLogs();
    }, [status, fetchLogs]);

    useEffect(() => {
        const el = containerRef.current;
        if (!el || !stickToBottomRef.current) return;
        el.scrollTop = el.scrollHeight;
    }, [lines]);

    const onScroll = () => {
        const el = containerRef.current;
        if (!el) return;
        const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
        stickToBottomRef.current = atBottom;
    };

    return (
        <Card
            size="small"
            title="Logs"
            extra={
                <>
                    {lines.length > 0 && (
                        <Text type="secondary" style={{ marginRight: 8 }}>
                            {lines.length.toLocaleString()} lines
                        </Text>
                    )}
                    <Button size="small" onClick={fetchLogs}>
                        Refresh
                    </Button>
                </>
            }
        >
            {lines.length === 0 ? (
                <Text type="secondary">No log lines yet.</Text>
            ) : (
                <pre
                    ref={containerRef}
                    onScroll={onScroll}
                    className="run-logs-viewer"
                >
                    {lines.map((ln) => (
                        <div
                            key={ln.seq}
                            className={`run-log-line run-log-${ln.level.toLowerCase()}`}
                        >
                            <span className="run-log-ts">{ln.logged_at}</span>{" "}
                            <span className="run-log-level">[{ln.level}]</span>{" "}
                            {ln.message}
                        </div>
                    ))}
                </pre>
            )}
        </Card>
    );
}
