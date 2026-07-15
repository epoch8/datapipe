import React, { useCallback, useEffect, useRef, useState } from "react";
import { Button, Card, Dropdown, Menu, Typography } from "antd";
import { opsApi } from "../../../api/client";
import type { RunLogLine } from "../../../types/ops";

const { Text } = Typography;
const MAX_LINES = 10_000;
const DEFAULT_POLL_MS = 5000;
const BATCH_LIMIT = 500;
const STORAGE_KEY = "datapipe.ops.logsRefreshMs";

const REFRESH_OPTIONS: { label: string; value: number | null }[] = [
    { label: "Off", value: null },
    { label: "1s", value: 1000 },
    { label: "5s", value: 5000 },
    { label: "10s", value: 10000 },
    { label: "30s", value: 30000 },
];

function readStoredRefreshMs(): number | null {
    try {
        const raw = localStorage.getItem(STORAGE_KEY);
        if (raw === "off") return null;
        if (raw) {
            const parsed = Number(raw);
            if (!Number.isNaN(parsed)) return parsed;
        }
    } catch {
        /* ignore */
    }
    return DEFAULT_POLL_MS;
}

function formatRefreshLabel(ms: number | null): string {
    if (ms == null) return "Off";
    if (ms < 1000) return `${ms}ms`;
    return `${ms / 1000}s`;
}

type Props = {
    runId: string;
    status: string;
};

export function RunLogsPanel({ runId, status }: Props) {
    const [lines, setLines] = useState<RunLogLine[]>([]);
    const [refreshMs, setRefreshMs] = useState<number | null>(() => readStoredRefreshMs());
    const lastSeqRef = useRef(0);
    const containerRef = useRef<HTMLPreElement>(null);
    const stickToBottomRef = useRef(true);

    const appendLines = useCallback((incoming: RunLogLine[]) => {
        if (!incoming.length) return;
        setLines((prev) => {
            const seen = new Set(prev.map((ln) => ln.seq));
            const unique = incoming.filter((ln) => !seen.has(ln.seq));
            if (!unique.length) return prev;
            const next = [...prev, ...unique];
            return next.length > MAX_LINES ? next.slice(-MAX_LINES) : next;
        });
        lastSeqRef.current = Math.max(
            lastSeqRef.current,
            incoming[incoming.length - 1].seq,
        );
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

    const setRefreshInterval = (value: number | null) => {
        setRefreshMs(value);
        try {
            localStorage.setItem(STORAGE_KEY, value == null ? "off" : String(value));
        } catch {
            /* ignore */
        }
    };

    useEffect(() => {
        lastSeqRef.current = 0;
        setLines([]);
        fetchLogs();
    }, [runId, fetchLogs]);

    useEffect(() => {
        if (status !== "running" || refreshMs == null) return;
        const timer = window.setInterval(fetchLogs, refreshMs);
        return () => window.clearInterval(timer);
    }, [status, refreshMs, fetchLogs]);

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

    const clearLogs = () => {
        lastSeqRef.current = 0;
        setLines([]);
    };

    const downloadLogs = () => {
        const text = lines
            .map((ln) => `${ln.logged_at} [${ln.level}] ${ln.message}`)
            .join("\n");
        const blob = new Blob([text], { type: "text/plain" });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = `run-${runId.slice(0, 8)}-logs.txt`;
        anchor.click();
        URL.revokeObjectURL(url);
    };

    const refreshMenu = (
        <Menu
            selectedKeys={[refreshMs == null ? "off" : String(refreshMs)]}
            onClick={({ key }) => {
                if (key === "off") setRefreshInterval(null);
                else setRefreshInterval(Number(key));
            }}
        >
            {REFRESH_OPTIONS.map((opt) => (
                <Menu.Item key={opt.value == null ? "off" : String(opt.value)}>
                    {opt.label}
                </Menu.Item>
            ))}
        </Menu>
    );

    return (
        <Card
            size="small"
            title="Logs"
            extra={
                <>
                    <Dropdown overlay={refreshMenu}>
                        <Button size="small" style={{ marginRight: 8 }}>
                            Refresh: {formatRefreshLabel(refreshMs)}
                        </Button>
                    </Dropdown>
                    {lines.length > 0 && (
                        <Text type="secondary" style={{ marginRight: 8 }}>
                            {lines.length.toLocaleString()} lines
                        </Text>
                    )}
                    <Button size="small" onClick={fetchLogs} style={{ marginRight: 8 }}>
                        Refresh
                    </Button>
                    <Button size="small" onClick={downloadLogs} disabled={!lines.length} style={{ marginRight: 8 }}>
                        Download
                    </Button>
                    <Button size="small" onClick={clearLogs} disabled={!lines.length}>
                        Clear
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
