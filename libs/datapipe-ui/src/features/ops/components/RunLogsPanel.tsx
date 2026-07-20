import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Alert, Button, Card, Dropdown, Menu, Typography } from "antd";
import { opsApi } from "../../../api/client";
import type { RunLogLine } from "../../../types/ops";
import { ansiToHtml, stripAnsi } from "./ansi";

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
    /** When set, only lines whose message mentions this step name are shown. */
    stepFilter?: string | null;
    onClearStepFilter?: () => void;
};

export function RunLogsPanel({ runId, status, stepFilter = null, onClearStepFilter }: Props) {
    const [lines, setLines] = useState<RunLogLine[]>([]);
    const [refreshMs, setRefreshMs] = useState<number | null>(() => readStoredRefreshMs());
    const [runLogsConfigured, setRunLogsConfigured] = useState<boolean | null>(null);
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
        opsApi
            .getSettings()
            .then((info) => setRunLogsConfigured(info.run_logs_configured ?? false))
            .catch(() => setRunLogsConfigured(null));
    }, []);

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

    const visibleLines = useMemo(() => {
        if (!stepFilter) return lines;
        const needle = stepFilter.toLowerCase();
        return lines.filter((ln) => ln.message.toLowerCase().includes(needle));
    }, [lines, stepFilter]);

    useEffect(() => {
        const el = containerRef.current;
        if (!el || !stickToBottomRef.current) return;
        el.scrollTop = el.scrollHeight;
    }, [visibleLines]);

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
        const text = visibleLines
            .map((ln) => `${ln.logged_at} [${ln.level}] ${stripAnsi(ln.message)}`)
            .join("\n");
        const blob = new Blob([text], { type: "text/plain" });
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        const suffix = stepFilter ? `-${stepFilter.slice(0, 24)}` : "";
        anchor.download = `run-${runId.slice(0, 8)}${suffix}-logs.txt`;
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
                    {visibleLines.length > 0 && (
                        <Text type="secondary" style={{ marginRight: 8 }}>
                            {visibleLines.length.toLocaleString()} lines
                            {stepFilter && lines.length !== visibleLines.length
                                ? ` / ${lines.length.toLocaleString()} total`
                                : ""}
                        </Text>
                    )}
                    <Button size="small" onClick={fetchLogs} style={{ marginRight: 8 }}>
                        Refresh
                    </Button>
                    <Button
                        size="small"
                        onClick={downloadLogs}
                        disabled={!visibleLines.length}
                        style={{ marginRight: 8 }}
                    >
                        Download
                    </Button>
                    <Button size="small" onClick={clearLogs} disabled={!lines.length}>
                        Clear
                    </Button>
                </>
            }
        >
            {runLogsConfigured === false && (
                <Alert
                    type="warning"
                    showIcon
                    style={{ marginBottom: 12 }}
                    message="Run logs are not being recorded"
                    description={
                        <>
                            Pass <Text code>run_logs_backend</Text> to{" "}
                            <Text code>DatapipeAPI</Text> (for example{" "}
                            <Text code>
                                {"RunLogsBackend.clickhouse('clickhouse://...')"}
                            </Text>
                            ).
                        </>
                    }
                />
            )}
            {stepFilter && (
                <div style={{ marginBottom: 8 }}>
                    <Text type="secondary">
                        Filtered by step{" "}
                        <Text code>{stepFilter}</Text>
                    </Text>
                    {onClearStepFilter && (
                        <Button type="link" size="small" onClick={onClearStepFilter}>
                            Show all
                        </Button>
                    )}
                </div>
            )}
            {visibleLines.length === 0 ? (
                <Text type="secondary">
                    {stepFilter && lines.length > 0
                        ? "No log lines mention this step."
                        : "No log lines yet."}
                </Text>
            ) : (
                <pre
                    ref={containerRef}
                    onScroll={onScroll}
                    className="run-logs-viewer"
                >
                    {visibleLines.map((ln) => (
                        <div
                            key={ln.seq}
                            className={`run-log-line run-log-${ln.level.toLowerCase()}`}
                        >
                            <span className="run-log-ts">{ln.logged_at}</span>{" "}
                            <span className="run-log-level">[{ln.level}]</span>{" "}
                            <span
                                className="run-log-msg"
                                dangerouslySetInnerHTML={{
                                    __html: ansiToHtml(ln.message),
                                }}
                            />
                        </div>
                    ))}
                </pre>
            )}
        </Card>
    );
}
