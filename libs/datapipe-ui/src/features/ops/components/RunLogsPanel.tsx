import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Card, Dropdown, Menu, Typography } from "antd";
import { opsApi } from "../../../api/client";
import type { RunLogLine } from "../../../types/ops";
import { ansiToHtml, stripAnsi } from "./ansi";

const { Text } = Typography;
/** Max lines kept in the browser at once (sliding window). */
const MAX_LINES = 10_000;
const DEFAULT_POLL_MS = 5000;
const BATCH_LIMIT = 500;
const STORAGE_KEY = "datapipe.ops.logsRefreshMs";
const SCROLL_LOAD_THRESHOLD_PX = 80;

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

function mergeUniqueBySeq(existing: RunLogLine[], incoming: RunLogLine[]): RunLogLine[] {
    if (!incoming.length) return existing;
    const seen = new Set(existing.map((ln) => ln.seq));
    const unique = incoming.filter((ln) => !seen.has(ln.seq));
    if (!unique.length) return existing;
    return [...existing, ...unique].sort((a, b) => a.seq - b.seq);
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
    const [maxSeq, setMaxSeq] = useState(0);
    const [hasOlder, setHasOlder] = useState(false);
    const [loadingOlder, setLoadingOlder] = useState(false);
    const [refreshMs, setRefreshMs] = useState<number | null>(() => readStoredRefreshMs());
    const lastSeqRef = useRef(0);
    const firstSeqRef = useRef(0);
    const loadingOlderRef = useRef(false);
    const containerRef = useRef<HTMLPreElement>(null);
    const stickToBottomRef = useRef(true);

    const applyWindow = useCallback((next: RunLogLine[], trimFrom: "start" | "end") => {
        let windowed = next;
        if (windowed.length > MAX_LINES) {
            windowed =
                trimFrom === "end"
                    ? windowed.slice(0, MAX_LINES)
                    : windowed.slice(-MAX_LINES);
        }
        firstSeqRef.current = windowed[0]?.seq ?? 0;
        lastSeqRef.current = windowed[windowed.length - 1]?.seq ?? 0;
        setHasOlder(firstSeqRef.current > 1);
        return windowed;
    }, []);

    const appendNewer = useCallback(
        (incoming: RunLogLine[]) => {
            if (!incoming.length) return;
            setLines((prev) => applyWindow(mergeUniqueBySeq(prev, incoming), "start"));
            setMaxSeq((m) => Math.max(m, incoming[incoming.length - 1].seq));
        },
        [applyWindow],
    );

    const prependOlder = useCallback(
        (incoming: RunLogLine[]) => {
            if (!incoming.length) return;
            setLines((prev) => {
                const firstSeq = prev[0]?.seq ?? Number.POSITIVE_INFINITY;
                const older = incoming.filter((ln) => ln.seq < firstSeq);
                if (!older.length) return prev;
                return applyWindow(mergeUniqueBySeq(older, prev), "end");
            });
        },
        [applyWindow],
    );

    const fetchNewer = useCallback(async () => {
        try {
            let after = lastSeqRef.current;
            while (true) {
                const data = await opsApi.getRunLogs(runId, after, BATCH_LIMIT);
                if (typeof data.max_seq === "number") {
                    setMaxSeq((m) => Math.max(m, data.max_seq ?? 0));
                }
                if (!data.lines.length) break;
                appendNewer(data.lines);
                after = data.last_seq;
                if (data.lines.length < BATCH_LIMIT) break;
            }
        } catch {
            /* ignore transient poll errors */
        }
    }, [runId, appendNewer]);

    const loadInitialTail = useCallback(async () => {
        try {
            // Probe max_seq with a tiny request, then load the last MAX_LINES window.
            const probe = await opsApi.getRunLogs(runId, 0, 1);
            const knownMax = typeof probe.max_seq === "number" ? probe.max_seq : probe.last_seq;
            setMaxSeq(knownMax);
            if (knownMax <= 0) {
                setLines([]);
                firstSeqRef.current = 0;
                lastSeqRef.current = 0;
                setHasOlder(false);
                return;
            }
            let after = Math.max(0, knownMax - MAX_LINES);
            const collected: RunLogLine[] = [];
            while (after < knownMax) {
                const data = await opsApi.getRunLogs(runId, after, BATCH_LIMIT);
                if (!data.lines.length) break;
                collected.push(...data.lines);
                after = data.last_seq;
                if (data.lines.length < BATCH_LIMIT) break;
            }
            const windowed = applyWindow(collected, "start");
            setLines(windowed);
            stickToBottomRef.current = true;
        } catch {
            /* ignore */
        }
    }, [runId, applyWindow]);

    const loadOlder = useCallback(async () => {
        if (loadingOlderRef.current) return;
        const firstSeq = firstSeqRef.current;
        if (firstSeq <= 1) {
            setHasOlder(false);
            return;
        }
        loadingOlderRef.current = true;
        setLoadingOlder(true);
        const el = containerRef.current;
        const prevHeight = el?.scrollHeight ?? 0;
        const prevTop = el?.scrollTop ?? 0;
        try {
            const after = Math.max(0, firstSeq - BATCH_LIMIT - 1);
            const data = await opsApi.getRunLogs(runId, after, BATCH_LIMIT);
            if (typeof data.max_seq === "number") {
                setMaxSeq((m) => Math.max(m, data.max_seq ?? 0));
            }
            const older = data.lines.filter((ln) => ln.seq < firstSeq);
            if (!older.length) {
                setHasOlder(false);
                return;
            }
            prependOlder(older);
            // Preserve viewport after prepending DOM nodes above.
            requestAnimationFrame(() => {
                const node = containerRef.current;
                if (!node) return;
                const delta = node.scrollHeight - prevHeight;
                node.scrollTop = prevTop + delta;
            });
        } catch {
            /* ignore */
        } finally {
            loadingOlderRef.current = false;
            setLoadingOlder(false);
        }
    }, [runId, prependOlder]);

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
        firstSeqRef.current = 0;
        setLines([]);
        setMaxSeq(0);
        setHasOlder(false);
        stickToBottomRef.current = true;
        loadInitialTail();
    }, [runId, loadInitialTail]);

    useEffect(() => {
        if (status !== "running" || refreshMs == null) return;
        const timer = window.setInterval(fetchNewer, refreshMs);
        return () => window.clearInterval(timer);
    }, [status, refreshMs, fetchNewer]);

    useEffect(() => {
        if (status === "running") return;
        fetchNewer();
    }, [status, fetchNewer]);

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
        if (el.scrollTop < SCROLL_LOAD_THRESHOLD_PX && hasOlder && !loadingOlderRef.current) {
            void loadOlder();
        }
    };

    const clearLogs = () => {
        lastSeqRef.current = 0;
        firstSeqRef.current = 0;
        setLines([]);
        setHasOlder(false);
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

    const windowLabel = useMemo(() => {
        if (!visibleLines.length) return null;
        const from = visibleLines[0].seq;
        const to = visibleLines[visibleLines.length - 1].seq;
        const totalHint = maxSeq > to ? ` / ${maxSeq.toLocaleString()} total` : "";
        return `${visibleLines.length.toLocaleString()} lines (seq ${from.toLocaleString()}–${to.toLocaleString()})${totalHint}`;
    }, [visibleLines, maxSeq]);

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
                    {windowLabel && (
                        <Text type="secondary" style={{ marginRight: 8 }}>
                            {windowLabel}
                            {stepFilter && lines.length !== visibleLines.length
                                ? ` (filtered from ${lines.length.toLocaleString()})`
                                : ""}
                        </Text>
                    )}
                    <Button size="small" onClick={fetchNewer} style={{ marginRight: 8 }}>
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
            {stepFilter && (
                <div style={{ marginBottom: 8 }}>
                    <Text type="secondary">
                        Filtered by step <Text code>{stepFilter}</Text>
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
                <pre ref={containerRef} onScroll={onScroll} className="run-logs-viewer">
                    {hasOlder && (
                        <div className="run-log-line run-log-info" style={{ opacity: 0.7 }}>
                            {loadingOlder
                                ? "Loading older logs…"
                                : "Scroll up to load older logs"}
                        </div>
                    )}
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
