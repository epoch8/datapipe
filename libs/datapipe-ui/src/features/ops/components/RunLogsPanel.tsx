import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button, Card, Dropdown, Menu, Typography } from "antd";
import { opsApi } from "../../../api/client";
import type { RunLogLine } from "../../../types/ops";
import { ansiToHtml, stripAnsi } from "./ansi";

const { Text } = Typography;

/**
 * Fetch / scroll chunk size.
 * Kubernetes Dashboard defaults to 100 lines from the end; infinite-scroll
 * log UIs commonly use 100–200. Stay under the API cap (1000).
 */
const PAGE_SIZE = 200;
/** Max lines kept in the browser at once (sliding window). */
const MAX_LINES = 10_000;
const DEFAULT_POLL_MS = 5000;
const STORAGE_KEY = "datapipe.ops.logsRefreshMs";
const SCROLL_EDGE_PX = 80;

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
    const [hasNewer, setHasNewer] = useState(false);
    const [loading, setLoading] = useState(true);
    const [loadingOlder, setLoadingOlder] = useState(false);
    const [loadingNewer, setLoadingNewer] = useState(false);
    const [refreshMs, setRefreshMs] = useState<number | null>(() => readStoredRefreshMs());
    const lastSeqRef = useRef(0);
    const firstSeqRef = useRef(0);
    const maxSeqRef = useRef(0);
    const loadingOlderRef = useRef(false);
    const loadingNewerRef = useRef(false);
    const loadingWindowRef = useRef(false);
    const containerRef = useRef<HTMLPreElement>(null);
    const stickToBottomRef = useRef(true);

    const syncCursor = useCallback((windowed: RunLogLine[], knownMax?: number) => {
        firstSeqRef.current = windowed[0]?.seq ?? 0;
        lastSeqRef.current = windowed[windowed.length - 1]?.seq ?? lastSeqRef.current;
        const max = knownMax ?? maxSeqRef.current;
        setHasOlder(firstSeqRef.current > 1);
        setHasNewer(lastSeqRef.current > 0 && lastSeqRef.current < max);
    }, []);

    const applyWindow = useCallback(
        (next: RunLogLine[], trimFrom: "start" | "end") => {
            let windowed = next;
            if (windowed.length > MAX_LINES) {
                windowed =
                    trimFrom === "end"
                        ? windowed.slice(0, MAX_LINES)
                        : windowed.slice(-MAX_LINES);
            }
            syncCursor(windowed);
            return windowed;
        },
        [syncCursor],
    );

    const noteMaxSeq = useCallback((value: number | undefined) => {
        if (typeof value !== "number" || value <= 0) return;
        maxSeqRef.current = Math.max(maxSeqRef.current, value);
        setMaxSeq((m) => Math.max(m, value));
    }, []);

    const replaceWindow = useCallback(
        async (after: number, stick: "top" | "bottom") => {
            if (loadingWindowRef.current) return;
            loadingWindowRef.current = true;
            setLoading(true);
            try {
                const data = await opsApi.getRunLogs(runId, after, PAGE_SIZE);
                noteMaxSeq(data.max_seq);
                const windowed = applyWindow(data.lines, "start");
                // Re-sync hasNewer against latest max after this fetch.
                syncCursor(windowed, maxSeqRef.current);
                setLines(windowed);
                stickToBottomRef.current = stick === "bottom";
                requestAnimationFrame(() => {
                    const el = containerRef.current;
                    if (!el) return;
                    el.scrollTop = stick === "bottom" ? el.scrollHeight : 0;
                });
            } catch {
                /* ignore */
            } finally {
                loadingWindowRef.current = false;
                setLoading(false);
            }
        },
        [runId, applyWindow, noteMaxSeq, syncCursor],
    );

    const jumpToStart = useCallback(async () => {
        await replaceWindow(0, "top");
    }, [replaceWindow]);

    const jumpToEnd = useCallback(async () => {
        try {
            const probe = await opsApi.getRunLogs(runId, 0, 1);
            const knownMax =
                typeof probe.max_seq === "number" ? probe.max_seq : probe.last_seq;
            noteMaxSeq(knownMax);
            if (knownMax <= 0) {
                setLines([]);
                firstSeqRef.current = 0;
                lastSeqRef.current = 0;
                setHasOlder(false);
                setHasNewer(false);
                setLoading(false);
                return;
            }
            const after = Math.max(0, knownMax - PAGE_SIZE);
            await replaceWindow(after, "bottom");
        } catch {
            setLoading(false);
        }
    }, [runId, noteMaxSeq, replaceWindow]);

    const appendNewer = useCallback(
        (incoming: RunLogLine[]) => {
            if (!incoming.length) return;
            setLines((prev) => applyWindow(mergeUniqueBySeq(prev, incoming), "start"));
            noteMaxSeq(incoming[incoming.length - 1].seq);
        },
        [applyWindow, noteMaxSeq],
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

    /**
     * Append only lines after the current cursor.
     * Never scans from seq 0 — that path caused Clear→Refresh to re-read 10k+.
     * If the gap is larger than the memory window, jump to the tail instead.
     */
    const fetchNewer = useCallback(async () => {
        const after = lastSeqRef.current;
        if (after <= 0) {
            await jumpToEnd();
            return;
        }
        try {
            const probe = await opsApi.getRunLogs(runId, after, 1);
            noteMaxSeq(
                typeof probe.max_seq === "number" ? probe.max_seq : undefined,
            );
            const knownMax = maxSeqRef.current;
            if (knownMax - after > MAX_LINES) {
                await jumpToEnd();
                return;
            }
            let cursor = after;
            while (cursor < knownMax) {
                const data = await opsApi.getRunLogs(runId, cursor, PAGE_SIZE);
                noteMaxSeq(data.max_seq);
                if (!data.lines.length) break;
                appendNewer(data.lines);
                cursor = data.last_seq;
                if (data.lines.length < PAGE_SIZE) break;
            }
            setHasNewer(lastSeqRef.current < maxSeqRef.current);
        } catch {
            /* ignore transient poll errors */
        }
    }, [runId, appendNewer, jumpToEnd, noteMaxSeq]);

    const loadOlder = useCallback(async () => {
        if (loadingOlderRef.current || loadingWindowRef.current) return;
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
            const after = Math.max(0, firstSeq - PAGE_SIZE - 1);
            const data = await opsApi.getRunLogs(runId, after, PAGE_SIZE);
            noteMaxSeq(data.max_seq);
            const older = data.lines.filter((ln) => ln.seq < firstSeq);
            if (!older.length) {
                setHasOlder(false);
                return;
            }
            prependOlder(older);
            requestAnimationFrame(() => {
                const node = containerRef.current;
                if (!node) return;
                node.scrollTop = prevTop + (node.scrollHeight - prevHeight);
            });
        } catch {
            /* ignore */
        } finally {
            loadingOlderRef.current = false;
            setLoadingOlder(false);
        }
    }, [runId, prependOlder, noteMaxSeq]);

    const loadNewerPage = useCallback(async () => {
        if (loadingNewerRef.current || loadingWindowRef.current) return;
        const after = lastSeqRef.current;
        if (after <= 0 || after >= maxSeqRef.current) {
            setHasNewer(false);
            return;
        }
        loadingNewerRef.current = true;
        setLoadingNewer(true);
        try {
            const data = await opsApi.getRunLogs(runId, after, PAGE_SIZE);
            noteMaxSeq(data.max_seq);
            if (!data.lines.length) {
                setHasNewer(false);
                return;
            }
            appendNewer(data.lines);
            setHasNewer(lastSeqRef.current < maxSeqRef.current);
        } catch {
            /* ignore */
        } finally {
            loadingNewerRef.current = false;
            setLoadingNewer(false);
        }
    }, [runId, appendNewer, noteMaxSeq]);

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
        maxSeqRef.current = 0;
        setLines([]);
        setMaxSeq(0);
        setHasOlder(false);
        setHasNewer(false);
        stickToBottomRef.current = true;
        void jumpToEnd();
    }, [runId, jumpToEnd]);

    useEffect(() => {
        if (status !== "running" || refreshMs == null) return;
        const timer = window.setInterval(() => {
            if (!stickToBottomRef.current) return;
            void fetchNewer();
        }, refreshMs);
        return () => window.clearInterval(timer);
    }, [status, refreshMs, fetchNewer]);

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
        if (el.scrollTop < SCROLL_EDGE_PX && hasOlder && !loadingOlderRef.current) {
            void loadOlder();
        }
        // Older window: scrolling to the bottom loads the next ~PAGE_SIZE page.
        // Live follow uses the poll path (fetchNewer) instead.
        if (atBottom && hasNewer && !loadingNewerRef.current) {
            void loadNewerPage();
        }
    };

    const clearLogs = () => {
        // Keep the high-water mark so Refresh/poll only append truly new lines
        // instead of re-reading history from seq 0.
        const watermark = Math.max(lastSeqRef.current, maxSeqRef.current);
        lastSeqRef.current = watermark;
        firstSeqRef.current = 0;
        setLines([]);
        setHasOlder(false);
        setHasNewer(false);
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

    const totalPages = Math.max(1, Math.ceil(maxSeq / PAGE_SIZE));
    const currentPage = useMemo(() => {
        if (!visibleLines.length || maxSeq <= 0) return null;
        // Page 1 = seq 1..PAGE_SIZE; page N = last PAGE_SIZE of the run.
        const mid = visibleLines[Math.floor(visibleLines.length / 2)].seq;
        return Math.min(totalPages, Math.max(1, Math.ceil(mid / PAGE_SIZE)));
    }, [visibleLines, maxSeq, totalPages]);

    const jumpToPage = useCallback(
        async (page: number) => {
            const clamped = Math.min(totalPages, Math.max(1, page));
            if (clamped >= totalPages) {
                await jumpToEnd();
                return;
            }
            const after = (clamped - 1) * PAGE_SIZE;
            await replaceWindow(after, clamped === 1 ? "top" : "top");
        },
        [totalPages, jumpToEnd, replaceWindow],
    );

    const windowLabel = useMemo(() => {
        if (!visibleLines.length) {
            if (maxSeq > 0) {
                return `cleared · ${maxSeq.toLocaleString()} total`;
            }
            return null;
        }
        const from = visibleLines[0].seq;
        const to = visibleLines[visibleLines.length - 1].seq;
        const pageHint =
            currentPage != null ? ` · page ${currentPage}/${totalPages}` : "";
        const totalHint = maxSeq > to ? ` / ${maxSeq.toLocaleString()} total` : "";
        return `${visibleLines.length.toLocaleString()} lines (seq ${from.toLocaleString()}–${to.toLocaleString()})${totalHint}${pageHint}`;
    }, [visibleLines, maxSeq, currentPage, totalPages]);

    const atStart = !hasOlder && lines.length > 0;
    const atEnd = !hasNewer && lines.length > 0;

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
                    <Button
                        size="small"
                        onClick={() => void jumpToStart()}
                        disabled={loading || atStart}
                        style={{ marginRight: 4 }}
                        title={`First ~${PAGE_SIZE} lines`}
                    >
                        Start
                    </Button>
                    <Button
                        size="small"
                        onClick={() =>
                            currentPage != null && void jumpToPage(currentPage - 1)
                        }
                        disabled={loading || currentPage == null || currentPage <= 1}
                        style={{ marginRight: 4 }}
                        title={`Previous ~${PAGE_SIZE} lines`}
                    >
                        Prev
                    </Button>
                    <Button
                        size="small"
                        onClick={() =>
                            currentPage != null && void jumpToPage(currentPage + 1)
                        }
                        disabled={
                            loading || currentPage == null || currentPage >= totalPages
                        }
                        style={{ marginRight: 4 }}
                        title={`Next ~${PAGE_SIZE} lines`}
                    >
                        Next
                    </Button>
                    <Button
                        size="small"
                        onClick={() => void jumpToEnd()}
                        disabled={loading || atEnd}
                        style={{ marginRight: 8 }}
                        title={`Last ~${PAGE_SIZE} lines`}
                    >
                        End
                    </Button>
                    <Button
                        size="small"
                        onClick={() => void jumpToEnd()}
                        style={{ marginRight: 8 }}
                        title="Reload the latest page"
                    >
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
                    <Button
                        size="small"
                        onClick={clearLogs}
                        disabled={!lines.length}
                        title="Clear the view; keep cursor so Refresh only loads new lines"
                    >
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
            {loading && visibleLines.length === 0 ? (
                <Text type="secondary">Loading logs…</Text>
            ) : visibleLines.length === 0 ? (
                <Text type="secondary">
                    {stepFilter && lines.length > 0
                        ? "No log lines mention this step."
                        : maxSeq > 0
                          ? "View cleared. Refresh/End to reload the latest page, or Start for the beginning."
                          : "No log lines yet."}
                </Text>
            ) : (
                <pre ref={containerRef} onScroll={onScroll} className="run-logs-viewer">
                    {hasOlder && (
                        <div className="run-log-line run-log-info" style={{ opacity: 0.7 }}>
                            {loadingOlder
                                ? "Loading older logs…"
                                : "Scroll up for older · or use Start / Prev"}
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
                    {hasNewer && (
                        <div className="run-log-line run-log-info" style={{ opacity: 0.7 }}>
                            {loadingNewer
                                ? "Loading newer logs…"
                                : "Scroll down for newer · or use Next / End"}
                        </div>
                    )}
                </pre>
            )}
        </Card>
    );
}
