import React from "react";
import {
    BarChartOutlined,
    ApartmentOutlined,
    DashboardOutlined,
    ExperimentOutlined,
    HistoryOutlined,
    QuestionCircleOutlined,
    ReloadOutlined,
    TableOutlined,
    DatabaseOutlined,
} from "@ant-design/icons";
import { Button, InputNumber, Popover, Slider } from "antd";
import { Link, Outlet, useLocation } from "react-router-dom";
import { opsApi } from "../api/ops";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { loadLastGraphStage } from "../features/cy/graphSessionState";
import { useResizableWidth } from "../hooks/useResizableWidth";
import type { OpsSpecSummary } from "../types/opsSpecs";

type NavItem = {
    key: string;
    href: string;
    label: string;
    icon: React.ReactNode;
};

function readRefreshSeconds(): number {
    const stored = localStorage.getItem("datapipe_ops_refresh_s");
    const seconds = stored ? parseInt(stored, 10) : 30;
    return Number.isFinite(seconds) ? seconds : 30;
}

function SidebarRefreshControl({ collapsed }: { collapsed: boolean }) {
    const [refreshSeconds, setRefreshSeconds] = React.useState(readRefreshSeconds);

    const onRefreshChange = (value: number) => {
        const next = value || 30;
        setRefreshSeconds(next);
        localStorage.setItem("datapipe_ops_refresh_s", String(next));
    };

    const popover = (
        <div className="ops-sidebar-refresh-popover">
            <div className="ops-sidebar-refresh-label">Refresh interval (seconds)</div>
            <Slider min={15} max={120} value={refreshSeconds} onChange={onRefreshChange} />
            <InputNumber
                min={15}
                max={120}
                value={refreshSeconds}
                onChange={(value) => onRefreshChange(value ?? 30)}
                style={{ width: "100%" }}
            />
        </div>
    );

    return (
        <Popover content={popover} trigger="click" placement="topLeft">
            <button type="button" className="datapipe-sidebar-item datapipe-sidebar-refresh">
                <span className="sidebar-icon">
                    <ReloadOutlined />
                </span>
                {!collapsed && `Refresh (${refreshSeconds}s)`}
            </button>
        </Popover>
    );
}

function matchNav(pathname: string, href: string): boolean {
    const hrefPath = href.split("?")[0] ?? href;
    if (hrefPath === "/runs") return pathname === "/runs" || pathname.startsWith("/runs/");
    if (hrefPath === "/") {
        return pathname === "/" || (pathname.startsWith("/pipelines/") && !pathname.includes("/metrics") && !pathname.includes("/classes") && !pathname.includes("/training"));
    }
    if (hrefPath === "/graph") return pathname.startsWith("/graph");
    return pathname === hrefPath || pathname.startsWith(`${hrefPath}/`);
}

export function OpsShell() {
    const location = useLocation();
    const [title, setTitle] = React.useState("Datapipe Ops");
    const [agentMode, setAgentMode] = React.useState(false);
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);
    const [collapsed, setCollapsed] = React.useState(false);
    const [capabilitiesError, setCapabilitiesError] = React.useState<unknown>(null);
    const [opsSpecs, setOpsSpecs] = React.useState<OpsSpecSummary[]>([]);
    const {
        width: sidebarWidth,
        onHandleMouseDown: onSidebarResize,
    } = useResizableWidth({
        initial: 280,
        min: 200,
        max: 480,
        storageKey: "dp.sidebarWidth",
        edge: "right",
    });

    React.useEffect(() => {
        opsApi.getCapabilities().then((c) => {
            setAgentMode(c.mode === "agent");
            setPipelineId(c.pipeline_id ?? null);
            setCapabilitiesError(null);
            setTitle(
                c.mode === "central"
                    ? "Datapipe Central Dashboard"
                    : `Datapipe Ops · ${c.pipeline_id || "agent"}`,
            );
            if (c.pipeline_id) {
                opsApi.getOpsSpecs(c.pipeline_id)
                    .then((res) => setOpsSpecs(res.specs))
                    .catch(() => setOpsSpecs([]));
            } else {
                setOpsSpecs([]);
            }
        }).catch((e) => {
            setCapabilitiesError(e);
        });
    }, []);

    const lastGraphStage = loadLastGraphStage();
    const graphHref = lastGraphStage
        ? `/graph?stage=${encodeURIComponent(lastGraphStage)}`
        : "/graph";

    const hasExplicitSpecs = opsSpecs.length > 0;
    const primaryItems: NavItem[] = [
        { key: "/", href: "/", label: "Overview", icon: <DashboardOutlined /> },
        ...(agentMode
            ? [{ key: "/graph", href: graphHref, label: "Graph", icon: <ApartmentOutlined /> }]
            : []),
        { key: "/runs", href: "/runs", label: "Runs", icon: <HistoryOutlined /> },
        ...(hasExplicitSpecs
            ? []
            : [
                { key: "/training", href: "/training", label: "Training", icon: <ExperimentOutlined /> },
                { key: "/metrics", href: "/metrics", label: "Metrics", icon: <BarChartOutlined /> },
                { key: "/classes", href: "/classes", label: "Class Metrics", icon: <TableOutlined /> },
            ]),
    ];

    const secondaryItems: NavItem[] = [
        { key: "/help", href: "/help", label: "Help", icon: <QuestionCircleOutlined /> },
    ];

    const allItems = [...primaryItems, ...secondaryItems];
    const selected =
        allItems.find((item) => matchNav(location.pathname, item.href))?.key ?? "/";

    const isGraph = location.pathname.startsWith("/graph");
    const isObsPage = ["/frozen-datasets", "/metrics", "/classes", "/class-metrics", "/training"].some((p) =>
        location.pathname.startsWith(p),
    );

    const renderItem = (item: NavItem) => (
        <Link
            key={item.key}
            to={item.href}
            className={`datapipe-sidebar-item${selected === item.key ? " active" : ""}`}
        >
            <span className="sidebar-icon">{item.icon}</span>
            {!collapsed && item.label}
        </Link>
    );

    const renderSpecGroup = (
        href: string,
        label: string,
        icon: React.ReactNode,
        enabled: (spec: OpsSpecSummary) => boolean,
    ) => {
        const specs = opsSpecs.filter(enabled);
        if (!specs.length) return null;
        return (
            <div className="ops-sidebar-section" key={href}>
                <Link
                    to={href}
                    className={`datapipe-sidebar-item${matchNav(location.pathname, href) ? " active" : ""}`}
                >
                    <span className="sidebar-icon">{icon}</span>
                    {!collapsed && label}
                </Link>
                {!collapsed && specs.map((spec) => (
                    <Link
                        key={`${href}/${spec.id}`}
                        to={`${href}/${spec.id}`}
                        className={`datapipe-sidebar-item ops-sidebar-nested${location.pathname === `${href}/${spec.id}` ? " active" : ""}`}
                    >
                        {spec.title}
                    </Link>
                ))}
            </div>
        );
    };

    return (
        <div className="datapipe-shell" style={{ display: "flex", minHeight: "var(--dp-vh)" }}>
            <aside
                className={`datapipe-sidebar${collapsed ? " collapsed" : ""}`}
                style={
                    {
                        display: "flex",
                        flexDirection: "column",
                        ...(collapsed ? {} : { ["--dp-sidebar-width" as string]: `${sidebarWidth}px` }),
                    } as React.CSSProperties
                }
            >
                {!collapsed && (
                    <div
                        className="dp-resize-handle dp-resize-handle-right"
                        role="separator"
                        aria-orientation="vertical"
                        onMouseDown={onSidebarResize}
                    />
                )}
                <div className="datapipe-sidebar-logo">Datapipe Ops</div>
                <nav className="datapipe-sidebar-nav">
                    {primaryItems.map(renderItem)}
                    {hasExplicitSpecs && renderSpecGroup("/frozen-datasets", "Frozen Datasets", <DatabaseOutlined />, (spec) => spec.has_frozen_datasets)}
                    {hasExplicitSpecs && renderSpecGroup("/training", "Training", <ExperimentOutlined />, (spec) => spec.has_training)}
                    {hasExplicitSpecs && renderSpecGroup("/metrics", "Metrics", <BarChartOutlined />, (spec) => spec.metric_tables_count > 0)}
                    {hasExplicitSpecs && renderSpecGroup("/class-metrics", "Class Metrics", <TableOutlined />, (spec) => spec.class_metric_tables_count > 0)}
                </nav>
                <div className="ops-sidebar-section">
                    {!collapsed && <div className="ops-sidebar-section-label">More</div>}
                    {secondaryItems.map(renderItem)}
                </div>
                <div className="datapipe-sidebar-footer">
                    {!collapsed && pipelineId && (
                        <div style={{ padding: "0 12px 8px", fontSize: 12, color: "var(--dp-sidebar-text-muted)" }}>
                            Production · {pipelineId}
                        </div>
                    )}
                    <SidebarRefreshControl collapsed={collapsed} />
                    <Button type="text" style={{ color: "var(--dp-sidebar-text)", width: "100%" }} onClick={() => setCollapsed(!collapsed)}>
                        {collapsed ? "→" : "Collapse"}
                    </Button>
                </div>
            </aside>
            <div className="datapipe-main" style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
                <ConnectivityBanner />
                {capabilitiesError ? (
                    <ApiErrorAlert error={capabilitiesError} style={{ borderRadius: 0 }} />
                ) : null}
                {!isObsPage && (
                    <header className="datapipe-header">
                        <h1 className="datapipe-title">{title}</h1>
                        {agentMode && pipelineId && (
                            <div className="run-status-pill">Running · {pipelineId}</div>
                        )}
                    </header>
                )}
                <main className={`datapipe-content${isGraph ? " datapipe-content-graph" : " datapipe-content-padded"}`}>
                    <ErrorBoundary key={location.pathname}>
                        <Outlet />
                    </ErrorBoundary>
                </main>
            </div>
        </div>
    );
}
