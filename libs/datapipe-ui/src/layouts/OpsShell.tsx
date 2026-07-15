import React from "react";
import {
    ApartmentOutlined,
    BarChartOutlined,
    DashboardOutlined,
    ExperimentOutlined,
    HistoryOutlined,
    PieChartOutlined,
    QuestionCircleOutlined,
    ReloadOutlined,
} from "@ant-design/icons";
import { Button, InputNumber, Popover, Slider } from "antd";
import { Link, Outlet, useLocation } from "react-router-dom";
import { opsApi } from "../api/client";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { loadLastGraphStage } from "../features/cy/graphSessionState";
import { useResizableWidth } from "../hooks/useResizableWidth";
import { getUiMlPlugin } from "../plugins/registry";
import type { Capabilities } from "../types/ops";
import type { OpsSpecSummary } from "@datapipe/ui-ml/types/opsSpecs";

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
    const mlPlugin = getUiMlPlugin();
    const [title, setTitle] = React.useState("Datapipe Ops");
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(null);
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
            setCapabilities(c);
            setPipelineId(c.pipeline_id ?? null);
            setCapabilitiesError(null);
            setTitle(`Datapipe Ops · ${c.pipeline_id || "agent"}`);
            if (c.pipeline_id && "getOpsSpecs" in opsApi) {
                (opsApi as typeof opsApi & { getOpsSpecs: (id: string) => Promise<{ specs: OpsSpecSummary[] }> })
                    .getOpsSpecs(c.pipeline_id)
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
    const legacyIconFor = (href: string): React.ReactNode => {
        if (href.startsWith("/training")) return <ExperimentOutlined />;
        if (href.startsWith("/metrics")) return <BarChartOutlined />;
        if (href.startsWith("/classes")) return <PieChartOutlined />;
        return <HistoryOutlined />;
    };
    const legacyMlItems = mlPlugin.renderLegacyNavItems({ hasExplicitSpecs, capabilities }).map((item) => ({
        ...item,
        icon: legacyIconFor(item.href),
    }));
    const primaryItems: NavItem[] = [
        { key: "/", href: "/", label: "Overview", icon: <DashboardOutlined /> },
        { key: "/graph", href: graphHref, label: "Graph", icon: <ApartmentOutlined /> },
        { key: "/runs", href: "/runs", label: "Runs", icon: <HistoryOutlined /> },
        ...legacyMlItems,
    ];

    const secondaryItems: NavItem[] = [
        { key: "/help", href: "/help", label: "Help", icon: <QuestionCircleOutlined /> },
    ];

    const allItems = [...primaryItems, ...secondaryItems];
    const selected =
        allItems.find((item) => matchNav(location.pathname, item.href))?.key ?? "/";

    const isGraph = location.pathname.startsWith("/graph");
    const isObsPage = mlPlugin.obsPagePrefixes.some((p) => location.pathname.startsWith(p));

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
                    {hasExplicitSpecs && mlPlugin.renderNavSections({
                        specs: opsSpecs,
                        collapsed,
                        pathname: location.pathname,
                        capabilities,
                    })}
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
                        {pipelineId && (
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
