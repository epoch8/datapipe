import React from "react";
import {
    AlertOutlined,
    BarChartOutlined,
    ApartmentOutlined,
    DashboardOutlined,
    DatabaseOutlined,
    ExperimentOutlined,
    FolderOutlined,
    HistoryOutlined,
    QuestionCircleOutlined,
    SettingOutlined,
    TableOutlined,
} from "@ant-design/icons";
import { Badge, Button } from "antd";
import { Link, Outlet, useLocation } from "react-router-dom";
import { opsApi } from "../api/ops";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { loadLastGraphStage } from "../features/cy/graphSessionState";
import { useResizableWidth } from "../hooks/useResizableWidth";

type NavItem = {
    key: string;
    href: string;
    label: string;
    icon: React.ReactNode;
    badge?: number;
    disabled?: boolean;
};

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
        }).catch((e) => {
            setCapabilitiesError(e);
        });
    }, []);

    const lastGraphStage = loadLastGraphStage();
    const graphHref = lastGraphStage
        ? `/graph?stage=${encodeURIComponent(lastGraphStage)}`
        : "/graph";

    const primaryItems: NavItem[] = [
        { key: "/", href: "/", label: "Overview", icon: <DashboardOutlined /> },
        { key: "/runs", href: "/runs", label: "Runs", icon: <HistoryOutlined /> },
        { key: "/metrics", href: "/metrics", label: "Metrics", icon: <BarChartOutlined /> },
        { key: "/classes", href: "/classes", label: "Classes", icon: <TableOutlined /> },
        { key: "/training", href: "/training", label: "Training", icon: <ExperimentOutlined /> },
        { key: "/sql-studio", href: "/sql-studio", label: "SQL Studio", icon: <DatabaseOutlined /> },
        ...(agentMode
            ? [{ key: "/graph", href: graphHref, label: "Graph", icon: <ApartmentOutlined /> }]
            : []),
    ];

    const secondaryItems: NavItem[] = [
        { key: "/alerts", href: "/alerts", label: "Alerts", icon: <AlertOutlined />, badge: 3, disabled: true },
        { key: "/artifacts", href: "/artifacts", label: "Artifacts", icon: <FolderOutlined />, disabled: true },
        { key: "/data", href: "/data", label: "Data", icon: <DatabaseOutlined />, disabled: true },
        { key: "/settings", href: "/settings", label: "Settings", icon: <SettingOutlined /> },
        { key: "/help", href: "/help", label: "Help", icon: <QuestionCircleOutlined /> },
    ];

    const allItems = [...primaryItems, ...secondaryItems];
    const selected =
        allItems.find((item) => matchNav(location.pathname, item.href))?.key ?? "/";

    const isGraph = location.pathname.startsWith("/graph");
    const isObsPage = ["/metrics", "/classes", "/training", "/sql-studio"].some((p) =>
        location.pathname.startsWith(p),
    );

    const renderItem = (item: NavItem) => {
        if (item.disabled) {
            return (
                <div key={item.key} className="datapipe-sidebar-item disabled" style={{ opacity: 0.5, cursor: "not-allowed" }}>
                    <span className="sidebar-icon">{item.icon}</span>
                    {!collapsed && (
                        <>
                            {item.label}
                            {item.badge != null && <Badge count={item.badge} style={{ marginLeft: "auto" }} />}
                        </>
                    )}
                </div>
            );
        }
        return (
            <Link
                key={item.key}
                to={item.href}
                className={`datapipe-sidebar-item${selected === item.key ? " active" : ""}`}
            >
                <span className="sidebar-icon">{item.icon}</span>
                {!collapsed && (
                    <>
                        {item.label}
                        {item.badge != null && <Badge count={item.badge} style={{ marginLeft: "auto" }} />}
                    </>
                )}
            </Link>
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
