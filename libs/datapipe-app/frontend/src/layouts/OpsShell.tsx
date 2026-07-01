import React from "react";
import {
    BugOutlined,
    DashboardOutlined,
    LineChartOutlined,
    QuestionCircleOutlined,
    SettingOutlined,
} from "@ant-design/icons";
import { Link, Outlet, useLocation } from "react-router-dom";
import { opsApi } from "../api/ops";
import { ErrorBoundary } from "../components/ErrorBoundary";

type NavItem = {
    key: string;
    href: string;
    label: string;
    icon: React.ReactNode;
};

export function OpsShell() {
    const location = useLocation();
    const [title, setTitle] = React.useState("Datapipe Ops");
    const [showMetrics, setShowMetrics] = React.useState(true);
    const [agentMode, setAgentMode] = React.useState(false);
    const [pipelineId, setPipelineId] = React.useState<string | null>(null);

    React.useEffect(() => {
        opsApi.getCapabilities().then((c) => {
            setShowMetrics(c.ml_metrics || c.ml_training);
            setAgentMode(c.mode === "agent");
            setPipelineId(c.pipeline_id ?? null);
            setTitle(
                c.mode === "central"
                    ? "Datapipe Central Dashboard"
                    : `Datapipe Ops · ${c.pipeline_id || "agent"}`,
            );
        }).catch(() => undefined);
    }, []);

    const selected = location.pathname.startsWith("/metrics")
        ? "/metrics"
        : location.pathname.startsWith("/debug")
          ? "/debug"
          : location.pathname.startsWith("/help")
            ? "/help"
            : location.pathname.startsWith("/settings")
              ? "/settings"
              : "/";

    const items: NavItem[] = [
        { key: "/", href: "/", label: "Overview", icon: <DashboardOutlined /> },
        ...(showMetrics
            ? [{ key: "/metrics", href: "/metrics", label: "Metrics", icon: <LineChartOutlined /> }]
            : []),
        ...(agentMode
            ? [{ key: "/debug", href: "/debug", label: "Debug", icon: <BugOutlined /> }]
            : []),
        { key: "/help", href: "/help", label: "Help", icon: <QuestionCircleOutlined /> },
        { key: "/settings", href: "/settings", label: "Settings", icon: <SettingOutlined /> },
    ];

    const isDebug = location.pathname.startsWith("/debug");

    return (
        <div className="datapipe-shell" style={{ display: "flex", minHeight: "100vh" }}>
            <aside className="datapipe-sidebar">
                <div className="datapipe-sidebar-logo">Datapipe Ops</div>
                <nav className="datapipe-sidebar-nav">
                    {items.map((item) => (
                        <Link
                            key={item.key}
                            to={item.href}
                            className={`datapipe-sidebar-item${selected === item.key ? " active" : ""}`}
                        >
                            <span className="sidebar-icon">{item.icon}</span>
                            {item.label}
                        </Link>
                    ))}
                </nav>
            </aside>
            <div className="datapipe-main" style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
                <header className="datapipe-header">
                    <h1 className="datapipe-title">{title}</h1>
                    {agentMode && pipelineId && (
                        <div className="run-status-pill">Running · {pipelineId}</div>
                    )}
                </header>
                <main className={`datapipe-content${isDebug ? "" : " datapipe-content-padded"}`}>
                    <ErrorBoundary key={location.pathname}>
                        <Outlet />
                    </ErrorBoundary>
                </main>
            </div>
        </div>
    );
}
