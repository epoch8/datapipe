import React from "react";
import {
    ApartmentOutlined,
    DashboardOutlined,
    QuestionCircleOutlined,
    ReloadOutlined,
    SettingOutlined,
} from "@ant-design/icons";
import { Button, InputNumber, Popover, Slider } from "antd";
import { Link, Outlet, useLocation } from "react-router-dom";
import { opsApi } from "../api/client";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { useResizableWidth } from "../hooks/useResizableWidth";
import {
    collectLegacyNavItems,
    getObsPagePrefixes,
    renderPluginNavSections,
    type OpsSpecSummary,
} from "../plugins/registry";
import type { Capabilities } from "../types/ops";

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
    if (hrefPath === "/") return pathname === "/";
    if (hrefPath === "/graph") {
        return (
            pathname.startsWith("/graph") ||
            pathname.startsWith("/tables/") ||
            pathname.startsWith("/transforms/") ||
            pathname.startsWith("/meta-steps/")
        );
    }
    return pathname === hrefPath || pathname.startsWith(`${hrefPath}/`);
}

export function OpsShell() {
    const location = useLocation();
    const [title, setTitle] = React.useState("Datapipe Ops");
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(null);
    const [collapsed, setCollapsed] = React.useState(false);
    const [capabilitiesError, setCapabilitiesError] = React.useState<unknown>(null);
    const [opsSpecs] = React.useState<OpsSpecSummary[]>([]);
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
        opsApi
            .getCapabilities()
            .then((c) => {
                setCapabilities(c);
                setCapabilitiesError(null);
                const addonNames = (c.addons ?? []).map((a) => a.name).filter(Boolean);
                setTitle(addonNames.length ? `Datapipe Ops · ${addonNames.join(", ")}` : "Datapipe Ops");
            })
            .catch((e) => {
                setCapabilitiesError(e);
            });
    }, []);

    const hasExplicitSpecs = opsSpecs.length > 0;
    const legacyPluginItems = collectLegacyNavItems({ hasExplicitSpecs, capabilities }).map((item) => ({
        ...item,
        icon: <ApartmentOutlined />,
    }));
    const primaryItems: NavItem[] = [
        { key: "/", href: "/", label: "General", icon: <DashboardOutlined /> },
        { key: "/graph", href: "/graph", label: "Graph", icon: <ApartmentOutlined /> },
        ...legacyPluginItems,
    ];

    const secondaryItems: NavItem[] = [
        { key: "/settings", href: "/settings", label: "Settings", icon: <SettingOutlined /> },
        { key: "/help", href: "/help", label: "Help", icon: <QuestionCircleOutlined /> },
    ];

    const allItems = [...primaryItems, ...secondaryItems];
    const selected =
        allItems.find((item) => matchNav(location.pathname, item.href))?.key ?? "/";

    const isGraphRoute = location.pathname.startsWith("/graph");
    const isObsPage = getObsPagePrefixes().some((p) => location.pathname.startsWith(p));

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
                    {hasExplicitSpecs &&
                        renderPluginNavSections({
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
                    </header>
                )}
                <main className={`datapipe-content${isGraphRoute ? " datapipe-content-graph" : " datapipe-content-padded"}`}>
                    <ErrorBoundary key={location.pathname}>
                        <Outlet />
                    </ErrorBoundary>
                </main>
            </div>
        </div>
    );
}
