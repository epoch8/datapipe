import React from "react";
import {
    ApartmentOutlined,
    DashboardOutlined,
    HistoryOutlined,
    MoonOutlined,
    SunOutlined,
} from "@ant-design/icons";
import { Button, Space } from "antd";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { opsApi } from "../api/client";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { useDatapipeUiConfig } from "../context/DatapipeUiContext";
import type { Capabilities } from "../types/ops";
import { applyUiTheme, readStoredUiTheme } from "@datapipe/ui-core";
import { LocalChromeActionsProvider } from "./LocalChromeActions";
import "./localShell.css";

function matchTab(pathname: string): "overview" | "graph" | "runs" {
    if (pathname.startsWith("/runs")) return "runs";
    if (
        pathname.startsWith("/graph") ||
        pathname.startsWith("/tables/") ||
        pathname.startsWith("/transforms/") ||
        pathname.startsWith("/meta-steps/")
    ) {
        return "graph";
    }
    return "overview";
}

/**
 * Local host chrome: left sidebar nav (Overview / Graph / Runs) + shared screens.
 */
export function LocalShell() {
    const location = useLocation();
    const navigate = useNavigate();
    const config = useDatapipeUiConfig();
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(config.capabilities);
    const [capabilitiesError, setCapabilitiesError] = React.useState<unknown>(null);
    const [theme, setTheme] = React.useState<"light" | "dark">(config.theme);
    const [starting, setStarting] = React.useState(false);
    const tab = matchTab(location.pathname);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((c) => {
                setCapabilities(c);
                setCapabilitiesError(null);
            })
            .catch((e) => setCapabilitiesError(e));
    }, [location.pathname]);

    const canStart = Boolean(
        (capabilities?.run_start ?? false) && (config.permissions?.canStartRun ?? true),
    );

    const onToggleTheme = () => {
        const next = theme === "dark" ? "light" : "dark";
        setTheme(next);
        applyUiTheme(next);
    };

    const onRefreshPage = React.useCallback(() => {
        navigate(0);
    }, [navigate]);

    const onRunSteps = React.useCallback(() => {
        if (!canStart) return;
        setStarting(true);
        opsApi
            .startRun([])
            .then((started) => navigate(config.linkTo(`runs/${started.run_id}`)))
            .catch((e) => setCapabilitiesError(e))
            .finally(() => setStarting(false));
    }, [canStart, config, navigate]);

    const chromeActions = React.useMemo(
        () => ({ onRefreshPage, onRunSteps, canStart, starting }),
        [onRefreshPage, onRunSteps, canStart, starting],
    );

    const navClass = (active: boolean) => `dp-local-nav-link${active ? " active" : ""}`;

    return (
        <div className="dp-local-shell" data-ui-theme={theme} data-theme={theme}>
            <ConnectivityBanner />
            <aside className="dp-local-sidebar" aria-label="Pipeline navigation">
                <div className="dp-local-sidebar-brand">Datapipe Ops</div>
                <nav className="dp-local-nav" aria-label="Pipeline sections">
                    <NavLink
                        to={config.linkTo("")}
                        end
                        className={() => navClass(tab === "overview")}
                    >
                        <DashboardOutlined aria-hidden />
                        <span>Overview</span>
                    </NavLink>
                    <NavLink
                        to={config.linkTo("graph")}
                        className={() => navClass(tab === "graph")}
                    >
                        <ApartmentOutlined aria-hidden />
                        <span>Graph</span>
                    </NavLink>
                    <NavLink
                        to={config.linkTo("runs")}
                        className={() => navClass(tab === "runs")}
                    >
                        <HistoryOutlined aria-hidden />
                        <span>Runs</span>
                    </NavLink>
                </nav>
                <div className="dp-local-sidebar-footer">
                    <Button
                        type="text"
                        className="dp-local-theme-toggle"
                        icon={theme === "dark" ? <SunOutlined /> : <MoonOutlined />}
                        onClick={onToggleTheme}
                        aria-label={theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}
                        title={theme === "dark" ? "Светлая тема" : "Тёмная тема"}
                    />
                    <Space size={0} className="dp-local-lang">
                        <Button size="small" type="link">
                            RU
                        </Button>
                        <Button size="small" type="link">
                            EN
                        </Button>
                    </Space>
                </div>
            </aside>

            <div className="dp-local-body">
                {capabilitiesError ? <ApiErrorAlert error={capabilitiesError} /> : null}
                <LocalChromeActionsProvider value={chromeActions}>
                    <main className="dp-local-main">
                        <ErrorBoundary key={location.pathname}>
                            <Outlet />
                        </ErrorBoundary>
                    </main>
                </LocalChromeActionsProvider>
            </div>
        </div>
    );
}

export function bootstrapLocalTheme(): "light" | "dark" {
    const theme = readStoredUiTheme("dark");
    applyUiTheme(theme);
    return theme;
}
