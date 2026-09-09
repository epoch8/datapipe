import React from "react";
import {
    HistoryOutlined,
    QuestionCircleOutlined,
    ReloadOutlined,
    BulbOutlined,
} from "@ant-design/icons";
import { Button, Space, Typography } from "antd";
import { Link, NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";
import { opsApi } from "../api/client";
import { ApiErrorAlert } from "../components/ApiErrorAlert";
import { ConnectivityBanner } from "../components/ConnectivityBanner";
import { ErrorBoundary } from "../components/ErrorBoundary";
import { useDatapipeUiConfig } from "../context/DatapipeUiContext";
import type { Capabilities } from "../types/ops";
import { applyUiTheme, readStoredUiTheme } from "@datapipe/ui-core";

const { Text } = Typography;

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
 * Local host chrome for datapipe-ui (mockups 8–9): Datapipe Ops / LOCAL,
 * API status, theme/lang/help, Overview/Graph/Runs tabs.
 */
export function LocalShell() {
    const location = useLocation();
    const navigate = useNavigate();
    const config = useDatapipeUiConfig();
    const [capabilities, setCapabilities] = React.useState<Capabilities | null>(config.capabilities);
    const [capabilitiesError, setCapabilitiesError] = React.useState<unknown>(null);
    const [apiHost, setApiHost] = React.useState("localhost");
    const [connected, setConnected] = React.useState(true);
    const [theme, setTheme] = React.useState<"light" | "dark">(config.theme);
    const [starting, setStarting] = React.useState(false);
    const tab = matchTab(location.pathname);

    React.useEffect(() => {
        if (typeof window !== "undefined") {
            setApiHost(window.location.host || "localhost");
        }
    }, []);

    React.useEffect(() => {
        opsApi
            .getCapabilities()
            .then((c) => {
                setCapabilities(c);
                setCapabilitiesError(null);
                setConnected(true);
            })
            .catch((e) => {
                setCapabilitiesError(e);
                setConnected(false);
            });
    }, [location.pathname]);

    const canStart = Boolean(
        (capabilities?.run_start ?? false) && (config.permissions?.canStartRun ?? true),
    );

    const onToggleTheme = () => {
        const next = theme === "dark" ? "light" : "dark";
        setTheme(next);
        applyUiTheme(next);
    };

    const onRefresh = () => {
        navigate(0);
    };

    const onRunSteps = () => {
        if (!canStart) return;
        setStarting(true);
        opsApi
            .startRun([])
            .then((started) => navigate(config.linkTo(`runs/${started.run_id}`)))
            .catch((e) => setCapabilitiesError(e))
            .finally(() => setStarting(false));
    };

    return (
        <div className="dp-local-shell" data-ui-theme={theme}>
            <ConnectivityBanner />
            <header className="dp-local-topbar">
                <div className="dp-local-brand">
                    <span className="dp-local-logo" aria-hidden>
                        ▣
                    </span>
                    <strong>Datapipe Ops</strong>
                    <span className="dp-badge-local">LOCAL</span>
                    <Text type="secondary" className="dp-local-pipeline-chip">
                        {config.pipelineName}
                    </Text>
                </div>
                <div className="dp-local-topbar-right">
                    <span className={`dp-api-status${connected ? " ok" : ""}`}>
                        <span className="dot" />
                        {connected ? "API подключён" : "API недоступен"} · {apiHost}
                    </span>
                    <Button
                        type="text"
                        icon={<BulbOutlined />}
                        onClick={onToggleTheme}
                        aria-label="Toggle theme"
                    />
                    <Space size={4} className="dp-lang-toggle">
                        <Button size="small" type="link">
                            RU
                        </Button>
                        <Button size="small" type="link">
                            EN
                        </Button>
                    </Space>
                    <Link to={config.linkTo("help")}>
                        <Button type="text" icon={<QuestionCircleOutlined />}>
                            Справка
                        </Button>
                    </Link>
                </div>
            </header>

            <div className="dp-local-subheader">
                <div>
                    <h1 className="dp-local-title">{config.pipelineName}</h1>
                    <Text type="secondary">Локальный пайплайн · {config.pipelineName}</Text>
                </div>
                <nav className="dp-local-tabs" aria-label="Pipeline sections">
                    <NavLink
                        to={config.linkTo("")}
                        end
                        className={({ isActive }) =>
                            `dp-local-tab${isActive || tab === "overview" ? " active" : ""}`
                        }
                    >
                        Overview
                    </NavLink>
                    <NavLink
                        to={config.linkTo("graph")}
                        className={({ isActive }) =>
                            `dp-local-tab${isActive || tab === "graph" ? " active" : ""}`
                        }
                    >
                        Graph
                    </NavLink>
                    <NavLink
                        to={config.linkTo("runs")}
                        className={({ isActive }) =>
                            `dp-local-tab${isActive || tab === "runs" ? " active" : ""}`
                        }
                    >
                        <HistoryOutlined /> Runs
                    </NavLink>
                </nav>
                <Space>
                    <Button icon={<ReloadOutlined />} onClick={onRefresh}>
                        Обновить
                    </Button>
                    <Button
                        type="primary"
                        className="dp-btn-primary"
                        disabled={!canStart}
                        loading={starting}
                        onClick={onRunSteps}
                    >
                        Запустить шаги
                    </Button>
                </Space>
            </div>

            {capabilitiesError ? <ApiErrorAlert error={capabilitiesError} /> : null}

            <main className="dp-local-main">
                <ErrorBoundary key={location.pathname}>
                    <Outlet />
                </ErrorBoundary>
            </main>
        </div>
    );
}

export function bootstrapLocalTheme(): "light" | "dark" {
    const theme = readStoredUiTheme("dark");
    applyUiTheme(theme);
    return theme;
}
