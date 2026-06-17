import React from "react";
import { Layout, Menu, Typography } from "antd";
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

const { Header, Sider, Content } = Layout;
const { Title } = Typography;

export function OpsShell() {
    const location = useLocation();
    const [title, setTitle] = React.useState("Datapipe Ops");
    const [showMetrics, setShowMetrics] = React.useState(true);
    const [agentMode, setAgentMode] = React.useState(false);

    React.useEffect(() => {
        opsApi.getCapabilities().then((c) => {
            setShowMetrics(c.ml_metrics || c.ml_training);
            setAgentMode(c.mode === "agent");
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

    const items = [
        { key: "/", icon: <DashboardOutlined />, label: <Link to="/">Overview</Link> },
        ...(showMetrics
            ? [{ key: "/metrics", icon: <LineChartOutlined />, label: <Link to="/metrics">Metrics</Link> }]
            : []),
        ...(agentMode
            ? [{ key: "/debug", icon: <BugOutlined />, label: <Link to="/debug">Debug</Link> }]
            : []),
        { key: "/help", icon: <QuestionCircleOutlined />, label: <Link to="/help">Help</Link> },
        { key: "/settings", icon: <SettingOutlined />, label: <Link to="/settings">Settings</Link> },
    ];

    return (
        <Layout style={{ minHeight: "100vh" }}>
            <Sider width={200} theme="light" style={{ borderRight: "1px solid #f0f0f0" }}>
                <div style={{ padding: 16, fontWeight: 600 }}>Datapipe Ops</div>
                <Menu mode="inline" selectedKeys={[selected]} items={items} />
            </Sider>
            <Layout>
                <Header style={{ background: "#fff", padding: "0 24px", borderBottom: "1px solid #f0f0f0" }}>
                    <Title level={4} style={{ margin: "16px 0" }}>
                        {title}
                    </Title>
                </Header>
                <Content style={{ padding: 24, overflow: "auto" }}>
                    <ErrorBoundary key={location.pathname}>
                        <Outlet />
                    </ErrorBoundary>
                </Content>
            </Layout>
        </Layout>
    );
}
