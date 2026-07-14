import React from "react";
import { Button, Input, Select, Tabs, message } from "antd";
import { opsApi, exportCsv } from "../../../api/ops";
import type { SqlQueryResponse, SqlSchemaResponse } from "../../../types/ops";
import { ChartCard, EmptyState, PageHeader, SortableDataTable } from "../shared";

const DEFAULT_SQL = `SELECT
  detection_model_id,
  subset_id,
  calc__weighted_f1_score,
  calc__weighted_precision,
  calc__support
FROM datapipe_analytics.metrics_on_subset
ORDER BY calc__weighted_f1_score DESC
LIMIT 1000;`;

const SAVED_KEY = "datapipe_sql_saved_queries";

type SavedQuery = { id: string; name: string; sql: string; updatedAt: string };

function loadSaved(): SavedQuery[] {
    try {
        return JSON.parse(localStorage.getItem(SAVED_KEY) ?? "[]");
    } catch {
        return [];
    }
}

export function SqlStudioPage() {
    const [sql, setSql] = React.useState(DEFAULT_SQL);
    const [schema, setSchema] = React.useState<SqlSchemaResponse | null>(null);
    const [result, setResult] = React.useState<SqlQueryResponse | null>(null);
    const [chartType, setChartType] = React.useState<"line" | "bar" | "scatter">("line");
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);
    const [saved, setSaved] = React.useState<SavedQuery[]>(loadSaved);
    const [activeTab, setActiveTab] = React.useState("query");

    React.useEffect(() => {
        opsApi.getSqlSchema().then(setSchema).catch(() => undefined);
    }, []);

    const runQuery = () => {
        setLoading(true);
        setError(null);
        opsApi
            .runSqlQuery({ sql, limit: 1000, datasource: "datapipe_analytics" })
            .then((res) => { setResult(res); setActiveTab("results"); })
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    };

    const saveQuery = () => {
        const entry: SavedQuery = { id: String(Date.now()), name: `Query ${saved.length + 1}`, sql, updatedAt: new Date().toISOString() };
        const next = [entry, ...saved].slice(0, 20);
        setSaved(next);
        localStorage.setItem(SAVED_KEY, JSON.stringify(next));
        message.success("Query saved");
    };

    const columns = result?.columns.map((c) => ({
        title: c.name,
        dataIndex: c.name,
        key: c.name,
        sorter: (a: Record<string, unknown>, b: Record<string, unknown>) => String(a[c.name]).localeCompare(String(b[c.name])),
    })) ?? [];

    const chartSpec = React.useMemo(() => {
        if (!result?.rows.length) return null;
        const cols = result.columns.map((c) => c.name);
        const numCol = cols.find((c) => typeof result.rows[0][c] === "number") ?? cols[4];
        const xCol = cols.find((c) => c.includes("date") || c.includes("run")) ?? cols[0];
        return {
            id: "sql-viz",
            title: "Query visualization",
            type: chartType,
            xLabel: xCol,
            yLabel: numCol ?? "value",
            series: [{
                key: "main",
                label: numCol ?? "value",
                points: result.rows.slice(0, 100).map((r) => ({ x: String(r[xCol] ?? ""), y: typeof r[numCol!] === "number" ? r[numCol!] as number : null })),
            }],
        };
    }, [result, chartType]);

    return (
        <div className="ops-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "SQL Studio" }]}
                title="SQL Studio"
                statusChips={[{ label: "Running", variant: "success" }]}
            />

            <Tabs activeKey={activeTab} onChange={setActiveTab}>
                <Tabs.TabPane tab="Query" key="query" />
                <Tabs.TabPane tab={`Results${result ? ` (${result.rows.length})` : ""}`} key="results" />
                <Tabs.TabPane tab="Visualization" key="viz" />
                <Tabs.TabPane tab="Saved Queries" key="saved" />
            </Tabs>

            <EmptyState loading={loading} error={error}>
                <div className="ops-sql-layout">
                    <div style={{ gridColumn: "1 / -1" }}>
                        <div style={{ display: "flex", gap: 8, marginBottom: 8, alignItems: "center" }}>
                            <Select defaultValue="datapipe_analytics" style={{ width: 180 }} options={[{ label: "datapipe_analytics", value: "datapipe_analytics" }]} />
                            <Select value={chartType} onChange={setChartType} style={{ width: 120 }} options={[{ label: "Line", value: "line" }, { label: "Bar", value: "bar" }, { label: "Scatter", value: "scatter" }]} />
                            <Button onClick={saveQuery}>Save</Button>
                            <Button type="primary" onClick={runQuery}>Run query</Button>
                        </div>
                        <div className="ops-sql-editor">
                            <Input.TextArea
                                value={sql}
                                onChange={(e) => setSql(e.target.value)}
                                autoSize={{ minRows: 10, maxRows: 20 }}
                                style={{ fontFamily: "monospace", fontSize: 13 }}
                            />
                        </div>
                    </div>

                    <div className="ops-sql-bottom" style={{ gridColumn: "1 / 2" }}>
                        {activeTab === "results" && result && (
                            <SortableDataTable
                                title={`Results (${result.rows.length} rows · ${result.execution_ms}ms)`}
                                columns={columns}
                                dataSource={result.rows.map((r, i) => ({ ...r, _rowId: i }))}
                                rowKey="_rowId"
                                total={result.total ?? result.rows.length}
                                page={1}
                                pageSize={25}
                                onPageChange={() => undefined}
                                extra={<Button size="small" onClick={() => exportCsv(result.columns.map((c) => c.name), result.rows)}>Export CSV</Button>}
                            />
                        )}
                        {activeTab === "viz" && chartSpec && <ChartCard spec={chartSpec} height={280} />}
                        {activeTab === "saved" && (
                            <div className="ops-panel">
                                {saved.map((q) => (
                                    <div key={q.id} style={{ padding: "8px 0", borderBottom: "1px solid var(--dp-gray-100)", cursor: "pointer" }} onClick={() => setSql(q.sql)}>
                                        <strong>{q.name}</strong>
                                        <div style={{ fontSize: 11, color: "var(--dp-gray-500)" }}>Updated {new Date(q.updatedAt).toLocaleDateString()}</div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    <div className="ops-panel" style={{ gridColumn: "2 / 3", gridRow: "1 / -1" }}>
                        <div className="ops-panel-title">Schema</div>
                        {schema?.tables.map((t) => (
                            <div key={t.name} style={{ marginBottom: 12, fontSize: 12 }}>
                                <strong>{t.name}</strong> ({t.row_count ?? "?"} rows)
                                <div style={{ color: "var(--dp-gray-500)" }}>{t.columns.map((c) => c.name).join(", ")}</div>
                            </div>
                        ))}
                    </div>
                </div>
            </EmptyState>

            {result && (
                <div style={{ marginTop: 8, fontSize: 12, color: "var(--dp-gray-500)" }}>
                    Query executed successfully · {result.execution_ms}ms · Ctrl+Enter to run
                </div>
            )}
        </div>
    );
}
