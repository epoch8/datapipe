import React from "react";
import { Alert, Button, Spin, Table, Tag } from "antd";
import { Link, useNavigate, useParams } from "react-router-dom";
import { opsApi } from "../../api/ops";

export function TrainingRuns() {
    const { id = "" } = useParams();
    const navigate = useNavigate();
    const [runs, setRuns] = React.useState<Record<string, unknown>[]>([]);
    const [selected, setSelected] = React.useState<string[]>([]);
    const [error, setError] = React.useState<string | null>(null);

    React.useEffect(() => {
        opsApi
            .getTrainingRuns(id)
            .then((r) => setRuns(r.runs))
            .catch((e) => setError(String(e)));
    }, [id]);

    if (error) return <Alert type="error" message={error} />;
    if (!runs && !error) return <Spin />;

    return (
        <div>
            <Link to={`/pipelines/${id}`}>← Pipeline detail</Link>
            <div style={{ margin: "16px 0" }}>
                <Button
                    disabled={selected.length < 2}
                    onClick={() =>
                        navigate(`/training/compare?run_keys=${selected.join(",")}`)
                    }
                >
                    Compare selected ({selected.length})
                </Button>
            </div>
            <Table
                rowKey={(r) => String(r.run_key)}
                dataSource={runs}
                rowSelection={{
                    selectedRowKeys: selected,
                    onChange: (keys) => setSelected(keys as string[]),
                }}
                columns={[
                    {
                        title: "Run key",
                        dataIndex: "run_key",
                        render: (v) => (
                            <Button type="link" onClick={() => navigate(`/training/${encodeURIComponent(String(v))}`)}>
                                {String(v).slice(0, 24)}…
                            </Button>
                        ),
                    },
                    { title: "Model", dataIndex: "model_id" },
                    {
                        title: "Status",
                        dataIndex: "status",
                        render: (v) => <Tag>{String(v)}</Tag>,
                    },
                    { title: "Attempt", dataIndex: "attempt" },
                    {
                        title: "Best",
                        dataIndex: "is_best_model",
                        render: (v) => (v ? "★" : ""),
                    },
                ]}
            />
        </div>
    );
}
