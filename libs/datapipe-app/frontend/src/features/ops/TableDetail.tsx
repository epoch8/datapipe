import React from "react";
import {
    Alert,
    Button,
    Card,
    Collapse,
    Descriptions,
    Spin,
    Typography,
} from "antd";
import { Link, useParams } from "react-router-dom";
import {
    findTableInGraph,
    usePipelineGraph,
} from "../../hooks/usePipelineGraph";
import { TableDataPanel } from "./components/TableDataPanel";

const { Text } = Typography;

export function TableDetail() {
    const { id: pipelineId = "", tableName = "" } = useParams();
    const decodedName = decodeURIComponent(tableName);
    const { graph, loading, error } = usePipelineGraph();
    const [showData, setShowData] = React.useState(false);

    if (error) return <Alert type="error" message={error} />;
    if (loading || !graph) return <Spin />;

    const table = findTableInGraph(graph, decodedName);
    if (!table) {
        return <Alert type="error" message={`Table not found: ${decodedName}`} />;
    }

    const pipeTable = {
        id: table.name,
        indexes: table.indexes,
        size: table.size,
        store_class: table.store_class,
        type: "table",
    };

    return (
        <div>
            <Text type="secondary">
                <Link to="/">Overview</Link> /{" "}
                <Link to={`/pipelines/${pipelineId}`}>{pipelineId}</Link> / Table
            </Text>
            <Card title={decodedName} style={{ marginTop: 16 }}>
                <Descriptions column={1} bordered size="small">
                    <Descriptions.Item label="Name">{table.name}</Descriptions.Item>
                    <Descriptions.Item label="Indexes">
                        {table.indexes.join(", ") || "—"}
                    </Descriptions.Item>
                    <Descriptions.Item label="Size">{table.size}</Descriptions.Item>
                    <Descriptions.Item label="Store">{table.store_class}</Descriptions.Item>
                </Descriptions>
                <Button
                    type="primary"
                    style={{ marginTop: 16 }}
                    onClick={() => setShowData((v) => !v)}
                >
                    {showData ? "Hide table data" : "Visualize table"}
                </Button>
            </Card>
            {showData && (
                <Collapse activeKey={["data"]} style={{ marginTop: 16 }}>
                    <Collapse.Panel header="Table data" key="data">
                        <TableDataPanel table={pipeTable} />
                    </Collapse.Panel>
                </Collapse>
            )}
        </div>
    );
}
