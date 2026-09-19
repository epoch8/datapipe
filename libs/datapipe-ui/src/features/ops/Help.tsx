import React from "react";
import { Card, Typography } from "antd";

const { Paragraph, Title } = Typography;

export function Help() {
    return (
        <Card>
            <Title level={4}>Datapipe Ops UI</Title>
            <Paragraph>
                Cloud dashboard for the slim <strong>Ops API v1alpha3</strong>: pipeline graph, table
                browsing, transform metadata, and interactive transform runs.
            </Paragraph>
            <Paragraph>
                <strong>Graph</strong> — DAG of tables and transforms from{" "}
                <code>/api/v1alpha3/graph</code>. Open a node for details, data, or a transform run.
            </Paragraph>
            <Paragraph>
                <strong>Tables / transforms</strong> — browse rows via{" "}
                <code>get-table-data</code> / <code>get-transform-data</code>, count sizes, and reset
                transform metadata when needed.
            </Paragraph>
            <Paragraph>
                OpenAPI docs: <a href="/api/v1alpha3/docs">/api/v1alpha3/docs</a>. Legacy{" "}
                <a href="/api/v1alpha1/docs">v1alpha1</a> and{" "}
                <a href="/api/v1alpha2/docs">v1alpha2</a> may still be mounted.
            </Paragraph>
        </Card>
    );
}
