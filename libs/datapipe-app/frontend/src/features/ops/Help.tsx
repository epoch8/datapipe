import React from "react";
import { Card, Typography } from "antd";

const { Paragraph, Title } = Typography;

export function Help() {
    return (
        <Card>
            <Title level={4}>Help & documentation</Title>
            <Paragraph>
                <strong>Pipeline lifecycle:</strong> annotation → Label Studio sync → train →
                inference → metrics → optional FiftyOne export.
            </Paragraph>
            <Paragraph>
                <strong>Pipeline graph</strong> — right-click any node for options: open a detail page,
                or expand/collapse multi-step pipeline groups. Table names show in full (up to 64
                characters). Zoom in to read compact sub-graph labels.
            </Paragraph>
            <Paragraph>
                <strong>Entity pages</strong> — tables, transforms, and pipeline steps have detail
                pages with properties, table visualization (Ant Design table), transform run with
                optional index filters, and execution logs.
            </Paragraph>
            <Paragraph>
                <strong>Pipeline run page</strong> — execution status at the top, live Logs panel
                (ring-buffer, Airflow-style), graph hidden under “Pipeline steps” collapse.
            </Paragraph>
            <Paragraph>
                <strong>Training run</strong> — one model training job tracked in
                training_status.
            </Paragraph>
            <Paragraph>
                <strong>Eval metrics</strong> — post-training metrics on a validation subset
                (Metrics page).
            </Paragraph>
            <Paragraph>
                API docs: <a href="/api/v1alpha3/docs">/api/v1alpha3/docs</a>
            </Paragraph>
        </Card>
    );
}
