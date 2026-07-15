import React from "react";
import { Card, Typography } from "antd";

const { Paragraph, Title } = Typography;

export function Help() {
    return (
        <Card>
            <Title level={4}>Help & documentation</Title>
            <Paragraph>
                <strong>Datapipe Ops</strong> is bound to one pipeline agent. The sidebar shows
                Overview, Graph, Runs, and — when the pipeline registers ops specs — per-spec
                sections contributed by installed UI plugins.
            </Paragraph>
            <Paragraph>
                <strong>Pipeline stages</strong> come from pipeline step labels (for example{" "}
                <code>stage=annotation</code>, <code>stage=train</code>, <code>stage=fiftyone</code>
                ). Exact stages depend on the pipeline; there is no single fixed lifecycle baked
                into Ops.
            </Paragraph>
            <Paragraph>
                <strong>Overview</strong> — pipeline health, label-graph summary, run stats, and
                recent runs. Use <em>Run steps</em> to start a full pipeline or a single stage.
            </Paragraph>
            <Paragraph>
                <strong>Graph</strong> — Cytoscape DAG of tables and transforms. Right-click a node:
                open its detail page, or expand/collapse grouped multi-step transforms. Table names
                wrap up to 64 characters. Use <code>?stage=…</code> in the URL to focus one stage.
                The compact label graph on Overview and Run pages supports right-click{" "}
                <em>Open graph</em> / <em>Run label</em>.
            </Paragraph>
            <Paragraph>
                <strong>Entity pages</strong> — tables, transforms, and meta-steps open detail pages
                with schema, data preview, transform run (optional index filters), and logs.
                Spec-driven pages add frozen datasets, models, training runs, per-class metrics, and
                image record views linked from ops specs.
            </Paragraph>
            <Paragraph>
                <strong>Run page</strong> — status and scope at the top, compact label graph, then
                tabs: <em>Logs</em> (live ring-buffer logs plus step graph for the run) and{" "}
                <em>Steps</em> (per-step table). <em>Run steps</em> in the header starts another
                run with the same scope.
            </Paragraph>
            <Paragraph>
                <strong>Ops API</strong> — the dashboard uses <code>/api/v1alpha3</code>. OpenAPI
                docs: <a href="/api/v1alpha3/docs">/api/v1alpha3/docs</a>. Legacy pipeline
                endpoints under <code>/api/v1alpha1</code> and <code>/api/v1alpha2</code> may still
                be mounted for compatibility; they are not part of this UI and are not documented
                here.
            </Paragraph>
        </Card>
    );
}
