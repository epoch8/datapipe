import React from "react";
import { Button } from "antd";
import { Link } from "react-router-dom";
import { EntityLink } from "./EntityLink";

type Props = {
    title: string;
    record?: Record<string, unknown> | null;
    preferredFields?: string[];
    sourceTable?: string | null;
    sourceTableUrl?: string | null;
    pipelineId?: string;
    emptyText?: string;
};

function formatValue(value: unknown): React.ReactNode {
    if (value == null || value === "") return "—";
    if (typeof value === "object") return JSON.stringify(value);
    if (typeof value === "boolean") return value ? "true" : "false";
    return String(value);
}

function entityRenderer(field: string, value: unknown): React.ReactNode {
    if (value == null || value === "") return "—";
    const text = String(value);
    if (field === "model_id" || field.endsWith("_model_id")) {
        return <EntityLink kind="model" id={text} />;
    }
    if (field === "frozen_dataset_id" || field.endsWith("_frozen_dataset_id")) {
        return <EntityLink kind="dataset" id={text} />;
    }
    return formatValue(value);
}

export function SourceRecordCard({
    title,
    record,
    preferredFields = [],
    sourceTable,
    sourceTableUrl,
    pipelineId,
    emptyText = "No source record found.",
}: Props) {
    const fields = React.useMemo(() => {
        if (!record) return [];
        const keys = new Set<string>();
        for (const key of preferredFields) {
            if (key in record) keys.add(key);
        }
        for (const key of Object.keys(record)) {
            if (!keys.has(key) && keys.size < 12) keys.add(key);
        }
        return Array.from(keys);
    }, [record, preferredFields]);

    return (
        <div className="ops-panel ops-source-record-card">
            <div className="ops-panel-title">{title}</div>
            {sourceTable && (
                <div className="ops-source-record-table-link">
                    source table:{" "}
                    {pipelineId ? (
                        <Link to={`/pipelines/${encodeURIComponent(pipelineId)}/tables/${encodeURIComponent(sourceTable)}`}>
                            {sourceTable} ↗
                        </Link>
                    ) : (
                        sourceTable
                    )}
                </div>
            )}
            {!record ? (
                <div className="ops-muted">{emptyText}</div>
            ) : (
                <dl className="ops-source-record-dl">
                    {fields.map((field) => (
                        <React.Fragment key={field}>
                            <dt>{field}</dt>
                            <dd>{entityRenderer(field, record[field])}</dd>
                        </React.Fragment>
                    ))}
                </dl>
            )}
            {sourceTableUrl && (
                <Button type="link" size="small" href={sourceTableUrl}>
                    Open row in table
                </Button>
            )}
        </div>
    );
}
