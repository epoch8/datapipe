import React from "react";
import { Button } from "antd";
import { Link } from "react-router-dom";
import { EntityLink } from "./EntityLink";
import { isPrimaryKeyField, recordFieldOrder } from "./recordFields";

type Props = {
    title: string;
    record?: Record<string, unknown> | null;
    sourcePk?: Record<string, unknown> | null;
    highlightFields?: Array<string | null | undefined>;
    sourceTable?: string | null;
    sourceTableUrl?: string | null;
    pipelineId?: string;
    emptyText?: string;
    maxFields?: number;
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
    sourcePk,
    highlightFields = [],
    sourceTable,
    sourceTableUrl,
    pipelineId,
    emptyText = "No source record found.",
    maxFields = 32,
}: Props) {
    const fields = React.useMemo(
        () => recordFieldOrder({ record, sourcePk, highlightFields, maxFields }),
        [record, sourcePk, highlightFields, maxFields],
    );

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
                            <dt>
                                {field}
                                {isPrimaryKeyField(field, sourcePk) ? (
                                    <span className="dp-col-pk-badge ops-source-record-pk-badge">PK</span>
                                ) : null}
                            </dt>
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
