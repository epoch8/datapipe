import React from "react";
import { Button, Space } from "antd";
import { Link } from "react-router-dom";
import type { TrainingExperimentRow } from "../../../types/opsMl";
import { TrainingExperimentStatusTag, formatExperimentParams } from "./TrainingExperimentStatusTag";

type Props = {
    experiment?: TrainingExperimentRow | null;
    specId: string;
    onView: (row: TrainingExperimentRow) => void;
    onEdit: (row: TrainingExperimentRow) => void;
    onDuplicate: (row: TrainingExperimentRow) => void;
    onArchiveToggle: (row: TrainingExperimentRow) => void;
    onDelete: (row: TrainingExperimentRow) => void;
    onNewRun: (row: TrainingExperimentRow) => void;
    busy?: boolean;
};

function fmtDate(value?: string | null): string {
    if (!value) return "—";
    return value.slice(0, 16).replace("T", " ");
}

export function TrainingExperimentPanel({
    experiment,
    specId,
    onView,
    onEdit,
    onDuplicate,
    onArchiveToggle,
    onDelete,
    onNewRun,
    busy,
}: Props) {
    if (!experiment) {
        return (
            <div className="ops-panel ops-polished-panel ops-side-rail">
                <div className="ops-panel-title">Experiment details</div>
                <p className="te-panel-empty">Select an experiment to see its details and actions.</p>
            </div>
        );
    }

    const caps = experiment.capabilities;
    const detailHref = `/training/${encodeURIComponent(specId)}/experiments/${encodeURIComponent(experiment.id)}`;

    return (
        <div className="ops-panel ops-polished-panel ops-side-rail">
            <div className="ops-panel-title">
                <Link className="dp-entity-link" to={detailHref}>
                    {experiment.display_name ?? experiment.id}
                </Link>
            </div>
            <Space size={6} wrap>
                <TrainingExperimentStatusTag row={experiment} />
                <span className="ops-muted">{experiment.config_type}</span>
            </Space>

            {experiment.description ? (
                <p className="te-params" style={{ marginTop: 10 }}>
                    {experiment.description}
                </p>
            ) : null}

            <dl className="ops-detail-list ops-detail-list-wide" style={{ marginTop: 12 }}>
                <dt>Main params</dt>
                <dd>{formatExperimentParams(experiment)}</dd>
                <dt>Requests</dt>
                <dd>{experiment.requests_count}</dd>
                <dt>Runs</dt>
                <dd>{experiment.runs_count}</dd>
                <dt>Revision</dt>
                <dd>{experiment.revision}</dd>
                <dt>Last used</dt>
                <dd>{fmtDate(experiment.last_used_at)}</dd>
                <dt>Updated</dt>
                <dd>{fmtDate(experiment.updated_at)}</dd>
            </dl>

            {caps.lock_reason ? <div className="te-lock-reason">{caps.lock_reason}</div> : null}

            <div className="te-panel-actions">
                <Link to={detailHref}>
                    <Button>Open experiment</Button>
                </Link>
                <Button
                    type="primary"
                    disabled={!caps.can_launch || busy}
                    onClick={() => onNewRun(experiment)}
                >
                    New training run
                </Button>
                {caps.can_edit ? (
                    <Button disabled={busy} onClick={() => onEdit(experiment)}>
                        Edit
                    </Button>
                ) : (
                    <Button disabled={busy} onClick={() => onView(experiment)}>
                        View config
                    </Button>
                )}
                {caps.can_duplicate ? (
                    <Button disabled={busy} onClick={() => onDuplicate(experiment)}>
                        Duplicate
                    </Button>
                ) : null}
                {caps.can_archive ? (
                    <Button disabled={busy} onClick={() => onArchiveToggle(experiment)}>
                        {experiment.active ? "Archive" : "Unarchive"}
                    </Button>
                ) : null}
                {caps.can_delete ? (
                    <Button danger disabled={busy} onClick={() => onDelete(experiment)}>
                        Delete
                    </Button>
                ) : null}
            </div>
        </div>
    );
}
