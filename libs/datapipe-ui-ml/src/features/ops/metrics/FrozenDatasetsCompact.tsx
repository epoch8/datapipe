import React from "react";
import { Link } from "react-router-dom";
import type { FrozenDatasetRow } from "../../../types/opsMl";
import { EntityLink } from "./EntityLink";
import { buildDatasetUrl } from "./entityUrls";
import { formatFrozenAt } from "./frozenDatasetFormat";

type Props = {
    rows: FrozenDatasetRow[];
    loading?: boolean;
    pipelineId?: string;
};

export function FrozenDatasetsCompact({ rows, loading, pipelineId }: Props) {
    return (
        <div className="ops-panel ops-frozen-compact">
            <div className="ops-panel-title">Frozen datasets</div>
            {loading && !rows.length ? (
                <div className="ops-muted">Loading…</div>
            ) : !rows.length ? (
                <div className="ops-muted">No frozen datasets</div>
            ) : (
                <table className="ops-frozen-compact-table">
                    <thead>
                        <tr>
                            <th>Dataset</th>
                            <th>Frozen at</th>
                            <th>Split</th>
                            <th>Models</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((row) => (
                            <tr key={row.dataset_id}>
                                <td>
                                    <EntityLink kind="dataset" id={row.dataset_id} />
                                </td>
                                <td>{formatFrozenAt(row.frozen_at)}</td>
                                <td>
                                    <span title="train / val / test">
                                        {row.train_count ?? 0} / {row.val_count ?? 0} / {row.test_count ?? 0}
                                    </span>
                                </td>
                                <td>
                                    <Link to={buildDatasetUrl(row.dataset_id, pipelineId)}>
                                        {row.models_count ?? 0} models
                                    </Link>
                                    <div className="ops-muted">trained on this snapshot</div>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </div>
    );
}

export function splitSizeLabel(row: { split_counts?: { train?: number; val?: number; test?: number } }) {
    const s = row.split_counts ?? {};
    return `${s.train ?? 0} / ${s.val ?? 0} / ${s.test ?? 0}`;
}
