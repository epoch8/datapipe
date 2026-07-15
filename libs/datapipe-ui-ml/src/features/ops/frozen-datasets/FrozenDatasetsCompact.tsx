import React from "react";
import { Link } from "react-router-dom";
import type { FrozenDatasetRow } from "../../../types/opsMl";
import { EntityLink } from "../shared/EntityLink";
import { formatFrozenAt } from "./frozenDatasetFormat";

type Props = {
    rows: FrozenDatasetRow[];
    loading?: boolean;
    pipelineId?: string;
    specId?: string;
};

export function FrozenDatasetsCompact({ rows, loading, specId }: Props) {
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
                                    <EntityLink kind="dataset" id={row.dataset_id} specId={specId} />
                                </td>
                                <td>{formatFrozenAt(row.frozen_at)}</td>
                                <td>
                                    <span title="train / val / test">
                                        {row.train_count ?? 0} / {row.val_count ?? 0} / {row.test_count ?? 0}
                                    </span>
                                </td>
                                <td>
                                    <EntityLink kind="dataset" id={row.dataset_id} specId={specId}>
                                        {row.models_count ?? 0} models
                                    </EntityLink>
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
