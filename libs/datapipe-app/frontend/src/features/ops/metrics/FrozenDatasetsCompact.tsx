import React from "react";
import { Button, Tooltip } from "antd";
import { CopyOutlined } from "@ant-design/icons";
import type { FrozenDatasetRow } from "../../../types/ops";

type Props = {
    rows: FrozenDatasetRow[];
    loading?: boolean;
};

function truncateId(id: string, max = 28): string {
    if (id.length <= max) return id;
    return `${id.slice(0, max)}…`;
}

function copyText(text: string) {
    void navigator.clipboard?.writeText(text);
}

function formatFrozenAt(iso?: string): string {
    if (!iso) return "—";
    const normalized = iso.replace("Z", "").replace(/\.\d+$/, "");
    const [date, time] = normalized.split("T");
    if (!time) return `${date} 00:00:00`;
    const [h = "00", m = "00", s = "00"] = time.split(":");
    return `${date} ${h.padStart(2, "0")}:${m.padStart(2, "0")}:${s.padStart(2, "0")}`;
}

export function FrozenDatasetsCompact({ rows, loading }: Props) {
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
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((row) => (
                            <tr key={row.dataset_id}>
                                <td>
                                    <Tooltip title={row.dataset_id}>
                                        <span className="ops-truncate-id">{truncateId(row.dataset_id)}</span>
                                    </Tooltip>
                                    <Button
                                        type="text"
                                        size="small"
                                        icon={<CopyOutlined />}
                                        aria-label="Copy dataset id"
                                        onClick={() => copyText(row.dataset_id)}
                                    />
                                </td>
                                <td>{formatFrozenAt(row.frozen_at)}</td>
                                <td>
                                    <Tooltip title="train / val / test">
                                        <span>
                                            {row.train_count ?? 0} / {row.val_count ?? 0} / {row.test_count ?? 0}
                                        </span>
                                    </Tooltip>
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
