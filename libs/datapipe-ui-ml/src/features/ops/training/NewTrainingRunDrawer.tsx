import React from "react";
import { Alert, Button, Checkbox, Drawer, Select, Typography, notification } from "antd";
import { Link, useNavigate } from "react-router-dom";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { ApiError } from "@datapipe/ui/api/ops";
import type {
    CreateTrainingRequestResponse,
    FrozenDatasetRow,
    TrainingExperimentModelRow,
    TrainingExperimentRow,
} from "../../../types/opsMl";
import { buildModelUrl } from "../shared/entityUrls";

type Props = {
    open: boolean;
    pipelineId: string;
    specId: string;
    onClose: () => void;
    onLaunched?: (result: CreateTrainingRequestResponse) => void;
    /** Preselect a specific experiment (e.g. "New run" from an experiment row). */
    preselectExperimentId?: string;
    /** Preselect a frozen dataset (e.g. "Train on this dataset"). */
    initialDatasetId?: string;
};

/** Generate a client request id once. Falls back when `crypto.randomUUID` is absent. */
export function generateClientRequestId(): string {
    const cryptoObj = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
    if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
        return cryptoObj.randomUUID();
    }
    return `req-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function hasLaunchLabels(runLabels: unknown): runLabels is [string, string][] {
    return Array.isArray(runLabels) && runLabels.length > 0;
}

/** Human-readable stage/label list from ops spec ``run_labels``. */
export function formatRunLabels(runLabels: [string, string][]): string {
    return runLabels
        .map((pair) => {
            if (Array.isArray(pair) && pair.length >= 2) {
                const [key, value] = pair;
                return key === "stage" ? String(value) : `${key}=${value}`;
            }
            return String(pair);
        })
        .join(", ");
}

/** Parse pandas-like durations such as ``1w``, ``2d``, ``15min``, ``10m``. */
export function parseWithinTimeMs(value: string): number | null {
    const match = value.trim().match(/^(\d+(?:\.\d+)?)\s*(w|d|h|min|m|s)$/i);
    if (!match) return null;
    const amount = Number(match[1]);
    if (!Number.isFinite(amount)) return null;
    const unit = match[2].toLowerCase();
    const mult =
        unit === "w"
            ? 7 * 86_400_000
            : unit === "d"
              ? 86_400_000
              : unit === "h"
                ? 3_600_000
                : unit === "min" || unit === "m"
                  ? 60_000
                  : 1_000;
    return amount * mult;
}

/**
 * A dataset is stale when it is older than ``maxWithinTime`` relative to the
 * newest frozen snapshot (same rule as training ``max_within_time``).
 */
export function isFrozenDatasetStale(
    row: Pick<FrozenDatasetRow, "frozen_at">,
    latestFrozenAt: string | undefined,
    maxWithinTime: string,
): boolean {
    if (!row.frozen_at || !latestFrozenAt) return false;
    const windowMs = parseWithinTimeMs(maxWithinTime);
    if (windowMs == null) return false;
    const latest = Date.parse(latestFrozenAt);
    const current = Date.parse(row.frozen_at);
    if (Number.isNaN(latest) || Number.isNaN(current)) return false;
    return latest - current >= windowMs;
}

function formatDatasetSplit(row: Pick<FrozenDatasetRow, "train_count" | "val_count" | "test_count">): string {
    return `${row.train_count ?? 0} / ${row.val_count ?? 0} / ${row.test_count ?? 0}`;
}

function DatasetOptionLabel({
    id,
    split,
    stale,
}: {
    id: string;
    split?: string;
    stale?: boolean;
}) {
    return (
        <span
            style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                gap: 12,
                width: "100%",
            }}
        >
            <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>
                {id}
                {stale ? <span className="ops-muted"> (stale)</span> : null}
            </span>
            {split ? (
                <span className="ops-muted" style={{ flexShrink: 0, fontVariantNumeric: "tabular-nums" }}>
                    {split}
                </span>
            ) : null}
        </span>
    );
}

export function NewTrainingRunDrawer({
    open,
    pipelineId,
    specId,
    onClose,
    onLaunched,
    preselectExperimentId,
    initialDatasetId,
}: Props) {
    const navigate = useNavigate();
    const [experiments, setExperiments] = React.useState<TrainingExperimentRow[]>([]);
    const [datasets, setDatasets] = React.useState<FrozenDatasetRow[]>([]);
    const [experimentModels, setExperimentModels] = React.useState<TrainingExperimentModelRow[]>([]);
    const [experimentId, setExperimentId] = React.useState<string | undefined>(preselectExperimentId);
    const [datasetId, setDatasetId] = React.useState<string | undefined>(initialDatasetId);
    const [runLabels, setRunLabels] = React.useState<[string, string][]>([]);
    const [maxWithinTime, setMaxWithinTime] = React.useState<string | null>(null);
    const [bypassStale, setBypassStale] = React.useState(false);
    const [launch, setLaunch] = React.useState(false);
    const [submitting, setSubmitting] = React.useState(false);
    const [error, setError] = React.useState<string | null>(null);

    // A single client_request_id per open session, reused across double-clicks.
    const clientRequestId = React.useRef<string>("");

    const launchConfigured = runLabels.length > 0;
    const launchLabelText = launchConfigured ? formatRunLabels(runLabels) : "";
    const freshnessConfigured = Boolean(maxWithinTime);

    const latestFrozenAt = React.useMemo(() => {
        let latest: string | undefined;
        for (const row of datasets) {
            if (!row.frozen_at) continue;
            if (!latest || Date.parse(row.frozen_at) > Date.parse(latest)) {
                latest = row.frozen_at;
            }
        }
        return latest;
    }, [datasets]);

    const datasetsWithStale = React.useMemo(() => {
        return datasets.map((row) => ({
            row,
            stale:
                freshnessConfigured && maxWithinTime
                    ? isFrozenDatasetStale(row, latestFrozenAt, maxWithinTime)
                    : false,
        }));
    }, [datasets, freshnessConfigured, maxWithinTime, latestFrozenAt]);

    const visibleDatasets = React.useMemo(() => {
        if (!freshnessConfigured || bypassStale) return datasetsWithStale;
        return datasetsWithStale.filter((item) => !item.stale);
    }, [datasetsWithStale, freshnessConfigured, bypassStale]);

    const existingModels = React.useMemo(() => {
        if (!experimentId || !datasetId) return [];
        return experimentModels.filter((row) => row.frozen_dataset_id === datasetId);
    }, [experimentModels, experimentId, datasetId]);

    React.useEffect(() => {
        if (open) {
            if (!clientRequestId.current) clientRequestId.current = generateClientRequestId();
            setExperimentId(preselectExperimentId);
            setDatasetId(initialDatasetId);
            setLaunch(false);
            setBypassStale(false);
            setError(null);
            setExperimentModels([]);
        } else {
            clientRequestId.current = "";
        }
    }, [open, preselectExperimentId, initialDatasetId]);

    React.useEffect(() => {
        if (!open || !pipelineId || !specId) return;
        let cancelled = false;
        opsApi
            .getOpsSpec(pipelineId, specId)
            .then((spec) => {
                if (cancelled) return;
                const labels = spec.training?.requests?.run_labels;
                setRunLabels(hasLaunchLabels(labels) ? labels : []);
                const window = spec.training?.requests?.max_within_time;
                setMaxWithinTime(typeof window === "string" && window.trim() ? window.trim() : null);
                setLaunch(false);
                setBypassStale(false);
            })
            .catch(() => {
                if (!cancelled) {
                    setRunLabels([]);
                    setMaxWithinTime(null);
                    setLaunch(false);
                    setBypassStale(false);
                }
            });
        opsApi
            .getTrainingExperiments(pipelineId, specId, { state: "active", limit: 200 })
            .then((res) => {
                if (cancelled) return;
                const launchable = res.rows.filter((row) => row.capabilities.can_launch);
                setExperiments(launchable);
                setExperimentId((current) => current ?? launchable[0]?.id);
            })
            .catch(() => {
                if (!cancelled) setExperiments([]);
            });
        opsApi
            .getFrozenDatasets(pipelineId)
            .then((res) => {
                if (cancelled) return;
                setDatasets(res.rows);
            })
            .catch(() => {
                if (!cancelled) setDatasets([]);
            });
        return () => {
            cancelled = true;
        };
    }, [open, pipelineId, specId]);

    React.useEffect(() => {
        if (!open || !pipelineId || !specId || !experimentId) {
            setExperimentModels([]);
            return;
        }
        let cancelled = false;
        opsApi
            .getTrainingExperimentModels(pipelineId, specId, experimentId)
            .then((res) => {
                if (!cancelled) setExperimentModels(res.models ?? []);
            })
            .catch(() => {
                if (!cancelled) setExperimentModels([]);
            });
        return () => {
            cancelled = true;
        };
    }, [open, pipelineId, specId, experimentId]);

    // Drop selection when the chosen dataset becomes hidden (stale + no bypass).
    React.useEffect(() => {
        if (!datasetId || datasets.length === 0) return;
        const stillVisible = visibleDatasets.some((item) => item.row.dataset_id === datasetId);
        if (!stillVisible) {
            // Keep an explicitly preselected id visible via bypass, otherwise clear.
            if (initialDatasetId && datasetId === initialDatasetId && !bypassStale) {
                setBypassStale(true);
                return;
            }
            setDatasetId(undefined);
        }
    }, [visibleDatasets, datasetId, initialDatasetId, bypassStale, datasets.length]);

    const datasetOptions = React.useMemo(() => {
        const options = visibleDatasets.map(({ row, stale }) => {
            const hasSplit =
                row.train_count != null || row.val_count != null || row.test_count != null;
            const split = hasSplit ? formatDatasetSplit(row) : undefined;
            return {
                value: row.dataset_id,
                label: <DatasetOptionLabel id={row.dataset_id} split={split} stale={stale} />,
            };
        });
        if (
            initialDatasetId &&
            !options.some((opt) => opt.value === initialDatasetId) &&
            bypassStale
        ) {
            options.unshift({
                value: initialDatasetId,
                label: <DatasetOptionLabel id={initialDatasetId} stale />,
            });
        }
        return options;
    }, [visibleDatasets, initialDatasetId, bypassStale]);

    const submit = async () => {
        if (!experimentId || !datasetId) {
            setError("Select both an experiment and a frozen dataset.");
            return;
        }
        setSubmitting(true);
        setError(null);
        try {
            const shouldLaunch = launchConfigured && launch;
            const result = await opsApi.createTrainingRequest(pipelineId, specId, {
                train_config_id: experimentId,
                frozen_dataset_id: datasetId,
                client_request_id: clientRequestId.current,
                launch: shouldLaunch,
                force: freshnessConfigured ? bypassStale : false,
            });
            const runId = result.launch?.run_id ?? null;
            notification.success({
                message: shouldLaunch ? "Training started" : "Training request created",
                description: runId ? `Run ${runId}` : `Request ${result.request.id}`,
            });
            onLaunched?.(result);
            onClose();
            if (shouldLaunch && runId) {
                navigate(`/runs/${encodeURIComponent(runId)}`);
            }
        } catch (err) {
            if (err instanceof ApiError) {
                setError(err.message);
            } else {
                setError(String(err));
            }
        } finally {
            setSubmitting(false);
        }
    };

    const actionLabel = launchConfigured && launch ? "Start training" : "Create request";
    const staleHiddenCount = freshnessConfigured
        ? datasetsWithStale.filter((item) => item.stale).length
        : 0;

    return (
        <Drawer
            title="New training run"
            visible={open}
            width={480}
            onClose={onClose}
            destroyOnClose
            footer={
                <div className="te-drawer-footer">
                    <Button onClick={onClose} disabled={submitting}>
                        Cancel
                    </Button>
                    <Button type="primary" onClick={submit} loading={submitting}>
                        {actionLabel}
                    </Button>
                </div>
            }
        >
            {error ? <Alert type="error" showIcon style={{ marginBottom: 12 }} message={error} /> : null}

            <div style={{ marginBottom: 16 }}>
                <Typography.Text strong>Experiment</Typography.Text>
                <Select
                    aria-label="Experiment"
                    style={{ width: "100%" }}
                    showSearch
                    optionFilterProp="label"
                    placeholder="Select an experiment"
                    value={experimentId}
                    onChange={setExperimentId}
                    options={experiments.map((row) => ({
                        label: row.display_name ?? row.id,
                        value: row.id,
                    }))}
                />
            </div>

            <div style={{ marginBottom: 16 }}>
                <Typography.Text strong>Frozen dataset</Typography.Text>
                <Select
                    aria-label="Frozen dataset"
                    style={{ width: "100%" }}
                    showSearch
                    optionFilterProp="value"
                    placeholder="Select a frozen dataset"
                    value={datasetId}
                    onChange={setDatasetId}
                    options={datasetOptions}
                />
                {freshnessConfigured && !bypassStale && staleHiddenCount > 0 ? (
                    <Typography.Paragraph type="secondary" style={{ marginTop: 8, marginBottom: 0 }}>
                        Hiding {staleHiddenCount} dataset{staleHiddenCount === 1 ? "" : "s"} older than{" "}
                        <code>{maxWithinTime}</code> vs the latest snapshot.
                    </Typography.Paragraph>
                ) : null}
            </div>

            {existingModels.length > 0 ? (
                <Alert
                    type="warning"
                    showIcon
                    style={{ marginBottom: 16 }}
                    message="Model already trained for this pair"
                    description={
                        <div>
                            <p style={{ marginBottom: 8 }}>
                                A model already exists for this experiment and frozen dataset.{" "}
                                {actionLabel} will not train a new model (pipeline idempotency).
                            </p>
                            <div>
                                {existingModels.map((row) => (
                                    <div key={`${row.model_id}::${row.frozen_dataset_id}`}>
                                        <Link
                                            className="dp-entity-link"
                                            to={buildModelUrl(row.model_id, {
                                                specId,
                                                dataset_id: row.frozen_dataset_id,
                                            })}
                                            onClick={onClose}
                                        >
                                            {row.model_id}
                                        </Link>
                                    </div>
                                ))}
                            </div>
                        </div>
                    }
                />
            ) : null}

            {freshnessConfigured ? (
                <div style={{ marginBottom: 12 }}>
                    <Checkbox checked={bypassStale} onChange={(e) => setBypassStale(e.target.checked)}>
                        Bypass max_within_time (<code>{maxWithinTime}</code>) — allow stale datasets
                    </Checkbox>
                </div>
            ) : null}

            {launchConfigured ? (
                <Checkbox checked={launch} onChange={(e) => setLaunch(e.target.checked)}>
                    Launch pipeline stage <code>{launchLabelText}</code>
                </Checkbox>
            ) : (
                <Typography.Paragraph type="secondary" style={{ marginBottom: 0 }}>
                    Immediate launch is not configured for this ops spec (set{" "}
                    <code>run_labels</code> on the training request spec).
                </Typography.Paragraph>
            )}
        </Drawer>
    );
}
