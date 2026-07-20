import React from "react";
import { Button, Input, Modal, Space, Switch, notification } from "antd";
import { useSearchParams } from "react-router-dom";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { ApiError } from "@datapipe/ui/api/ops";
import type {
    TrainingExperimentRow,
    TrainingExperimentsSummary,
} from "../../../types/opsMl";
import { EmptyState } from "../shared";
import { TrainingExperimentsTable } from "./TrainingExperimentsTable";
import { TrainingExperimentPanel } from "./TrainingExperimentPanel";
import { TrainingExperimentDrawer, type TrainingExperimentDrawerMode } from "./TrainingExperimentDrawer";
import "./trainingExperiments.css";

type Props = {
    pipelineId: string;
    specId: string;
    /** Bumping this value forces a reload (e.g. after a run is launched). */
    refreshToken?: number;
};

const SUMMARY_CARDS: { key: keyof TrainingExperimentsSummary; label: string }[] = [
    { key: "total", label: "Total" },
    { key: "builtin", label: "Built-in" },
    { key: "custom", label: "Custom" },
    { key: "editable", label: "Editable" },
    { key: "locked", label: "Locked" },
    { key: "archived", label: "Archived" },
];

const EMPTY_SUMMARY: TrainingExperimentsSummary = {
    total: 0,
    builtin: 0,
    custom: 0,
    editable: 0,
    locked: 0,
    archived: 0,
};

type DrawerKind = "" | "create" | "edit" | "view" | "run";

export function TrainingExperimentsTab({ pipelineId, specId, refreshToken }: Props) {
    const [searchParams, setSearchParams] = useSearchParams();
    const [rows, setRows] = React.useState<TrainingExperimentRow[]>([]);
    const [summary, setSummary] = React.useState<TrainingExperimentsSummary>(EMPTY_SUMMARY);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<unknown>(null);
    const [busy, setBusy] = React.useState(false);
    const [search, setSearch] = React.useState(searchParams.get("q") ?? "");
    const [includeArchived, setIncludeArchived] = React.useState(searchParams.get("archived") === "1");

    const selectedId = searchParams.get("experiment") ?? undefined;
    const drawer = (searchParams.get("drawer") ?? "") as DrawerKind;

    const patchParams = React.useCallback(
        (patch: Record<string, string | undefined>) => {
            setSearchParams(
                (prev) => {
                    const next = new URLSearchParams(prev);
                    Object.entries(patch).forEach(([key, value]) => {
                        if (value) next.set(key, value);
                        else next.delete(key);
                    });
                    return next;
                },
                { replace: true },
            );
        },
        [setSearchParams],
    );

    const load = React.useCallback(() => {
        if (!pipelineId || !specId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getTrainingExperiments(pipelineId, specId, {
                search: search.trim() || undefined,
                include_archived: includeArchived,
            })
            .then((res) => {
                setRows(res.rows);
                setSummary(res.summary ?? EMPTY_SUMMARY);
            })
            .catch((e) => setError(e))
            .finally(() => setLoading(false));
    }, [pipelineId, specId, search, includeArchived]);

    React.useEffect(() => {
        const timer = window.setTimeout(load, 250);
        return () => window.clearTimeout(timer);
    }, [load, refreshToken]);

    const selected = React.useMemo(
        () => rows.find((row) => row.id === selectedId),
        [rows, selectedId],
    );

    const openDrawer = (kind: DrawerKind, experimentId?: string) =>
        patchParams({ drawer: kind || undefined, experiment: experimentId ?? selectedId });
    const closeDrawer = () => patchParams({ drawer: undefined });

    const runAction = async (
        action: () => Promise<TrainingExperimentRow | void>,
        successMessage: string,
    ) => {
        setBusy(true);
        try {
            const result = await action();
            notification.success({ message: successMessage });
            load();
            return result ?? undefined;
        } catch (err) {
            if (err instanceof ApiError && err.status === 409 && err.code === "experiment_locked") {
                notification.warning({
                    message: "Experiment is locked",
                    description: err.message,
                });
                load();
            } else {
                notification.error({
                    message: "Action failed",
                    description: err instanceof ApiError ? err.message : String(err),
                });
            }
            return undefined;
        } finally {
            setBusy(false);
        }
    };

    const handleDuplicate = (row: TrainingExperimentRow) =>
        runAction(
            () => opsApi.duplicateTrainingExperiment(pipelineId, specId, row.id),
            "Experiment duplicated",
        );

    const handleArchiveToggle = (row: TrainingExperimentRow) =>
        runAction(
            () =>
                row.active
                    ? opsApi.archiveTrainingExperiment(pipelineId, specId, row.id)
                    : opsApi.unarchiveTrainingExperiment(pipelineId, specId, row.id),
            row.active ? "Experiment archived" : "Experiment unarchived",
        );

    const handleDelete = (row: TrainingExperimentRow) => {
        Modal.confirm({
            title: "Delete experiment?",
            content: `"${row.display_name ?? row.id}" will be permanently removed.`,
            okText: "Delete",
            okButtonProps: { danger: true },
            onOk: () =>
                runAction(
                    () => opsApi.deleteTrainingExperiment(pipelineId, specId, row.id),
                    "Experiment deleted",
                ),
        });
    };

    const drawerMode: TrainingExperimentDrawerMode | null =
        drawer === "create" ? "create" : drawer === "edit" ? "edit" : drawer === "view" ? "view" : null;

    return (
        <div>
            <div className="te-summary-grid">
                {SUMMARY_CARDS.map((card) => (
                    <div className="te-summary-card" key={card.key}>
                        <div className="te-summary-label">{card.label}</div>
                        <div className="te-summary-value">{summary[card.key]}</div>
                    </div>
                ))}
            </div>

            <div className="ops-spec-table-head" style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
                <Space>
                    <Input.Search
                        allowClear
                        placeholder="Search experiments..."
                        defaultValue={search}
                        style={{ width: 260 }}
                        onSearch={(value) => {
                            setSearch(value);
                            patchParams({ q: value || undefined });
                        }}
                    />
                    <span>
                        <Switch
                            size="small"
                            checked={includeArchived}
                            onChange={(checked) => {
                                setIncludeArchived(checked);
                                patchParams({ archived: checked ? "1" : undefined });
                            }}
                        />{" "}
                        Show archived
                    </span>
                </Space>
                <Button type="primary" onClick={() => openDrawer("create", undefined)}>
                    New experiment
                </Button>
            </div>

            <div className="te-layout">
                <div className="ops-panel ops-polished-panel ops-spec-table-panel">
                    <EmptyState
                        loading={loading}
                        error={error}
                        empty={!rows.length && !loading}
                        emptyMessage="No experiments yet. Create one to get started."
                        keepChildrenWhileLoading
                    >
                        <TrainingExperimentsTable
                            rows={rows}
                            loading={loading}
                            selectedId={selectedId}
                            specId={specId}
                            onSelect={(row) => patchParams({ experiment: row.id })}
                        />
                    </EmptyState>
                </div>

                <TrainingExperimentPanel
                    experiment={selected}
                    specId={specId}
                    busy={busy}
                    onView={(row) => openDrawer("view", row.id)}
                    onEdit={(row) => openDrawer("edit", row.id)}
                    onDuplicate={handleDuplicate}
                    onArchiveToggle={handleArchiveToggle}
                    onDelete={handleDelete}
                    onNewRun={(row) => openDrawer("run", row.id)}
                />
            </div>

            {drawerMode ? (
                <TrainingExperimentDrawer
                    open
                    mode={drawerMode}
                    pipelineId={pipelineId}
                    specId={specId}
                    experiment={drawerMode === "create" ? null : selected}
                    onClose={closeDrawer}
                    onConflict={load}
                    onSaved={(row) => {
                        load();
                        patchParams({ drawer: undefined, experiment: row.id });
                    }}
                    onSaveAndStart={
                        drawerMode === "create"
                            ? (row) => {
                                  load();
                                  patchParams({ drawer: "run", experiment: row.id });
                              }
                            : undefined
                    }
                />
            ) : null}
        </div>
    );
}
