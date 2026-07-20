import React from "react";
import { Alert, Button, Drawer, Input, Space, Typography, notification } from "antd";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { ApiError } from "@datapipe/ui/api/ops";
import type { TrainingExperimentRow } from "../../../types/opsMl";
import { TrainConfigForm } from "./TrainConfigForm";
import { defaultsFromTrainConfigSchema } from "./trainingUtils";

export type TrainingExperimentDrawerMode = "create" | "edit" | "view";

type Props = {
    open: boolean;
    mode: TrainingExperimentDrawerMode;
    pipelineId: string;
    specId: string;
    experiment?: TrainingExperimentRow | null;
    onClose: () => void;
    onSaved: (row: TrainingExperimentRow) => void;
    /** Called after a successful create when the user chose "Save and start". */
    onSaveAndStart?: (row: TrainingExperimentRow) => void;
    /** Notify the parent it should reload (e.g. after a lock/revision conflict). */
    onConflict?: () => void;
};

type FieldError = { field?: string | null; message?: string };

function extractValidationErrors(err: unknown): FieldError[] {
    if (err instanceof ApiError && err.details && typeof err.details === "object") {
        const details = err.details as { errors?: unknown };
        if (Array.isArray(details.errors)) {
            return details.errors as FieldError[];
        }
    }
    return [];
}

export function TrainingExperimentDrawer({
    open,
    mode,
    pipelineId,
    specId,
    experiment,
    onClose,
    onSaved,
    onSaveAndStart,
    onConflict,
}: Props) {
    const [displayName, setDisplayName] = React.useState("");
    const [description, setDescription] = React.useState("");
    const [params, setParams] = React.useState<Record<string, unknown>>({});
    const [schema, setSchema] = React.useState<Record<string, unknown> | null>(null);
    const [paramsValid, setParamsValid] = React.useState(true);
    const [saving, setSaving] = React.useState(false);
    const [validationErrors, setValidationErrors] = React.useState<FieldError[]>([]);
    const [errorMessage, setErrorMessage] = React.useState<string | null>(null);

    const readOnly = mode === "view" || (mode === "edit" && !experiment?.capabilities.can_edit);

    // Reset the form whenever the drawer (re)opens for a different experiment.
    React.useEffect(() => {
        if (!open) return;
        setDisplayName(experiment?.display_name ?? "");
        setDescription(experiment?.description ?? "");
        setParams({ ...(experiment?.params ?? {}) });
        setParamsValid(true);
        setValidationErrors([]);
        setErrorMessage(null);
    }, [open, experiment]);

    // Load the config schema; for create, prefill every field with codec defaults.
    React.useEffect(() => {
        if (!open || !pipelineId || !specId) return;
        let cancelled = false;
        opsApi
            .getTrainConfigSchema(pipelineId, specId, experiment?.config_type)
            .then((res) => {
                if (cancelled) return;
                const nextSchema = res.schema ?? null;
                setSchema(nextSchema);
                if (mode === "create") {
                    const defaults = defaultsFromTrainConfigSchema(nextSchema);
                    setParams((prev) => ({ ...defaults, ...prev }));
                }
            })
            .catch(() => {
                if (!cancelled) setSchema(null);
            });
        return () => {
            cancelled = true;
        };
    }, [open, pipelineId, specId, experiment?.config_type, mode]);

    const handleError = (err: unknown) => {
        const validation = extractValidationErrors(err);
        setValidationErrors(validation);
        if (err instanceof ApiError) {
            if (err.code === "experiment_locked" || err.code === "experiment_revision_conflict") {
                notification.warning({
                    message: "Experiment changed",
                    description: err.message,
                });
                onConflict?.();
                onClose();
                return;
            }
            setErrorMessage(validation.length ? null : err.message);
        } else {
            setErrorMessage(String(err));
        }
    };

    const persist = async (): Promise<TrainingExperimentRow | null> => {
        if (!displayName.trim()) {
            setErrorMessage("A display name is required.");
            return null;
        }
        setSaving(true);
        setErrorMessage(null);
        setValidationErrors([]);
        try {
            if (mode === "edit" && experiment) {
                return await opsApi.updateTrainingExperiment(pipelineId, specId, experiment.id, {
                    display_name: displayName.trim(),
                    description: description.trim() || null,
                    params,
                    expected_revision: experiment.revision,
                });
            }
            return await opsApi.createTrainingExperiment(pipelineId, specId, {
                display_name: displayName.trim(),
                description: description.trim() || null,
                params,
            });
        } catch (err) {
            handleError(err);
            return null;
        } finally {
            setSaving(false);
        }
    };

    const handleSave = async () => {
        const row = await persist();
        if (row) onSaved(row);
    };

    const handleSaveAndStart = async () => {
        const row = await persist();
        if (row) onSaveAndStart?.(row);
    };

    const title =
        mode === "create"
            ? "New experiment"
            : mode === "edit"
              ? `Edit ${experiment?.display_name ?? experiment?.id ?? "experiment"}`
              : experiment?.display_name ?? experiment?.id ?? "Experiment";

    return (
        <Drawer
            title={title}
            visible={open}
            width={640}
            onClose={onClose}
            destroyOnClose
            footer={
                readOnly ? (
                    <div className="te-drawer-footer">
                        <Button onClick={onClose}>Close</Button>
                    </div>
                ) : (
                    <div className="te-drawer-footer">
                        <Button onClick={onClose} disabled={saving}>
                            Cancel
                        </Button>
                        {onSaveAndStart ? (
                            <Button onClick={handleSaveAndStart} loading={saving} disabled={!paramsValid}>
                                Save and start training
                            </Button>
                        ) : null}
                        <Button type="primary" onClick={handleSave} loading={saving} disabled={!paramsValid}>
                            Save
                        </Button>
                    </div>
                )
            }
        >
            {readOnly && experiment?.capabilities.lock_reason ? (
                <Alert
                    type="info"
                    showIcon
                    style={{ marginBottom: 12 }}
                    message={experiment.capabilities.lock_reason}
                />
            ) : null}

            {errorMessage ? (
                <Alert type="error" showIcon style={{ marginBottom: 12 }} message={errorMessage} />
            ) : null}

            {validationErrors.length ? (
                <div className="te-validation-errors">
                    <ul>
                        {validationErrors.map((e, i) => (
                            <li key={i}>
                                {e.field ? `${e.field}: ` : ""}
                                {e.message}
                            </li>
                        ))}
                    </ul>
                </div>
            ) : null}

            <Space direction="vertical" size={12} style={{ width: "100%" }}>
                <div>
                    <Typography.Text strong>Display name</Typography.Text>
                    <Input
                        aria-label="Display name"
                        value={displayName}
                        disabled={readOnly}
                        onChange={(e) => setDisplayName(e.target.value)}
                        placeholder="e.g. YOLOv8m high-res"
                    />
                </div>
                <div>
                    <Typography.Text strong>Description</Typography.Text>
                    <Input.TextArea
                        aria-label="Description"
                        value={description}
                        disabled={readOnly}
                        autoSize={{ minRows: 2, maxRows: 4 }}
                        onChange={(e) => setDescription(e.target.value)}
                    />
                </div>
                <div>
                    <Typography.Text strong>Parameters</Typography.Text>
                    <TrainConfigForm
                        schema={schema}
                        value={params}
                        onChange={setParams}
                        onValidityChange={setParamsValid}
                        disabled={readOnly}
                    />
                </div>
            </Space>
        </Drawer>
    );
}
