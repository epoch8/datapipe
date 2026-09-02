import React from "react";
import { Button, Popover, Select, Space, Typography } from "antd";
import { opsApi } from "../../../api/client";
import type { LabelGraphPayload, StageItem } from "../../../types/ops";
import { DEFAULT_LABEL_KEY, normalizeLabelKey } from "../utils/labelKey";

const ALL_VALUES = "__all_values__";
const ALL_LABELS = "__all_labels__";

type RunStepsDropdownProps = {
    pipelineId: string;
    stages?: StageItem[] | { stage: string }[];
    availableLabelKeys?: string[];
    labelGraph?: LabelGraphPayload | null;
    defaultLabelKey?: string;
    onStart: (labels: [string, string][]) => void;
    disabled?: boolean;
    primary?: boolean;
};

function valuesFromGraph(graph: LabelGraphPayload | null | undefined): string[] {
    if (!graph?.nodes?.length) return [];
    const seen = new Set<string>();
    const values: string[] = [];
    for (const node of graph.nodes) {
        if (node.kind !== "label") continue;
        if (seen.has(node.id)) continue;
        seen.add(node.id);
        values.push(node.id);
    }
    return values;
}

function valuesFromStages(stages: RunStepsDropdownProps["stages"]): string[] {
    return (stages ?? []).map((s) => s.stage);
}

export function RunStepsDropdown({
    pipelineId,
    stages = [],
    availableLabelKeys = [],
    labelGraph = null,
    defaultLabelKey,
    onStart,
    disabled,
    primary = true,
}: RunStepsDropdownProps) {
    const keys = React.useMemo(() => {
        if (availableLabelKeys.length) return availableLabelKeys;
        return [DEFAULT_LABEL_KEY];
    }, [availableLabelKeys]);

    const initialKey = normalizeLabelKey(defaultLabelKey ?? labelGraph?.label_key, keys);
    const [open, setOpen] = React.useState(false);
    const [labelKey, setLabelKey] = React.useState(initialKey);
    const [values, setValues] = React.useState<string[]>(() => {
        const fromGraph =
            labelGraph?.label_key === initialKey ? valuesFromGraph(labelGraph) : [];
        return fromGraph.length ? fromGraph : valuesFromStages(stages);
    });
    const [selected, setSelected] = React.useState<string[]>([]);
    const [loadingValues, setLoadingValues] = React.useState(false);

    React.useEffect(() => {
        if (!open) return;
        const nextKey = normalizeLabelKey(defaultLabelKey ?? labelGraph?.label_key, keys);
        setLabelKey(nextKey);
        setSelected([]);
        if (labelGraph?.label_key === nextKey && valuesFromGraph(labelGraph).length) {
            setValues(valuesFromGraph(labelGraph));
        } else if (nextKey === DEFAULT_LABEL_KEY && stages.length) {
            setValues(valuesFromStages(stages));
        }
    }, [open, defaultLabelKey, labelGraph, keys, stages]);

    const loadValuesForKey = React.useCallback(
        async (key: string) => {
            setLoadingValues(true);
            try {
                if (key === labelGraph?.label_key) {
                    const fromGraph = valuesFromGraph(labelGraph);
                    if (fromGraph.length) {
                        setValues(fromGraph);
                        return;
                    }
                }
                if (key === DEFAULT_LABEL_KEY && stages.length) {
                    setValues(valuesFromStages(stages));
                    return;
                }
                const detail = await opsApi.getPipeline({ label_key: key });
                const fromGraph = valuesFromGraph(detail.label_graph);
                setValues(
                    fromGraph.length
                        ? fromGraph
                        : key === DEFAULT_LABEL_KEY
                          ? valuesFromStages(detail.stages)
                          : [],
                );
            } catch {
                setValues(key === DEFAULT_LABEL_KEY ? valuesFromStages(stages) : []);
            } finally {
                setLoadingValues(false);
            }
        },
        [pipelineId, labelGraph, stages],
    );

    React.useEffect(() => {
        if (!open) return;
        void loadValuesForKey(labelKey);
    }, [open, labelKey, loadValuesForKey]);

    const valueOptions = React.useMemo(
        () => [
            ...values.map((value) => ({ label: value, value })),
            { label: "All values", value: ALL_VALUES },
            { label: "All labels", value: ALL_LABELS },
        ],
        [values],
    );

    const onValuesChange = (next: string[]) => {
        if (next.includes(ALL_LABELS)) {
            // Exclusive: run the whole pipeline (empty label filter).
            setSelected([ALL_LABELS]);
            return;
        }
        if (next.includes(ALL_VALUES)) {
            setSelected([...values]);
            return;
        }
        setSelected(next);
    };

    const runSelected = () => {
        if (!selected.length) return;
        if (selected.includes(ALL_LABELS)) {
            onStart([]);
        } else {
            onStart(selected.map((value) => [labelKey, value]));
        }
        setOpen(false);
    };

    const content = (
        <div style={{ width: 300 }} onClick={(e) => e.stopPropagation()}>
            <Space direction="vertical" style={{ width: "100%" }} size="small">
                <div>
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                        Label key
                    </Typography.Text>
                    <Select
                        style={{ width: "100%", marginTop: 4 }}
                        value={labelKey}
                        options={keys.map((key) => ({ label: key, value: key }))}
                        onChange={(next) => {
                            setLabelKey(next);
                            setSelected([]);
                        }}
                    />
                </div>
                <div>
                    <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                        Values
                    </Typography.Text>
                    <Select
                        mode="multiple"
                        allowClear
                        style={{ width: "100%", marginTop: 4 }}
                        placeholder={loadingValues ? "Loading…" : "Select values"}
                        loading={loadingValues}
                        value={selected}
                        options={valueOptions}
                        onChange={onValuesChange}
                        maxTagCount="responsive"
                    />
                </div>
                <Button
                    type="primary"
                    block
                    disabled={!selected.length}
                    onClick={runSelected}
                >
                    Run selected
                </Button>
            </Space>
        </div>
    );

    return (
        <Popover
            trigger="click"
            visible={open}
            onVisibleChange={setOpen}
            placement="bottomRight"
            content={content}
        >
            <Button type={primary ? "primary" : "default"} disabled={disabled}>
                Run steps
            </Button>
        </Popover>
    );
}
