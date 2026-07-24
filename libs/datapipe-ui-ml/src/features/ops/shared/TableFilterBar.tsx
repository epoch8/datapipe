import React from "react";
import {
    DeleteOutlined,
    DownOutlined,
    FilterOutlined,
    HolderOutlined,
    PlusOutlined,
    SearchOutlined,
    SortAscendingOutlined,
    TableOutlined,
    UpOutlined,
} from "@ant-design/icons";
import { Badge, Button, Input, Select, Space, Tag } from "antd";
import type { OpsColumn } from "../../../types/opsSpecs";
import {
    FILTER_OPERATORS,
    FILTER_OPERATOR_LABELS,
    SUBSET_CHIP_VALUES,
    activeRules,
    chipKind,
    defaultOperatorForColumn,
    findFilterColumn,
    formatRuleParts,
    isSubsetEntityColumn,
    makeDefaultFilterRule,
    createClientId,
    type OpsFilterRuleWithId,
    type OpsFilterState,
} from "./tableFilters";
import "./TableFilterBar.css";

type TableFilterBarProps = {
    columns: OpsColumn[];
    entityLinks?: Record<string, string>;
    value: OpsFilterState;
    onChange: (next: OpsFilterState) => void;
    sortLabel?: string;
    onSortClick?: () => void;
    metricSource?: string;
    onColumnsClick?: () => void;
    searchPlaceholder?: string;
};

function withIds(rules: OpsFilterRuleWithId[]): OpsFilterRuleWithId[] {
    return rules.map((rule) => ({ ...rule, id: rule.id ?? createClientId() }));
}

function SubsetValueInput({
    rule,
    onChange,
}: {
    rule: OpsFilterRuleWithId;
    onChange: (next: OpsFilterRuleWithId) => void;
}) {
    const selected = new Set(
        (rule.value ?? "")
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean),
    );

    const toggle = (chip: string) => {
        const next = new Set(selected);
        if (next.has(chip)) next.delete(chip);
        else next.add(chip);
        onChange({ ...rule, value: Array.from(next).join(",") });
    };

    return (
        <div className="ops-filter-subset-chips">
            {SUBSET_CHIP_VALUES.map((chip) => (
                <Tag
                    key={chip}
                    className={
                        selected.has(chip)
                            ? "ops-filter-subset-chip ops-filter-subset-chip-active"
                            : "ops-filter-subset-chip"
                    }
                    onClick={() => toggle(chip)}
                >
                    {chip}
                </Tag>
            ))}
            <Input
                size="small"
                placeholder="or type value"
                value={rule.value ?? ""}
                onChange={(event) => onChange({ ...rule, value: event.target.value })}
            />
        </div>
    );
}

function FilterValueInput({
    rule,
    column,
    entityLinks,
    onChange,
}: {
    rule: OpsFilterRuleWithId;
    column?: OpsColumn;
    entityLinks: Record<string, string>;
    onChange: (next: OpsFilterRuleWithId) => void;
}) {
    if (rule.operator === "is_empty") {
        return <span className="ops-muted">—</span>;
    }
    if (column && isSubsetEntityColumn(column, entityLinks)) {
        return <SubsetValueInput rule={rule} onChange={onChange} />;
    }
    return (
        <Input
            className="ops-filter-rule-value"
            value={rule.value ?? ""}
            placeholder="Value"
            onChange={(event) => onChange({ ...rule, value: event.target.value })}
            allowClear
        />
    );
}

export function TableFilterBar({
    columns,
    entityLinks = {},
    value,
    onChange,
    sortLabel = "Order by",
    onSortClick,
    metricSource,
    onColumnsClick,
    searchPlaceholder = "Search by text...",
}: TableFilterBarProps) {
    const [open, setOpen] = React.useState(false);
    const rules = withIds(value.rules as OpsFilterRuleWithId[]);
    const applied = activeRules(rules);

    const updateRules = (nextRules: OpsFilterRuleWithId[]) => {
        onChange({ ...value, rules: nextRules });
    };

    const updateRule = (id: string, patch: Partial<OpsFilterRuleWithId>) => {
        updateRules(rules.map((rule) => (rule.id === id ? { ...rule, ...patch } : rule)));
    };

    const removeRule = (id: string) => {
        updateRules(rules.filter((rule) => rule.id !== id));
    };

    const addRule = () => {
        const defaultColumn = columns[0];
        if (!defaultColumn) return;
        updateRules([...rules, makeDefaultFilterRule(defaultColumn) as OpsFilterRuleWithId]);
        setOpen(true);
    };

    const clearAll = () => {
        onChange({ ...value, rules: [] });
    };

    const columnOptions = React.useMemo(() => {
        const seen = new Set<string>();
        return columns
            .filter((item) => {
                if (!item.filterable || !item.source) return false;
                if (seen.has(item.source)) return false;
                seen.add(item.source);
                return true;
            })
            .map((item) => ({ value: item.source, label: item.label || item.source }));
    }, [columns]);

    return (
        <div className="ops-table-filter-bar">
            <div className="ops-table-filter-toolbar">
                <Button
                    className={`ops-filter-toggle${open ? " active" : ""}`}
                    icon={<FilterOutlined />}
                    onClick={() => setOpen((prev) => !prev)}
                >
                    <Space size={6}>
                        Filters
                        <Badge count={applied.length} showZero style={{ backgroundColor: applied.length ? "#1677ff" : "#d0d5dd" }} />
                        {open ? <UpOutlined /> : <DownOutlined />}
                    </Space>
                </Button>
                <Button icon={<SortAscendingOutlined />} onClick={onSortClick}>
                    {sortLabel}
                </Button>
                <Select
                    className="ops-filter-mode-select"
                    value={value.mode}
                    onChange={(mode) => onChange({ ...value, mode })}
                    options={[
                        { value: "or", label: "Group by: OR" },
                        { value: "and", label: "Group by: AND" },
                    ]}
                />
                <Input
                    className="ops-table-search-input"
                    prefix={<SearchOutlined />}
                    placeholder={searchPlaceholder}
                    value={value.search ?? ""}
                    onChange={(event) => onChange({ ...value, search: event.target.value })}
                    allowClear
                />
                <div className="ops-table-toolbar-spacer" />
                {metricSource ? <Tag>Metric source: {metricSource}</Tag> : null}
                <Button icon={<TableOutlined />} onClick={onColumnsClick}>
                    Columns
                </Button>
            </div>

            {open ? (
                <div className="ops-filter-builder-panel">
                    {rules.map((rule) => {
                        const column = findFilterColumn(rule.column_id, columns) ?? columns[0];
                        const columnSource = column?.source ?? rule.column_id;
                        return (
                            <div className="ops-filter-rule-row" key={rule.id}>
                                <HolderOutlined className="ops-filter-rule-drag" />
                                <Select
                                    className="ops-filter-rule-column"
                                    value={columnSource}
                                    options={columnOptions}
                                    onChange={(source) => {
                                        const nextColumn = findFilterColumn(source, columns);
                                        updateRule(rule.id, {
                                            column_id: source,
                                            operator: nextColumn ? defaultOperatorForColumn(nextColumn) : rule.operator,
                                        });
                                    }}
                                />
                                <Select
                                    className="ops-filter-rule-operator"
                                    value={rule.operator}
                                    options={FILTER_OPERATORS.map((operator) => ({
                                        value: operator,
                                        label: FILTER_OPERATOR_LABELS[operator],
                                    }))}
                                    onChange={(operator) =>
                                        updateRule(rule.id, {
                                            operator,
                                            value: operator === "is_empty" ? undefined : rule.value ?? "",
                                        })
                                    }
                                />
                                <FilterValueInput
                                    rule={rule}
                                    column={column}
                                    entityLinks={entityLinks}
                                    onChange={(next) => updateRule(rule.id, next)}
                                />
                                <Button type="text" danger icon={<DeleteOutlined />} onClick={() => removeRule(rule.id)} />
                            </div>
                        );
                    })}
                    <div className="ops-filter-builder-footer">
                        <Button type="link" icon={<PlusOutlined />} onClick={addRule}>
                            Add filter
                        </Button>
                        <div className="ops-filter-builder-footer-spacer" />
                        <Button type="link" danger onClick={clearAll}>
                            Clear all filters
                        </Button>
                        <Button type="link" onClick={() => setOpen(false)}>
                            Hide filters
                        </Button>
                    </div>
                </div>
            ) : null}

            {applied.length ? (
                <div className="ops-active-filter-row">
                    {rules
                        .filter((rule) => rule.operator === "is_empty" || Boolean(rule.value?.trim()))
                        .map((rule) => (
                            <Tag
                                key={rule.id}
                                closable
                                className={`ops-filter-chip ops-filter-chip-${chipKind(rule.column_id, columns, entityLinks)}`}
                                onClose={(event) => {
                                    event.preventDefault();
                                    removeRule(rule.id);
                                }}
                            >
                                {(() => {
                                    const parts = formatRuleParts(rule, columns);
                                    return (
                                        <>
                                            <span className="ops-filter-chip-value">{parts.label}</span>
                                            <span className="ops-filter-chip-meta">{parts.operator}</span>
                                            {parts.values.map((chipValue) => (
                                                <span className="ops-filter-chip-value" key={chipValue}>
                                                    {chipValue}
                                                </span>
                                            ))}
                                        </>
                                    );
                                })()}
                            </Tag>
                        ))}
                    <Button type="link" className="ops-inline-link" onClick={clearAll}>
                        Clear all
                    </Button>
                </div>
            ) : null}
        </div>
    );
}

export default TableFilterBar;
