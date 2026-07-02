import React from "react";
import { Badge, Button, Input, Select, Space } from "antd";
import { FilterOutlined, SearchOutlined } from "@ant-design/icons";

export type FilterOption = { label: string; value: string };

export type FilterDef = {
    key: string;
    label: string;
    options: FilterOption[];
    value?: string;
    placeholder?: string;
    mode?: "multiple";
};

type Props = {
    filters: FilterDef[];
    onFilterChange: (key: string, value: string | string[] | undefined) => void;
    search?: string;
    onSearchChange?: (value: string) => void;
    searchPlaceholder?: string;
    advancedCount?: number;
    onAdvancedClick?: () => void;
};

export function FilterBar({
    filters,
    onFilterChange,
    search,
    onSearchChange,
    searchPlaceholder = "Search…",
    advancedCount = 0,
    onAdvancedClick,
}: Props) {
    return (
        <div className="ops-filter-bar">
            <Space wrap size={12}>
                {filters.map((f) => (
                    <div key={f.key} className="ops-filter-item">
                        <span className="ops-filter-label">{f.label}</span>
                        <Select
                            allowClear
                            mode={f.mode}
                            placeholder={f.placeholder ?? f.label}
                            value={f.value}
                            style={{ minWidth: 140 }}
                            options={f.options}
                            onChange={(v) => onFilterChange(f.key, v)}
                        />
                    </div>
                ))}
                {onSearchChange && (
                    <Input
                        prefix={<SearchOutlined />}
                        placeholder={searchPlaceholder}
                        value={search}
                        onChange={(e) => onSearchChange(e.target.value)}
                        style={{ width: 240 }}
                        allowClear
                    />
                )}
                {onAdvancedClick && (
                    <Button icon={<FilterOutlined />} onClick={onAdvancedClick}>
                        Filters
                        {advancedCount > 0 && <Badge count={advancedCount} style={{ marginLeft: 8 }} />}
                    </Button>
                )}
            </Space>
        </div>
    );
}
