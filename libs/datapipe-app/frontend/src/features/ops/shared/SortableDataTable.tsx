import React from "react";
import { Pagination, Table } from "antd";
import type { ColumnsType, TablePaginationConfig } from "antd/es/table";
import type { SorterResult } from "antd/es/table/interface";

type Props<T extends object> = {
    columns: ColumnsType<T>;
    dataSource: T[];
    rowKey: string | ((row: T) => string);
    loading?: boolean;
    total: number;
    page: number;
    pageSize: number;
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange?: (sortBy: string | undefined, sortDir: "asc" | "desc" | undefined) => void;
    rowSelection?: {
        selectedRowKeys: React.Key[];
        onChange: (keys: React.Key[]) => void;
    };
    title?: React.ReactNode;
    extra?: React.ReactNode;
    scroll?: { x?: number | string };
};

export function SortableDataTable<T extends object>({
    columns,
    dataSource,
    rowKey,
    loading,
    total,
    page,
    pageSize,
    onPageChange,
    onSortChange,
    rowSelection,
    title,
    extra,
    scroll,
}: Props<T>) {
    const handleChange = (
        _pagination: TablePaginationConfig,
        _filters: Record<string, (string | number | boolean)[] | null>,
        sorter: SorterResult<T> | SorterResult<T>[],
    ) => {
        if (!onSortChange) return;
        const s = Array.isArray(sorter) ? sorter[0] : sorter;
        if (!s?.field) {
            onSortChange(undefined, undefined);
            return;
        }
        const field = String(s.field);
        const order = s.order === "ascend" ? "asc" : s.order === "descend" ? "desc" : undefined;
        onSortChange(field, order);
    };

    return (
        <div className="ops-data-table">
            {(title || extra) && (
                <div className="ops-data-table-toolbar">
                    <div className="ops-data-table-title">{title}</div>
                    <div>{extra}</div>
                </div>
            )}
            <Table
                columns={columns}
                dataSource={dataSource}
                rowKey={rowKey}
                loading={loading}
                pagination={false}
                rowSelection={rowSelection}
                onChange={handleChange}
                scroll={scroll}
                size="middle"
                className="ops-table"
            />
            <div className="ops-table-pagination">
                <Pagination
                    current={page}
                    pageSize={pageSize}
                    total={total}
                    showSizeChanger
                    showTotal={(t, range) => `${range[0]}–${range[1]} of ${t}`}
                    onChange={onPageChange}
                />
            </div>
        </div>
    );
}
