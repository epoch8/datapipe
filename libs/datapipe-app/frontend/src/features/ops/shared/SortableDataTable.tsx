import React from "react";
import { Pagination, Table } from "antd";
import type { ColumnsType, TablePaginationConfig } from "antd/es/table";
import type { SorterResult, SortOrder } from "antd/es/table/interface";
import type { SortSpec } from "./sortUtils";

export type { SortSpec };

type Props<T extends object> = {
    columns: ColumnsType<T>;
    dataSource: T[];
    rowKey: string | ((row: T) => string);
    loading?: boolean;
    total: number;
    page: number;
    pageSize: number;
    onPageChange: (page: number, pageSize: number) => void;
    onSortChange?: (sorts: SortSpec[]) => void;
    activeSorts?: SortSpec[];
    multiSort?: boolean;
    rowSelection?: {
        selectedRowKeys: React.Key[];
        onChange: (keys: React.Key[]) => void;
    };
    title?: React.ReactNode;
    extra?: React.ReactNode;
    scroll?: { x?: number | string };
};

function columnField<T extends object>(col: ColumnsType<T>[number]): string | undefined {
    if ("children" in col) return undefined;
    if (col.key != null) return String(col.key);
    const di = col.dataIndex;
    if (di == null) return undefined;
    return Array.isArray(di) ? di.join(".") : String(di);
}

function sortersFromChange<T extends object>(sorter: SorterResult<T> | SorterResult<T>[]): SortSpec[] {
    // antd derives `field` from a column's dataIndex; columns that only set `key`
    // (e.g. computed metric columns) leave `field` undefined, so fall back to
    // `columnKey` to keep server-side sorting working for them.
    const list = (Array.isArray(sorter) ? sorter : [sorter]).filter(
        (s) => (s.field ?? s.columnKey) != null && s.order,
    );
    return list.map((s) => ({
        field: String(s.field ?? s.columnKey),
        direction: s.order === "ascend" ? "asc" : "desc",
    }));
}

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
    activeSorts,
    multiSort = false,
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
        const specs = sortersFromChange(sorter);
        onSortChange(multiSort ? specs : specs.slice(0, 1));
    };

    const resolvedColumns = React.useMemo(
        () =>
            columns.map((col) => {
                const field = columnField(col);
                if (!field || !("sorter" in col) || !col.sorter) return col;
                const spec = activeSorts?.find((s) => s.field === field);
                const sortOrder: SortOrder | undefined = spec
                    ? spec.direction === "asc"
                        ? "ascend"
                        : "descend"
                    : undefined;
                return { ...col, sortOrder };
            }),
        [columns, activeSorts],
    );

    return (
        <div className="ops-data-table">
            {(title || extra) && (
                <div className="ops-data-table-toolbar">
                    <div className="ops-data-table-title">{title}</div>
                    <div>{extra}</div>
                </div>
            )}
            <Table
                columns={resolvedColumns}
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
