import { AlertProps } from "antd";
import { InputRef } from "antd/lib/input/Input";
import { FilterValue } from "antd/lib/table/interface";
import { Dispatch, RefObject, SetStateAction } from "react";

interface TableColumn {
    name: string;
    type: string;
}

interface PipeTable {
    id: string;
    indexes: string[];
    size?: number | null;
    store_class: string;
    schema?: TableColumn[];
    type: string;
}

interface GraphData {
    catalog: {
        [name: string]: PipeTable;
    };
    pipeline: Node[];
    stages?: string[];
}

interface BaseNode {
    id: string;
    type: string;
    name: string;
    func?: string;
}

interface TransformNode extends BaseNode {
    type: "transform";
    inputs: string[];
    outputs: string[];
    labels?: string[][];
    transform_type?: string;
    indexes?: string[];
    transform_primary_keys?: string[];
    tpk?: string[];
    primary_keys?: string[];
    has_transform_meta?: boolean;
    total_idx_count?: number;
    changed_idx_count?: number;
}

interface MetaNode extends BaseNode {
    type: "meta";
    graph: GraphData;
    inputs?: string[];
    outputs?: string[];
    transform_type?: string;
    transform_primary_keys?: string[];
    tpk?: string[];
    labels?: string[][];
}

interface GetDataReq {
    table: string;
    page: number;
    page_size: number;
    include_total?: boolean;
    focus?: {
        table_name: string;
        items_idx: Record<string, string | number>[];
    };
    filters?: Record<string, string | number>;
    order_by?: string;
    order?: "asc" | "desc";
}

interface Options {
    total: number | null;
    page: number;
    pageSize: number;
    hasMore: boolean;
}

type IdxRow = {
    [name: string]: string | number;
};

interface FocusType {
    table_name: string;
    keys: React.Key[];
    indexes: IdxRow[];
}

interface TableLoadingOptions {
    page?: number;
    pageSize?: number;
    overFocus?: FocusType | null;
    filters?: Record<string, FilterValue | null>;
    orderBy?: string;
    order?: "asc" | "desc";
}

interface Pagination {
    page: number;
    pageSize: number;
}

interface Sorting {
    orderBy?: string;
    order?: "asc" | "desc";
}

interface FilterDropDownComponentProps {
    searchInput: RefObject<InputRef>;
    column: string;
    selectedKeys: any;
    colValue: any;
    setSelectedKeys: (keys: any) => void;
    confirm: any;
    clearFilters: any;
}

interface RunStepWebSocketComponentProps {
    transform: string;
    setAlertMsg: Dispatch<SetStateAction<AlertProps | null>>;
    tableFocus: FocusType | null | undefined;
    setDataIsProcessed: Dispatch<SetStateAction<boolean>>;
}

interface TableProps {
    current: PipeTable;
    setAlertMsg: Dispatch<SetStateAction<AlertProps | null>>;
    knownRowCount?: number | null;
    hideRunStep?: boolean;
    pipelineId?: string;
    initialColumnFilter?: { column: string; value: string };
}

interface listOfSelectedColumnsProps {
    tableFocus: FocusType | null;
}

interface RunStepRequestProps {
    transform: string;
    operation: "run-step";
    filters: IdxRow[] | null;
}

interface RunStepResponseProps {
    status: "active" | "normal" | "exception" | "success" | undefined;
    processed: number;
    total: number;
}

type Node = MetaNode | TransformNode;
export type {
    TransformNode,
    MetaNode,
    PipeTable,
    GraphData,
    GetDataReq,
    TableLoadingOptions,
    Pagination,
    Sorting,
    FilterDropDownComponentProps,
    FocusType,
    Options,
    IdxRow,
    RunStepWebSocketComponentProps,
    TableProps,
    TableColumn,
    listOfSelectedColumnsProps,
    RunStepRequestProps,
    RunStepResponseProps,
};
