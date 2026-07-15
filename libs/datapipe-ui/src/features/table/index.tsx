import React, {
    useCallback,
    useEffect,
    useState,
    useRef,
    SetStateAction,
    Dispatch,
    RefObject,
    FC,
} from "react";
import {
    Button,
    Table as AntTable,
    TablePaginationConfig,
    Input,
    Space,
    InputRef,
    Progress,
    AlertProps,
    Select,
} from "antd";
import { ColumnsType } from "antd/lib/table";
import {
    PipeTable,
    GetDataReq,
    FilterDropDownComponentProps,
    Pagination,
    Sorting,
    TableLoadingOptions,
    FocusType,
    Options,
    IdxRow,
    TableProps,
    listOfSelectedColumnsProps,
    RunStepRequestProps,
    RunStepResponseProps,
    RunStepWebSocketComponentProps,
} from "../../types";
import { FilterValue, SorterResult } from "antd/lib/table/interface";
import { getDefaultTablePageSize, setDefaultTablePageSize } from "../../api/graph";
import { EntityLink } from "@datapipe/ui-ml/features/ops/metrics/EntityLink";

function renderCellValue(value: unknown, columnName: string, pipelineId?: string): React.ReactNode {
    if (value === null || value === undefined) {
        return value as null | undefined;
    }
    if (value === "—" || value === "" || value === false) {
        return value as React.ReactNode;
    }
    if (typeof value === "number" && value === 0) {
        return "0";
    }
    const isModelIdColumn = columnName === "model_id" || columnName.endsWith("_model_id");
    const isFrozenDatasetIdColumn =
        columnName === "frozen_dataset_id" || columnName.endsWith("_frozen_dataset_id");
    if (typeof value === "string" && isModelIdColumn) {
        return <EntityLink kind="model" id={value} />;
    }
    if (typeof value === "string" && isFrozenDatasetIdColumn) {
        return <EntityLink kind="dataset" id={value} />;
    }
    if (typeof value === "object") {
        return (
            <pre
                style={{
                    fontSize: 10,
                    margin: 0,
                    maxHeight: 120,
                    overflow: "auto",
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                }}
            >
                {JSON.stringify(value, null, 2)}
            </pre>
        );
    }
    if (typeof value === "boolean") {
        return value ? "True" : "False";
    }
    return value as React.ReactNode;
}

const RunStepWebSocketComponent: FC<RunStepWebSocketComponentProps> = ({
    transform,
    setAlertMsg,
    tableFocus,
    setDataIsProcessed,
}) => {
    const [data, setData] = useState<RunStepResponseProps>({
        status: undefined,
        processed: 0,
        total: 100,
    });
    const [ws, setWs] = useState<WebSocket | null>(null);

    const handleSendMessage = () => {
        const payload: RunStepRequestProps = {
            transform: transform,
            operation: "run-step",
            filters: tableFocus?.indexes || null,
        };
        if (ws) {
            ws.send(JSON.stringify(payload));
        }
    };

    useEffect(() => {
        const ws = new WebSocket(
            `${process.env["REACT_APP_WEBSOCKET_URL"]}${transform}/run-status`,
        );
        setWs(ws);
        ws.onopen = () => { };
        ws.onmessage = (event) => {
            const msg = JSON.parse(event.data);
            let status: "active" | "success" | undefined;
            let processed: number = 0;
            let total: number = 0;
            if (msg.status === "not found" || msg.status === "not allowed") {
                setAlertMsg({
                    type: "warning",
                    message: msg.status,
                } as AlertProps);
                return;
            }
            if (msg.status === "running") {
                status = "active";
                processed = msg.processed;
                total = msg.total;
            } else if (msg.status === "finished") {
                processed = msg.processed;
                total = msg.total;
                status = "success";
                setDataIsProcessed(true);
            }
            const runStepResponse: RunStepResponseProps = {
                status: status,
                processed: processed,
                total: total,
            };
            setData(runStepResponse);
        };

        ws.onerror = (event) => {
            const runStepError: RunStepResponseProps = {
                status: "exception",
                processed: data.processed,
                total: data.total,
            };
            setData(runStepError);
            console.error("WebSocket error:", event);
        };

        ws.onclose = () => {
            console.log("WebSocket closed");
        };

        return () => {
            ws.close();
        };
    }, [transform]);

    return (
        <>
            {(data.status === "active" || data.status === "success") && (
                <>
                    <div>Total: {data.total}</div>
                    <div>Processed: {data.processed}</div>
                    <Progress
                        status={data.status}
                        percent={Math.round(
                            (data.processed * 100) / data.total,
                        )}
                        size="small"
                    ></Progress>
                </>
            )}
            <Button type="primary" onClick={handleSendMessage}>
                Run Step
            </Button>
        </>
    );
};

const FilterDropDownComponent: FC<FilterDropDownComponentProps> = ({
    searchInput,
    column,
    selectedKeys,
    colValue,
    setSelectedKeys,
    confirm,
    clearFilters,
}) => {
    const handleReset = (clearFilters: () => void) => {
        clearFilters();
    };
    return (
        <div style={{ padding: 8 }}>
            <Input
                ref={searchInput}
                placeholder={`Search ${column}`}
                value={selectedKeys[0]}
                onChange={(e) => {
                    switch (typeof colValue) {
                        case "number":
                            setSelectedKeys(
                                e.target.value ? [e.target.value] : [],
                            ); //TODO OnlyNumbers
                            break;
                        case "string":
                            setSelectedKeys(
                                e.target.value ? [e.target.value] : [],
                            );
                    }
                }}
                onPressEnter={() => confirm()}
                style={{
                    marginBottom: 8,
                    display: "block",
                }}
            />
            <Space>
                <Button
                    type="primary"
                    onClick={() => confirm()}
                    size="small"
                    style={{
                        width: 90,
                    }}
                >
                    Search
                </Button>
                <Button
                    onClick={() => {
                        clearFilters && handleReset(clearFilters);
                        confirm();
                    }}
                    size="small"
                    style={{
                        width: 90,
                    }}
                >
                    Reset
                </Button>
            </Space>
        </div>
    );
};

const ListOfSelectedColumns: FC<listOfSelectedColumnsProps> = ({
    tableFocus,
}) => {
    const defaultValue = 5;
    const [isLoadAll, setIsLoadAll] = useState<boolean>(false);
    const [showCount, setShowCount] = useState<number>(defaultValue);

    if (!tableFocus) {
        return null;
    }

    const toggleShowAll = () => {
        setIsLoadAll(!isLoadAll);
        if (!isLoadAll) {
            setShowCount(tableFocus.indexes.length);
        } else {
            setShowCount(defaultValue);
        }
    };

    return (
        <div style={{ margin: "8px" }}>
            {(tableFocus.indexes || [])
                .slice(0, isLoadAll ? tableFocus.indexes?.length : showCount)
                .map((idx, index) => (
                    <div key={index}>
                        {Object.entries(idx).map(([key, val], index) => (
                            <span key={index}>
                                <strong>{key}</strong>=<strong>{val}</strong>
                                &nbsp;
                            </span>
                        ))}
                    </div>
                ))}
            {tableFocus.indexes?.length && (
                <Button size="small" type="primary" onClick={toggleShowAll}>
                    {isLoadAll ? "Show Less" : "Show All"}
                </Button>
            )}
        </div>
    );
};

const loadTable = async (
    searchInput: RefObject<InputRef>,
    setLoading: Dispatch<SetStateAction<boolean>>,
    setData: Dispatch<SetStateAction<any>>,
    setColumns: Dispatch<SetStateAction<ColumnsType<any>>>,
    setOptions: Dispatch<SetStateAction<Options>>,
    current: PipeTable,
    options: Options,
    loadingsOptions?: TableLoadingOptions,
    tableFocus?: FocusType,
    knownRowCount?: number | null,
    pipelineId?: string,
) => {
    loadingsOptions = loadingsOptions ?? ({} as TableLoadingOptions);
    const page = loadingsOptions.page ?? 1;
    const pageSize = loadingsOptions.pageSize;
    const overFocus = loadingsOptions.overFocus;
    const _filters = loadingsOptions.filters;
    let data: any;

    setLoading(true);
    const _focus = overFocus === null ? null : overFocus ?? tableFocus;

    const postBody = {
        table: current.id,
        page: page - 1,
        page_size: pageSize || options.pageSize,
        order_by: loadingsOptions.orderBy,
        order: loadingsOptions.order,
        include_total: false,
    } as GetDataReq;

    if (_focus && _focus.table_name !== current.id) {
        postBody.focus = {
            table_name: _focus.table_name,
            items_idx: _focus.indexes,
        };
    }
    if (_filters) {
        postBody.filters = {};
        const respFilters = {} as { [key: string]: string | number };
        let flag = false;
        Object.entries(_filters).forEach(([tableName, vals]) => {
            if (vals && vals[0] && typeof vals[0] !== "boolean") {
                respFilters[tableName] = vals[0] as string | number;
                flag = true;
            }
        });
        if (flag) {
            postBody.filters = respFilters;
        }
    }

    try {
        let reqUrl: string;
        let body = postBody as any; //quickfix for different (table_name, table) field names
        if (current.type === "transform") {
            reqUrl = process.env["REACT_APP_GET_TRANSFORM_URL"] as string;
        } else {
            reqUrl = process.env["REACT_APP_GET_TABLE_URL"] as string;
        }
        const response = await fetch(reqUrl, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(body),
        });
        data = await response.json();
    } catch (er) {
        console.error(er);
        setLoading(false);
        setData([]);
        return;
    }
    if (!data?.data) {
        setLoading(false);
        setData([]);
        return;
    }
    if (data.data.length === 0) {
        setLoading(false);
        setData([]);
        const effectivePageSize = data.page_size ?? options.pageSize;
        const currentPage = (data.page ?? 0) + 1;
        const total = knownRowCount ?? data.total ?? null;
        const hasMore =
            total != null
                ? currentPage * effectivePageSize < total
                : false;
        setOptions({
            total,
            page: currentPage,
            pageSize: effectivePageSize,
            hasMore,
        });
        return;
    }

    const sampleRow = data.data[0];
    if (!sampleRow || typeof sampleRow !== "object") {
        setLoading(false);
        setData([]);
        return;
    }

    const pkKeys = new Set(current.indexes ?? []);

    setColumns(
        Object.entries(sampleRow).map(([column, colValue]) => {
            const isPk = pkKeys.has(column);
            return {
                title: isPk ? (
                    <span className="dp-col-title">
                        {column}
                        <span className="dp-col-pk-badge">PK</span>
                    </span>
                ) : (
                    column
                ),
                dataIndex: column,
                className: isPk ? "dp-pk-col" : undefined,
                sorter: typeof colValue !== "object",
                render: (value: unknown) => renderCellValue(value, column, pipelineId),
                filterDropdown:
                    typeof colValue !== "object" &&
                    (({
                        setSelectedKeys,
                        selectedKeys,
                        confirm,
                        clearFilters,
                    }) => {
                        return (
                            <FilterDropDownComponent
                                searchInput={searchInput}
                                column={column}
                                selectedKeys={selectedKeys}
                                colValue={colValue}
                                setSelectedKeys={setSelectedKeys}
                                confirm={confirm}
                                clearFilters={clearFilters}
                            />
                        );
                    }),
                filteredValue: loadingsOptions?.filters
                    ? loadingsOptions.filters[column]
                    : null,
                onFilterDropdownOpenChange: (visible: any) => {
                    if (visible) {
                        setTimeout(() => searchInput.current?.select(), 100);
                    }
                },
            };
        }),
    );

    setData(
        data.data.map((element: any, index: number) => ({ ...element, index })),
    );
    setLoading(false);
    const effectivePageSize = data.page_size ?? options.pageSize;
    const currentPage = data.page + 1;
    const total = knownRowCount ?? data.total ?? null;
    const hasMore =
        total != null
            ? currentPage * effectivePageSize < total
            : data.data.length >= effectivePageSize;
    setOptions({
        total,
        page: currentPage,
        pageSize: effectivePageSize,
        hasMore,
    });
};

const PAGE_SIZE_OPTIONS = [5, 10, 20, 50, 100];

const Table: FC<TableProps> = ({
    current,
    setAlertMsg,
    knownRowCount = null,
    hideRunStep = false,
    pipelineId,
    initialColumnFilter,
}) => {
    const [columns, setColumns] = useState<ColumnsType<any>>([]);
    const [data, setData] = useState<any>();
    const [loading, setLoading] = useState(false);
    const [tableFocus, setTableFocus] = useState<FocusType>();
    const [options, setOptions] = useState<Options>({
        total: knownRowCount,
        page: 1,
        pageSize: getDefaultTablePageSize(),
        hasMore: false,
    });
    const [pagination, setPagination] = useState<Pagination>({
        page: 1,
        pageSize: getDefaultTablePageSize(),
    });
    const [sorting, setSorting] = useState<Sorting>({
        orderBy: undefined,
        order: undefined,
    });
    const [filteredInfo, setFilteredInfo] = useState<
        Record<string, FilterValue | null>
    >(initialColumnFilter ? { [initialColumnFilter.column]: [initialColumnFilter.value] } : {});
    const [dataIsProcessed, setDataIsProcessed] = useState<boolean>(false);
    const skipRenderFlag = useRef(true);
    const searchInput = useRef<InputRef>(null);

    const rowSelection = {
        onChange: (selectedRowKeys: React.Key[], selectedRows: any[]) => {
            const newFocus = {
                table_name: current.id,
                keys: selectedRowKeys,
                indexes: selectedRows.map((row) => {
                    return current.indexes.reduce((acc, index) => {
                        acc[index] = row[index];
                        return acc;
                    }, {} as IdxRow);
                }),
            };
            skipRenderFlag.current = true;
            setTableFocus(newFocus);
        },
    };

    const changeHandler = useCallback(
        (
            newPagination: TablePaginationConfig,
            newFilters: Record<string, FilterValue | null>,
            newSorter: SorterResult<any>[] | SorterResult<any>,
        ) => {
            if (newSorter) {
                const sorter = Array.isArray(newSorter)
                    ? newSorter[0]
                    : newSorter;
                setSorting({
                    orderBy: Array.isArray(sorter.field)
                        ? sorter.field[0]
                        : sorter.field,
                    order: sorter.order === "descend" ? "desc" : "asc",
                });
            }
            if (newPagination.current && newPagination.pageSize) {
                setPagination({
                    page: Math.max(1, newPagination.current),
                    pageSize: newPagination.pageSize,
                });
            }
            setFilteredInfo(newFilters);
        },
        [],
    );

    const clearFocus = useCallback(() => {
        setTableFocus(undefined);
    }, [current, options, tableFocus]);

    useEffect(() => {
        setFilteredInfo(
            initialColumnFilter ? { [initialColumnFilter.column]: [initialColumnFilter.value] } : {},
        );
        setSorting({});
        const pageSize = getDefaultTablePageSize();
        setPagination({
            page: 1,
            pageSize,
        });
        setData([]);
        setColumns([]);
        skipRenderFlag.current = false;
        loadTable(
            searchInput,
            setLoading,
            setData,
            setColumns,
            setOptions,
            current,
            options,
            { page: 1, pageSize },
            tableFocus,
            knownRowCount,
            pipelineId,
        );
    }, [current.id, initialColumnFilter?.column, initialColumnFilter?.value]);

    useEffect(() => {
        setOptions((prev) => ({ ...prev, total: knownRowCount }));
    }, [knownRowCount]);

    useEffect(() => {
        if (skipRenderFlag.current) {
            skipRenderFlag.current = false;
            return;
        }
        loadTable(
            searchInput,
            setLoading,
            setData,
            setColumns,
            setOptions,
            current,
            options,
            {
                orderBy: sorting.orderBy,
                order: sorting.order,
                page: pagination.page,
                pageSize: pagination.pageSize,
                filters: filteredInfo,
            },
            tableFocus,
            knownRowCount,
            pipelineId,
        );
    }, [filteredInfo, tableFocus, pagination, dataIsProcessed, sorting.orderBy, sorting.order, knownRowCount, pipelineId]);

    const totalPages =
        options.total != null && options.total >= 0
            ? Math.max(1, Math.ceil(options.total / pagination.pageSize))
            : null;

    return (
        <>
            {current.type === "transform" && !hideRunStep && (
                <RunStepWebSocketComponent
                    transform={current.id}
                    setAlertMsg={setAlertMsg}
                    tableFocus={tableFocus}
                    setDataIsProcessed={setDataIsProcessed}
                />
            )}
            <div
                style={{
                    height: tableFocus ? "fit-content" : 1,
                    opacity: tableFocus ? 1 : 0,
                    transition: ".2s all ease-out",
                    overflow: "hidden",
                }}
            >
                <div style={{ color: "red" }}>
                    <strong>Focus mode</strong>
                </div>
                {tableFocus && (
                    <>
                        table: <strong>{tableFocus?.table_name}&nbsp;</strong>
                        indexes:&nbsp;
                        <div>
                            <ListOfSelectedColumns tableFocus={tableFocus} />
                        </div>
                        <Space>
                            <Button size="small" onClick={clearFocus}>
                                Clear
                            </Button>
                        </Space>
                    </>
                )}
            </div>
            {Object.values(filteredInfo).length > 0 &&
                (!data || data.length === 0) && (
                    <Button
                        onClick={() => {
                            setFilteredInfo({});
                        }}
                    >
                        Clear filters
                    </Button>
                )}
            <div style={{ display: "flex", justifyContent: "flex-end", marginBottom: 8 }}>
                <Space>
                    <Button
                        size="small"
                        disabled={pagination.page <= 1 || loading}
                        onClick={() =>
                            setPagination((prev) => ({
                                ...prev,
                                page: Math.max(1, prev.page - 1),
                            }))
                        }
                    >
                        Previous
                    </Button>
                    <span>
                        Page {pagination.page}
                        {totalPages != null ? ` of ${totalPages}` : ""}
                    </span>
                    <Button
                        size="small"
                        disabled={!options.hasMore || loading}
                        onClick={() =>
                            setPagination((prev) => ({
                                ...prev,
                                page: prev.page + 1,
                            }))
                        }
                    >
                        Next
                    </Button>
                    <Select
                        size="small"
                        value={pagination.pageSize}
                        options={PAGE_SIZE_OPTIONS.map((value) => ({ value, label: `${value} / page` }))}
                        onChange={(pageSize) => {
                            setDefaultTablePageSize(pageSize);
                            setPagination({ page: 1, pageSize });
                        }}
                    />
                    {options.total != null && (
                        <span style={{ color: "rgba(0,0,0,0.45)" }}>
                            {options.total.toLocaleString()} rows
                        </span>
                    )}
                </Space>
            </div>
            <AntTable
                loading={loading}
                showHeader={!loading && data?.length > 0}
                onChange={changeHandler}
                pagination={false}
                rowKey={(record) => {
                    if (current.indexes.length > 0) {
                        const idx_string = current.indexes.reduce((acc, value) => {
                            acc += value + "_" + record[value] + "_";
                            return acc;
                        }, "");
                        return `${current.id}_${idx_string}`;
                    }
                    return `${current.id}_row_${record.index ?? JSON.stringify(record)}`;
                }}
                rowSelection={
                    tableFocus && current.id !== tableFocus.table_name
                        ? undefined
                        : {
                            type: "checkbox",
                            selectedRowKeys: tableFocus
                                ? tableFocus.keys
                                : [],
                            preserveSelectedRowKeys: true,
                            ...rowSelection,
                        }
                }
                size="small"
                style={{ width: "100%" }}
                columns={columns}
                dataSource={loading ? [] : (data ?? [])}
            />
        </>
    );
};

export { Table };
