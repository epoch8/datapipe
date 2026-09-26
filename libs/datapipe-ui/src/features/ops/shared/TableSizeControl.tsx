import React from "react";
import { Button, Space } from "antd";

type Props = {
    size: number | null | undefined;
    loading?: boolean;
    onCount: () => void;
};

export function TableSizeControl({ size, loading, onCount }: Props) {
    return (
        <Space size={8}>
            <span>{size != null ? size.toLocaleString() : "—"}</span>
            <Button size="small" loading={loading} onClick={onCount}>
                Count size
            </Button>
        </Space>
    );
}
