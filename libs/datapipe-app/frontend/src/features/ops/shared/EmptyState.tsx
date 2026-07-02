import React from "react";
import { Alert, Empty, Spin } from "antd";

type Props = {
    loading?: boolean;
    error?: string | null;
    empty?: boolean;
    emptyMessage?: string;
    children: React.ReactNode;
};

export function EmptyState({
    loading,
    error,
    empty,
    emptyMessage = "No data available",
    children,
}: Props) {
    if (loading) {
        return (
            <div className="ops-empty-state">
                <Spin size="large" />
            </div>
        );
    }
    if (error) {
        return (
            <div className="ops-empty-state">
                <Alert type="error" message={error} showIcon />
            </div>
        );
    }
    if (empty) {
        return (
            <div className="ops-empty-state">
                <Empty description={emptyMessage} />
            </div>
        );
    }
    return <>{children}</>;
}
