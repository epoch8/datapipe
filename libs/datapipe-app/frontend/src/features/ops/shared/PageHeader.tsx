import React from "react";
import { Button, DatePicker, Space, Tag } from "antd";
import { ReloadOutlined, StarOutlined } from "@ant-design/icons";
import { Link } from "react-router-dom";
import moment, { Moment } from "moment";

type StatusChip = { label: string; color?: string; variant?: "success" | "purple" | "default" };

type Props = {
    breadcrumbs?: { label: string; href?: string }[];
    title: string;
    titleTooltip?: string;
    subtitle?: string;
    statusChips?: StatusChip[];
    dateRange?: [Moment, Moment] | null;
    onDateRangeChange?: (range: [Moment, Moment] | null) => void;
    onRefresh?: () => void;
    primaryAction?: { label: string; onClick?: () => void; href?: string };
    extra?: React.ReactNode;
};

export function PageHeader({
    breadcrumbs = [],
    title,
    titleTooltip,
    subtitle,
    statusChips = [],
    dateRange,
    onDateRangeChange,
    onRefresh,
    primaryAction,
    extra,
}: Props) {
    return (
        <div className="ops-page-header">
            {breadcrumbs.length > 0 && (
                <div className="ops-breadcrumb">
                    {breadcrumbs.map((b, i) => (
                        <React.Fragment key={b.label}>
                            {i > 0 && <span className="ops-breadcrumb-sep"> / </span>}
                            {b.href ? <Link to={b.href}>{b.label}</Link> : <span>{b.label}</span>}
                        </React.Fragment>
                    ))}
                </div>
            )}
            <div className="ops-page-header-main">
                <div className="ops-page-header-left">
                    <div className="ops-page-title-row">
                        <h1 className="ops-page-title" title={titleTooltip}>
                            {title}
                        </h1>
                        <StarOutlined className="ops-page-star" />
                    </div>
                    {subtitle && <p className="ops-page-subtitle">{subtitle}</p>}
                    {statusChips.length > 0 && (
                        <Space size={8} className="ops-status-chips">
                            {statusChips.map((chip) => (
                                <Tag
                                    key={chip.label}
                                    className={`ops-status-chip ops-status-chip-${chip.variant ?? "default"}`}
                                >
                                    {chip.label}
                                </Tag>
                            ))}
                        </Space>
                    )}
                </div>
                <div className="ops-page-header-actions">
                    {dateRange && onDateRangeChange && (
                        <DatePicker.RangePicker
                            value={dateRange}
                            onChange={(vals) => {
                                if (vals?.[0] && vals?.[1]) {
                                    onDateRangeChange([vals[0], vals[1]]);
                                }
                            }}
                            format="MMM D, YYYY"
                            className="ops-date-range"
                        />
                    )}
                    {onRefresh && (
                        <Button icon={<ReloadOutlined />} onClick={onRefresh}>
                            Refresh
                        </Button>
                    )}
                    {primaryAction &&
                        (primaryAction.href ? (
                            <Link to={primaryAction.href}>
                                <Button type="primary">{primaryAction.label}</Button>
                            </Link>
                        ) : (
                            <Button type="primary" onClick={primaryAction.onClick}>
                                {primaryAction.label}
                            </Button>
                        ))}
                    {extra}
                </div>
            </div>
        </div>
    );
}

export function defaultDateRange(): [Moment, Moment] {
    return [moment().subtract(7, "days"), moment()];
}
