import React from "react";
import { Link, useParams } from "react-router-dom";
import { opsApi } from "../../../api/ops";
import { usePipelineId } from "../../../hooks/usePipelineId";
import type { OpsSpecDetail } from "../../../types/opsSpecs";
import { EmptyState, PageHeader } from "../shared";
import { ImageRecordsTable } from "./ImageRecordsTable";

export function ImageSpecPage() {
    const { specId: rawSpecId = "" } = useParams<{ specId?: string }>();
    const specId = decodeURIComponent(rawSpecId || "");
    const { pipelineId, loading: pidLoading } = usePipelineId();

    const [spec, setSpec] = React.useState<OpsSpecDetail | null>(null);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        if (!pipelineId || !specId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getOpsSpec(pipelineId, specId)
            .then(setSpec)
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId, specId]);

    React.useEffect(() => {
        load();
    }, [load]);

    const imageView = spec?.data?.image_view;

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[
                    { label: "Datapipe Ops", href: "/" },
                    { label: "Image", href: "/image" },
                    { label: spec?.title ?? specId },
                ]}
                title={spec?.title ?? specId}
                subtitle={spec?.description}
                onRefresh={load}
                statusChips={[{ label: spec?.title ?? specId, variant: "purple" }]}
            />

            <EmptyState loading={pidLoading || loading} error={error ?? undefined} empty={!spec && !loading}>
                {imageView ? (
                    <>
                        <div className="ops-panel ops-polished-panel" style={{ marginBottom: 16 }}>
                            <div className="ops-panel-title" style={{ marginBottom: 8 }}>
                                Image source tables
                            </div>
                            <ul style={{ margin: 0, paddingLeft: 18 }}>
                                <li>
                                    Image:{" "}
                                    <Link to={`/pipelines/${pipelineId}/tables/${imageView.image_table}`}>{imageView.image_table}</Link>
                                </li>
                                {imageView.subset_table ? (
                                    <li>
                                        Split:{" "}
                                        <Link to={`/pipelines/${pipelineId}/tables/${imageView.subset_table}`}>{imageView.subset_table}</Link>
                                    </li>
                                ) : null}
                                {imageView.ground_truth ? (
                                    <li>
                                        Ground truth:{" "}
                                        <Link to={`/pipelines/${pipelineId}/tables/${imageView.ground_truth.table}`}>{imageView.ground_truth.table}</Link>
                                    </li>
                                ) : null}
                            </ul>
                        </div>

                        <ImageRecordsTable
                            pipelineId={pipelineId}
                            specId={specId}
                            scope="data"
                            mode="image"
                            title="Images"
                        />
                    </>
                ) : null}
            </EmptyState>
        </div>
    );
}

