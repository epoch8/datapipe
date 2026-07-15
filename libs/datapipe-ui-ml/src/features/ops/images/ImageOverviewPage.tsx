import React from "react";
import { Link } from "react-router-dom";
import { PageHeader, EmptyState } from "../shared";
import { opsApi } from "@datapipe/ui/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import type { OpsSpecSummary } from "../../../types/opsSpecs";

export function ImageOverviewPage() {
    const { pipelineId, loading: pidLoading } = usePipelineId();
    const [specs, setSpecs] = React.useState<OpsSpecSummary[]>([]);
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);

    const load = React.useCallback(() => {
        if (!pipelineId) return;
        setLoading(true);
        setError(null);
        opsApi
            .getOpsSpecs(pipelineId)
            .then((res) => setSpecs(res.specs.filter((s) => s.has_image)))
            .catch((e) => setError(String(e)))
            .finally(() => setLoading(false));
    }, [pipelineId]);

    React.useEffect(() => {
        load();
    }, [load]);

    return (
        <div className="ops-page ops-spec-page">
            <PageHeader
                breadcrumbs={[{ label: "Datapipe Ops", href: "/" }, { label: "Image" }]}
                title="Images"
                subtitle="Image views driven by ops specs"
                statusChips={[{ label: "Running", variant: "success" }, { label: "All specifications", variant: "purple" }]}
                onRefresh={load}
            />
            <EmptyState loading={pidLoading || loading} error={error ?? undefined} empty={!specs.length && !loading}>
                <div className="ops-landing-grid">
                    {specs.map((spec) => (
                        <Link key={spec.id} className="ops-landing-card" to={`/image/${encodeURIComponent(spec.id)}`}>
                            <div className="ops-landing-card-head">
                                <div style={{ marginRight: 10 }}>
                                    <div style={{ width: 10, height: 10, borderRadius: 999, background: spec.color ?? "blue" }} />
                                </div>
                                <div>
                                    <div className="ops-landing-title">{spec.title}</div>
                                    <div className="ops-landing-description">{spec.description ?? ""}</div>
                                </div>
                            </div>
                        </Link>
                    ))}
                </div>
            </EmptyState>
        </div>
    );
}

