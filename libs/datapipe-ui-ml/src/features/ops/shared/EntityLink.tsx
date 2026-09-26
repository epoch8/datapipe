import React from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import { useParams } from "react-router-dom";
import { opsApi } from "@datapipe/ui-ml/api/client";
import { usePipelineId } from "@datapipe/ui/hooks/usePipelineId";
import { buildDatasetUrl, buildModelUrl } from "./entityUrls";

type Props =
    | {
          kind: "model";
          id: string;
          datasetId?: string;
          subset?: string;
          specId?: string;
          children?: React.ReactNode;
          className?: string;
      }
    | {
          kind: "dataset";
          id: string;
          subset?: string;
          specId?: string;
          children?: React.ReactNode;
          className?: string;
      };

type ResolveCacheEntry = { specId: string; datasetId?: string };
const resolveCache = new Map<string, ResolveCacheEntry>();

function cacheKey(kind: "model" | "dataset", id: string, pipelineId: string) {
    return `${pipelineId}::${kind}::${id}`;
}

export function EntityLink(props: Props) {
    const { id: routePipelineId, specId: routeSpecId } = useParams<{ id?: string; specId?: string }>();
    const { pipelineId: hookPipelineId } = usePipelineId();
    const pipelineId = routePipelineId || hookPipelineId || undefined;
    const knownSpecId = props.specId || routeSpecId || undefined;
    const label = props.children ?? props.id;

    const [resolved, setResolved] = React.useState<ResolveCacheEntry | null>(() => {
        if (knownSpecId) {
            return {
                specId: knownSpecId,
                datasetId: props.kind === "model" ? props.datasetId : undefined,
            };
        }
        if (!pipelineId || !props.id) return null;
        return resolveCache.get(cacheKey(props.kind, props.id, pipelineId)) ?? null;
    });

    React.useEffect(() => {
        if (!props.id || props.id === "—") return;
        const explicitDatasetId = props.kind === "model" ? props.datasetId : undefined;
        if (knownSpecId) {
            setResolved({
                specId: knownSpecId,
                datasetId: explicitDatasetId,
            });
            return;
        }
        if (!pipelineId) return;
        const key = cacheKey(props.kind, props.id, pipelineId);
        const cached = resolveCache.get(key);
        if (cached) {
            setResolved({
                specId: cached.specId,
                datasetId: explicitDatasetId ?? cached.datasetId,
            });
            return;
        }
        let cancelled = false;
        opsApi
            .resolveMetricsEntity(pipelineId, {
                model_id: props.kind === "model" ? props.id : undefined,
                dataset_id: props.kind === "dataset" ? props.id : undefined,
            })
            .then((res) => {
                const entry: ResolveCacheEntry = {
                    specId: res.spec_id,
                    datasetId: res.dataset_id ?? explicitDatasetId,
                };
                resolveCache.set(key, entry);
                if (!cancelled) setResolved(entry);
            })
            .catch(() => {
                if (!cancelled) setResolved(null);
            });
        return () => {
            cancelled = true;
        };
    }, [knownSpecId, pipelineId, props.kind, props.id, props.kind === "model" ? props.datasetId : undefined]);

    if (!props.id || props.id === "—") {
        return <>{label}</>;
    }

    if (!resolved?.specId) {
        return <span className={["dp-entity-link", "dp-entity-link-pending", props.className].filter(Boolean).join(" ")}>{label}</span>;
    }

    const to =
        props.kind === "model"
            ? buildModelUrl(props.id, {
                  specId: resolved.specId,
                  dataset_id: props.datasetId ?? resolved.datasetId,
                  subset: props.subset,
              })
            : buildDatasetUrl(props.id, { specId: resolved.specId, subset: props.subset });

    return (
        <Tooltip title={props.id}>
            <Link to={to} className={["dp-entity-link", props.className].filter(Boolean).join(" ")}>
                {label}
            </Link>
        </Tooltip>
    );
}
