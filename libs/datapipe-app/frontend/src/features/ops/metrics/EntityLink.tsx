import React from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "antd";
import { useParams } from "react-router-dom";
import { buildDatasetUrl, buildModelUrl, truncateMiddle } from "./entityUrls";

type Props =
    | { kind: "model"; id: string; datasetId?: string; subset?: string; children?: React.ReactNode; className?: string }
    | { kind: "dataset"; id: string; subset?: string; children?: React.ReactNode; className?: string };

export function EntityLink(props: Props) {
    const { id: routePipelineId } = useParams<{ id?: string }>();
    const pipelineId = routePipelineId || undefined;
    const label = props.children ?? truncateMiddle(props.id);

    if (!props.id || props.id === "—") {
        return <>{label}</>;
    }

    const to =
        props.kind === "model"
            ? buildModelUrl(props.id, pipelineId, { dataset_id: props.datasetId, subset: props.subset })
            : buildDatasetUrl(props.id, pipelineId, { subset: props.subset });

    return (
        <Tooltip title={props.id}>
            <Link to={to} className={["dp-entity-link", props.className].filter(Boolean).join(" ")}>
                {label}
            </Link>
        </Tooltip>
    );
}
