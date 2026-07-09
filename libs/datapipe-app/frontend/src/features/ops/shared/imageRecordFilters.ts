import type { OpsColumn } from "../../../types/opsSpecs";

export function imageRecordFilterColumns(mode: "image" | "frozen_dataset" | "prediction"): OpsColumn[] {
    if (mode === "image") {
        return [
            { id: "image_name", label: "Image", source: "image_name", filterable: true, sortable: true },
            { id: "image_url", label: "URL", source: "image_url", filterable: true, sortable: true },
            { id: "subset", label: "Subset", source: "subset_id", kind: "chip", filterable: true, sortable: true },
        ];
    }
    if (mode === "frozen_dataset") {
        return [
            { id: "image_name", label: "Image", source: "image_name", filterable: true, sortable: true },
            { id: "subset_id", label: "Subset", source: "subset_id", kind: "chip", filterable: true, sortable: true },
            { id: "image_url", label: "URL", source: "image__image_path", filterable: true, sortable: true },
        ];
    }
    return [
        { id: "image_name", label: "Image", source: "image_name", filterable: true, sortable: true },
        { id: "subset", label: "Subset", source: "subset_id", kind: "chip", filterable: true, sortable: true },
        { id: "image_url", label: "URL", source: "image_url", filterable: true, sortable: true },
    ];
}

export const IMAGE_RECORD_ENTITY_LINKS: Record<string, string> = {
    subset: "subset_id",
};
