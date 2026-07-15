import type React from "react";

/** Minimal ops-spec shape used by the shell nav (plugins may supply richer specs). */
export type OpsSpecSummary = {
    id: string;
    title: string;
    description?: string;
    icon?: string;
    color?: string;
    has_image?: boolean;
    has_model_predictions?: boolean;
    has_frozen_datasets?: boolean;
    has_training?: boolean;
    metric_tables_count?: number;
    class_metric_tables_count?: number;
    tags?: string[];
};

export type EntityLinkProps =
    | {
          kind: "model";
          id: string;
          datasetId?: string;
          subset?: string;
          children?: React.ReactNode;
          className?: string;
      }
    | {
          kind: "dataset";
          id: string;
          subset?: string;
          children?: React.ReactNode;
          className?: string;
      };
