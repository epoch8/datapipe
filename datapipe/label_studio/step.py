from datapipe.dsl import PipelineStep


class LSModeration(PipelineStep):
    def __init__(
        self,
        ls_url,
        auth,
        project_identifier,
        project_label_config_at_create,
        annotations_column,
        inputs,
        outputs,
        chunk_size,
    ):
        pass
