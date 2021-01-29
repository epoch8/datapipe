from label_studio.storage.base import register_storage
from c12n_pipe.label_studio_utils.filesystem_modified import (
    ExternalTasksJSONStorageModified, CompletionsDirStorageModified,
    ExternalTasksJSONStorageModifiedNoSetNoRemove
)
register_storage('tasks-json-modified', ExternalTasksJSONStorageModified)
register_storage('tasks-json-modified-no-set-no-remove', ExternalTasksJSONStorageModifiedNoSetNoRemove)
register_storage('completions-dir-modified', CompletionsDirStorageModified)

