# 0.4.0
* Added Python3.11 support, datapipe-core dependency 0.14.8+
* Label Studio 1.22.0+ support
* Major refactoring:
- `LabelStudioStep` -> `LabelStudioUploadTasks`
- Added `LabelStudioUploadPredictions`
- For questions with migration ask @bobokvsky
* Added Storages support (S3 or GCS) for `LabelStudioUploadTasks`

# 0.3.5

* Update datapipe-core dependency 0.14+

# 0.3.4

* Update label_studio_sdk dependency (allow >2 pydantic)

# 0.3.3

* Update python dependency (allow 3.11 python)

# 0.3.2

* Update Pandas dependency (allow 2+ version)

# 0.3.1

* Add drop_duplicates on fetch data from LabelStudio (get_annotations_from_ls)

# 0.3.0

# 0.3.0
* Update datapipe-core version (0.13.0-alpha.4)
* `LabelStudioStep` now supports deleting tasks in `upload` table. In `output` table older annotations are not deleted.
* Added argument `delete_unannotated_tasks_only_on_update` in `LabelStudioStep`.
* Removed some dependencies

# 0.2.2
* Update datapipe-core version (0.12.0)

# 0.2.1
* Removed `sqlite3` from required dependencies in pyproject.toml

# 0.2.0

* Add `name` parameter to `LabelStudioStep`
* Add name prefixes to transformation steps
* Add workaround for `500` from LS when trying to delete non-existent task
* Remove cyclic dependency on input_uploader_dt for upload task

# 0.1.0

* First version
