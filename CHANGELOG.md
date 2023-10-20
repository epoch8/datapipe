# WIP 0.13.5

* Add create_engine_kwargs for `DBConn`

# 0.13.4

* Fix `TableStoreFiledir` usage of `auto_mkdir` (enable only for "file://")

# 0.13.3

* Fix `TableStoreFiledir` ignoring `fsspec_kwargs`
* Added dropna and idx check for `TransformMetaTable`

# 0.13.2-post.1

* Allow `pandas >= 2` and `numpy >= 1.21`

# 0.13.2

* Add `GPU` support for RayExecutor
* Add `auto_mkdir` to `TableStoreFiledir`, fixes issues with local filedir
* Add Python 3.11 support.

# 0.13.1

* Add `api_key` to `QdrantStore` constructor. Now can run pipelines with Qdrant
  authentication
* Fix `TableStoreDB.update_rows` method crashing when trying to store pandas
  none-types

# 0.13.0

## Changes

### Core

* Add `datapipe.metastore.TransformMetaTable`. Now each transform gets it's own
  meta table that tracks status of each transformation
* Generalize `BatchTransform` and `DatatableBatchTransform` through
  `BaseBatchTransformStep`
* Add `transform_keys` to `*BatchTransform`
* Move changed idx computation out of `DataStore` to `BaseBatchTransformStep`
* Add column `priority` to transform meta table, sort work by priority
* Switch from vanilla `tqdm` to `tqdm_loggable` for better display in logs
* `TableStoreFiledir` constructor accepts new argument `fsspec_kwargs`
* Add `filters`, `order_by`, `order` arguments to `*BatchTransformStep`
* Add magic injection of `ds`, `idx`, `run_config` to transform function via
  parameters introspection to `BatchTransform`
* Add magic `ds` inject into `BatchGenerate`
* Split `core_steps` into `step.batch_transform`, `step.batch_generate`,
  `step.datatable_transform`, `step.update_external_table`
* Move `metatable.MetaTable` to `datatable`
* Enable WAL mode for sqlite database by default

### CLI

* Add `step reset-metadata` CLI command
* Add `step fill-metadata` CLI command that populates transform meta-table with
  all indices to process
* Add `step run-idx` CLI command
* CLI `step run_changelist` command accepts new argument `--chunk-size`
* New CLI command `table migrate_transform_tables` for `0.13` migration
* Add `--start-step` parameter to `step run-changelist` CLI
* Move `--executor` parameter from `datapipe step` to `datapipe` command

### Execution

* Executors: `datapipe.executor.SingleThreadExecutor`,
  `datapipe.executor.ray.RayExecutor`

## Bugfixes

* Fix `QdrantStore.read_rows` when no idx is specified
* Fix `RedisStore` serialization for Ray

# 0.13.0-beta.4

* Fix `RedisStore` serialization for Ray

# 0.13.0-beta.3

* Enable WAL mode for sqlite database by default

# 0.13.0-beta.2

* Refactor all database writes to `insert on conflict update`
* Remove check for non-overlapping input indices because they are supported now
* Add `transform_keys` to `DatatableBatchTransform`
* Fix `BatchTransformStep.get_full_process_ids` ids duplication
* Add `MetaTable.get_changed_rows_count_after_timestamp`

# 0.13.0-beta.1

* Split `core_steps` into `step.batch_transform`, `step.batch_generate`,
  `step.datatable_transform`, `step.update_external_table`
* Move `metatable.MetaTable` to `datatable`

# 0.13.0-alpha.8

* Fix SingleThreadExecutor initialization
* Fix CLI `table migrate-transform-tables` for complex case
* Add magic `ds` inject into `BatchGenerate`

# 0.13.0-alpha.7

* Try to setup logging in RayExecutor (fails so far)
* Lazy initialisation of Ray to speedup things in CLI
* Add `ExecutorConfig.parallelism` parameter
* Add `name` parameter to `executor.run_process_batch` to customize task name in
  ray dashboard
* Migrate `run_changelist` to executor, possible parallelisation
* Limit number of in-flight Ray tasks in one `run_process_batch` to 100
* Fix batch count in tqdm in `run_changelist`

* Add `--start-step` parameter to `step run-changelist` CLI
* Move `--executor` parameter from `datapipe step` to `datapipe` command

# 0.13.0-alpha.6

* Move batch functions to `BaseBatchTransformStep`
* fix index_difference index assert

# 0.13.0-alpha.5

* Allow passing empty dfs when idx is passed to func

# 0.13.0-alpha.4

* `TableStoreFiledir` constructor accepts new argument `fsspec_kwargs`
* Add `filters`, `order_by`, `order` arguments to `*BatchTransformStep`
* Add magic injection of `ds`, `idx`, `run_config` to transform function via
  parameters introspection
* CLI `step run_changelist` command accepts new argument `--chunk-size`
* New CLI command `table migrate_transform_tables` for `0.13` migration

# 0.13.0-alpha.3

* Switch from vanilla `tqdm` to `tqdm_loggable` for better display in logs

# 0.12.0

## Breaking changes
* Move cli from `datapipe-app` to `datapipe`
* Remove separate `datapipe step status` command, now it's a flag: `datapipe
  step list --status`
* `DatatableTransform` moved from `datapipe.compute` to `datapipe.core_steps`
* Remove `datapipe.metastore.MetaTableData` (nobody used it anyway)

## New features

* Add command `step run_changelist` to CLI
* Add `datapipe.store.qdrant.QdrantStore`

## Refactorings
* Add `labels` arg and property to `ComputeStep` base class
* Add `labels` arg to `BatchTransform` and `BatchTransformStep`
* Add `labels` arg to `BatchGenerate` and `DatatableTransformStep`
* Add `labels` arg to `UpdateExternalTable` and `DatatableTransformStep`
* Large refactoring, `ComputeStep` now contains pieces of overridable functions
  for `run_full` and `run_changelist`
* Add prototype events logging for steps, add
  `event_logger.log_step_full_complete`, add table `datapipe_step_events`

# 0.11.11

* Add formatting through `traceback_with_variables` in `event_logger`
* Add exception traceback to debug log output
* Install sqlite from `pysqlite3-binary` package; Add `sqlite` extra; Update
  examples; Add `docs/sqlite.md` document 
* Update type annotations for `mypy >= 0.991`

# 0.11.10

* Fix `RedisStore` and `TableDataSingleFileStore` `.read_rows` in case when
  nonexistent rows are requested, align behavior with `TableStoreDB`
* Add test for nonexistent rows read

# 0.11.9

* New method `DataTable.reset_metadata()`
* Add `idx` logging on errors
* Add specialized implementation for `DataStoreFiledir` when file contents are
  not needed
* Add trace span for getting ids to process
* Add protocol in `TableStoreFiledir._read_rows_fast`
* `try/except` for `DatatableTransformStep`
* Add check for indexes uniqueness in `DataTable.store_chunk`

# 0.11.8

* Fix metadata columns in case of empty `get_metadata`

# 0.11.7

* Fix `DataTable.get_data` for large batches
* Refactored implementation for chunked storage in `MetaTable` and
  `TableStoreDB`
* Switched default Python version to 3.9
* Fix `DataTable.get_data` indices in returned DataFrame

# 0.11.6

* Fix `RedisStore.read_rows`

# 0.11.5

* Force initialization of all tables in catalog in `build_compute`
* Do not throw `NotImplementedError` in `run_changelist`
* Fix some more Pandas warnings in `metastore`
* New table store: `RedisStore`

# 0.11.4

* Fix [#178](https://github.com/epoch8/datapipe/issues/178)

# 0.11.3

* Added `DataTable.get_size()` method

# 0.11.2

* `DatatableTransform`, `DatatableTransformStep`, `BatchGenerate`,
  `BatchGenerateStep`, `BatchTransform`, `BatchTransformStep` now accepts
  keyworded arguments `**kwargs` to parametrize arguments of corresponding
  function `func` (https://github.com/epoch8/datapipe/pull/150)

# 0.11.1

* Column `filepath` is now written to meta-pseudo-df of `TableStoreFiledir` when
  `add_filepath_column` is enabled (https://github.com/epoch8/datapipe/pull/149)
* Fix `TableStoreFiledir` issues with regular expressions:
  https://github.com/epoch8/datapipe/issues/146 and
  https://github.com/epoch8/datapipe/issues/147
  (https://github.com/epoch8/datapipe/pull/149)
* Added new arguments `readonly` and `disable_rm` in `TableStoreFiledir`. By
  default `TableStoreFiledir` is running as reader and writer, but if
  `enable_rm=True` then it also removes files. When `readonly=None` (defualt),
  it checks for patterns `*` to disable/enable writing files, but if it needed
  to force enable or disable writing, `readonly` should be changed accordingly.
* Addeed OR patterns support in format `(aaa|bbb|ccc)` in `TableStoreFiledir`.
  For example: `/path/to/(folder1|folder2|folder3)/to/the/files.(jpg|png|jpeg)`.
* Fix: `read_rows()` should return `DataFrame` with primary key columns even if
  empty

# 0.11.0

## 0.11.0-beta.7 - UI and CLI

**Backwards incompatible changes**

* Move `datapipe.cli` and `datapipe.debug_ui` to `datapipe_app`
* Remove obsolete dependencies: `requests` and `toml`
* Changed default of `create_table` parameters to `False`; now by default no
  tables will be created. Needed for Alembic migrations autogeneration

## 0.11.0-beta.6

* Make `gcsfs` and `s3fs` dependencies optional
* Remove `poetry.lock` from project

## 0.11.0-beta.5

* Split `run_changelist` into `run_changelist` and `run_steps_changelist`

## 0.11.0-beta.4

* Add `create_meta_table` flag in `DataStore` - controls automatic creation of
  meta tables.
* Fix optionality for opentelemetry packages

## 0.11.0-beta.3

* Relax dependencies for `fsspec`

## 0.11.0-beta.2

* New table store: `MilvusStore`
* fix: Fixed pipeline run with changlist by chunk_size

## 0.11.0-beta.1 - Realtime

**Несовместимые изменения**

* Вернулся класс `ComputeStep` как основа вычислительного пайплайна
* Введено понятие `full` и `changelist` обработки
* `batch_transform_wrapper` переименован в `do_full_batch_transform`
* Появился метод `do_batch_transform` который принимает итератор индексов и
  делает итерационную обработку
* `batch_generate_wrapper` переименован в `do_batch_generate`
* Исключения в `do_batch_generate` больше не скрываются
* По всему коду параметр `batchsize` переименован в `batch_size` для
  консистентности с `input_dts`, `change_list` и тп
* `DataStore.get_process_ids` переименован в `get_full_process_ids`
* Добавлен метод `get_change_list_process_ids`

# 0.10.9

* Fix SettingWithCopyWarning in `MetaTable`
* Add `MetaKey()` support for `MetaTable` columns. Now it's possible to add
  non-primary keys into MetaTable for joins. See example:
  `examples/meta_key_pipeline.py`

# 0.10.8

* Add `read_data` parameter to `TableStoreFiledir`
  (https://github.com/epoch8/datapipe/pull/132)
* Fix fields order for compound indexes in `get_process_idx`
  (https://github.com/epoch8/datapipe/pull/136)
* Add `check_for_changes` parameter to `DatatableTransform` step
  (https://github.com/epoch8/datapipe/pull/131)
* Update `Pillow` to version `9.0.0`

# 0.10.7

* Move LabelStudio support to separate repo
* Move LevelDB TableStore to separate repo
* Remove `UPDATE FROM VALUES` support for SQLite
* Add methods `Catalog.add_datatable`, `Catalog.remove_datatable`,
  `DataStore.get_datatable`
* Add methods `index_intersection`, `index_to_data`

# 0.10.6

* Disable SQLAlchemy compiled cache for `UPDATE FROM VALUES` query
* Backport from 0.11.0-alpha.1: Фикс для join-а таблиц без пересекающихся
  индексов

# 0.10.5

* Fix `DBConn.supports_update_from` serialization

# 0.10.4

* Ускорение обновления метаданных через UPDATE FROM

# 0.10.3

* Добавлено инструментирование для трейсинга выполнения пайплайнов в Jaeger

# 0.10.2

* Исправлен баг с падением проверки типов в `DatatableTransformStep` при
  несколькох входных и выходных таблицах

# 0.10.1

* Поддержка явного задания `primary_schema` для `TableStoreFiledir`
* Первичная проверка корректности схем данных в `DatatableTransformStep`

# 0.10.0

* Не считать отсутствие строки в одной из входных таблиц необходимым условием
  для повторной обработки

## 0.10.0-alpha.2

* Добавлен `LevelDBStore`
* Трансформация может работать без пересекающихся ключей (любая строка слева
  сравнивается со строкой справа)
* `ComputeStep` объединен с `DatatableTransformStep`

## 0.10.0-alpha.1

**Несовместимые изменения**

* Удален класс `ExternalTable`, теперь это явный шаг пайплайна
  `UpdateExternalTable`
* Удален модуль `dsl`, классы переехали в `compute` и `core_steps`
* Удален модуль `step`, классы переехали в `run_config` и `compute`
* Исправлена проблема обновления `ExternalTable` с фильтрацией через `RunConfig`

# 0.9.20

* Фикс сохранения пустого DF

# 0.9.19

* Исправить удаление строк в `DataTable.store_chunk` при пустом входном DF

# 0.9.18

* Логгировать количество обработанных записей в `event_logger`

# 0.9.17

* Фикс получения данных из входной таблицы с фильтрацией

# 0.9.16

* Фикс для `read_rows` в `TableStore`, для случая когда передаеться пустой
  `IndexDF`

# 0.9.15

* Фикс для случая когда одна и та же таблица является и входом и выходом в
  трансформацию

# 0.9.14

* Ускорение работы `TableStoreDB`

# 0.9.13

* Фикс фильтрации `get_process_ids` из `RunConfig`

# 0.9.12

* Логгирование меты из `run_config` в `datapipe_events`.

# 0.9.11

* Метадата использует `JSONB` поле в случае Postgres соединения
* Багфикс: assert путей, содержащие протоколы, и `w{mode}+` -> `w{mode}`
* Фикс бага с добавлением и обновление пустых табличек
* Учет `RunConfig.filters` при обновлении `ExternalTable` с `TableStoreDB`

# 0.9.10

* Новый класс `RunConfig` который можно передать в `run_pipeline`. Предоставляет
  возможность фильтрации данных по конкретному значению одного или нескольких
  индексов.

# 0.9.9

* Фикс для кейса повторного добавления строк в `TableStoreDB`

# 0.9.8

* Фикс работы с SQLite для чанков больше 1000
  (https://github.com/epoch8/datapipe/issues/63)

# 0.9.7

* Новый CLI параметр `--debug-sql`
* Стриминг реализация чтения данных из БД в `ExternalTable` сценарии

## Minor breaking changes: code

* теперь нужно создавать отдельный `DBConn` для данных и метаданных из-за
  стримингового чтения

# 0.9.6

* Исправлена запись пустого `DataDF` в `DataTable`

# 0.9.5

* Исправлено несоответствие типов для поля `hash` в sqlite

## Breaking changes DB

* Поле `hash` метадаты теперь имеет тип `int32` и считается с помощью модуля
  `cityhash`

# 0.9.4

* update SQLAlchemy to 1.4 version

# 0.9.3

* FileDir DataStore поддерживает множественную идентификацию.

# 0.9.0

## Обратной совместимости нет

* **Индексация данных теперь множественная**
* Класса `MetaStore` больше нет, его роль выполняет `DataStore`
* `DataTable.store_chunk` теперь принимает `processed_idx`, отдельного метода
  `sync_meta_for_store_chunk` больше нет

# 0.8.2

* `inc_process_many` работает полностью инкрементально

# 0.8.1

* Функция `MetaStore.get_process_chunks` перестала быть методом `MetaStore` и
  переехала в модуль `datatable`

# 0.8.0

* Добавлена обработка ошибок в gen_process: исключение ловится, логгируется и
  выполнение переходит к следующим шагам
* Добавлена обработка ошибок в inc_process: исключение ловится, чанк с ошибкой
  игнорируется и выполнение продолжается

## Breaking changes DB

* Таблица datapipe_events изменила структуру (требует пересоздания)

## Breaking changes code

* агрумент ф-ии в gen_process всегда должен быть генератором

# 0.7.0

* Добавлен аттрибут `const_idx` в `TableStoreDB`, позволяет хранить данные
  разных шагов/пайплайнов в одной физической таблице с помощью
  доп.идентификаторов
* Добавлен класс `metastore.MetaTable`, который собирает в себе все задачи по
  работе с метаданными одной таблицы.

**Важно**: Поменялся интерфейс создания `DataTable`.

Было: `DataTable(meta_store, name, data_store)`

Стало: `DataTable(name, meta_store.create_meta_table(name), data_store)`

# 0.2.0 (2021-02-01)
- Add major code with Label Studio implementation
- Add Data cataloges and Nodes
