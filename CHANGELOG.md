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

* Add `read_data` parameter to `TableStoreFiledir` (https://github.com/epoch8/datapipe/pull/132)
* Fix fields order for compound indexes in `get_process_idx` (https://github.com/epoch8/datapipe/pull/136)
* Add `check_for_changes` parameter to `DatatableTransform` step (https://github.com/epoch8/datapipe/pull/131)
* Update `Pillow` to version `9.0.0`

# 0.10.7

* Move LabelStudio support to separate repo
* Move LevelDB TableStore to separate repo
* Remove `UPDATE FROM VALUES` support for SQLite
* Add methods `Catalog.add_datatable`, `Catalog.remove_datatable`, `DataStore.get_datatable`
* Add methods `index_intersection`, `index_to_data`

# 0.10.6

* Disable SQLAlchemy compiled cache for `UPDATE FROM VALUES` query
* Backport from 0.11.0-alpha.1: Фикс для join-а таблиц без пересекающихся индексов

# 0.10.5

* Fix `DBConn.supports_update_from` serialization

# 0.10.4

* Ускорение обновления метаданных через UPDATE FROM

# 0.10.3

* Добавлено инструментирование для трейсинга выполнения пайплайнов в Jaeger

# 0.10.2

* Исправлен баг с падением проверки типов в `DatatableTransformStep` при несколькох входных и выходных таблицах

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
