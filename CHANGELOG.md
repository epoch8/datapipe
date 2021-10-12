# 0.9.6

* Исправлена запись пустого `DataDF` в `DataTable`

# 0.9.5

* Исправлено несоответствие типов для поля `hash` в sqlite

## Breaking changes DB

* Поле `hash` метадаты теперь имеет тип `int32` и считается с помощью модуля `cityhash`

# 0.9.4

* update SQLAlchemy to 1.4 version

# 0.9.3

* FileDir DataStore поддерживает множественную идентификацию. 

# 0.9.0

## Обратной совместимости нет

* **Индексация данных теперь множественная**
* Класса `MetaStore` больше нет, его роль выполняет `DataStore`
* `DataTable.store_chunk` теперь принимает `processed_idx`, отдельного метода `sync_meta_for_store_chunk` больше нет

# 0.8.2

* `inc_process_many` работает полностью инкрементально

# 0.8.1

* Функция `MetaStore.get_process_chunks` перестала быть методом `MetaStore` и переехала в модуль `datatable`

# 0.8.0

* Добавлена обработка ошибок в gen_process: исключение ловится, логгируется и выполнение переходит к следующим шагам
* Добавлена обработка ошибок в inc_process: исключение ловится, чанк с ошибкой игнорируется и выполнение продолжается

## Breaking changes DB

* Таблица datapipe_events изменила структуру (требует пересоздания)

## Breaking changes code

* агрумент ф-ии в gen_process всегда должен быть генератором

# 0.7.0

* Добавлен аттрибут `const_idx` в `TableStoreDB`, позволяет хранить данные разных шагов/пайплайнов в одной физической таблице с помощью доп.идентификаторов
* Добавлен класс `metastore.MetaTable`, который собирает в себе все задачи по работе с метаданными одной таблицы.

**Важно**: Поменялся интерфейс создания `DataTable`.

Было: `DataTable(meta_store, name, data_store)`

Стало: `DataTable(name, meta_store.create_meta_table(name), data_store)`

# 0.2.0 (2021-02-01)
- Add major code with Label Studio implementation
- Add Data cataloges and Nodes
