# 0.7.0

Добавлен класс `metastore.MetaTable`, который собирает в себе все задачи по работе с метаданными одной таблицы.

**Важно**: Поменялся интерфейс создания `DataTable`.

Было: `DataTable(meta_store, name, data_store)`

Стало: `DataTable(name, meta_store.create_meta_table(name), data_store)`

# 0.2.0 (2021-02-01)
- Add major code with Label Studio implementation
- Add Data cataloges and Nodes
