# Хранение и управление офсетами

**Расположение в коде:** `datapipe/meta/sql_meta.py:1218-1396`

[← Назад к обзору](./offset-optimization.md)

---

## Класс TransformInputOffsetTable

Таблица для хранения максимальных обработанных временных меток (офсетов) для каждой комбинации трансформации и входной таблицы.

**Схема:**
```sql
Table: transform_input_offset
Primary Key: (transformation_id, input_table_name)
Columns:
  - transformation_id: VARCHAR
  - input_table_name: VARCHAR
  - update_ts_offset: FLOAT
```

---

## Основные методы API

### get_offsets_for_transformation()

Получить все офсеты для трансформации **одним запросом** (оптимизировано):

```python
offsets = ds.offset_table.get_offsets_for_transformation("process_posts")
# {'posts': 1702345678.123, 'profiles': 1702345600.456}
```

### update_offsets_bulk()

Атомарное обновление множества офсетов в одной транзакции:

```python
offsets = {
    ("process_posts", "posts"): 1702345678.123,
    ("process_posts", "profiles"): 1702345600.456,
}
ds.offset_table.update_offsets_bulk(offsets)
```

**Критическая деталь:** Используется `GREATEST(existing, new)` — офсет **никогда не уменьшается**, что предотвращает потерю данных при race conditions.

### reset_offset()

Сброс офсета для повторной обработки:

```python
# Сбросить офсет для одной таблицы
ds.offset_table.reset_offset("process_posts", "posts")

# Сбросить все офсеты трансформации
ds.offset_table.reset_offset("process_posts")
```

---

[← Назад к обзору](./offset-optimization.md)
