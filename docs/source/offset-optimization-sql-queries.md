# Оптимизированные SQL-запросы (v1 vs v2)

**Расположение в коде:** `datapipe/meta/sql_meta.py:720-1215`

[← Назад к обзору](./offset-optimization.md)

---

## Алгоритм v1: FULL OUTER JOIN

**Концепция:** Объединить входные таблицы с метаданными через FULL OUTER JOIN.

```sql
SELECT transform_keys, input.update_ts
FROM input_table input
FULL OUTER JOIN transform_meta meta ON transform_keys
WHERE
    meta.process_ts IS NULL
    OR (meta.is_success = True AND input.update_ts > meta.process_ts)
    OR meta.is_success != True
```

**Производительность:** O(N) где N — размер transform_meta. **Деградирует** с ростом метаданных.

---

## Алгоритм v2: Offset-based

**Концепция:** Ранняя фильтрация по офсетам + UNION вместо JOIN.

```sql
WITH
-- Получить офсеты
offsets AS (
    SELECT input_table_name, update_ts_offset
    FROM transform_input_offset
    WHERE transformation_id = :transformation_id
),

-- Изменения в каждой входной таблице (ранняя фильтрация!)
input_changes AS (
    SELECT transform_keys, update_ts
    FROM input_table
    WHERE update_ts >= (SELECT update_ts_offset FROM offsets ...)
),

-- Записи с ошибками (всегда включены)
error_records AS (
    SELECT transform_keys, NULL as update_ts
    FROM transform_meta
    WHERE is_success != True
),

-- UNION всех источников
all_changes AS (
    SELECT * FROM input_changes
    UNION ALL
    SELECT * FROM error_records
)

-- Фильтр для исключения уже обработанных
SELECT DISTINCT all_changes.*
FROM all_changes
LEFT JOIN transform_meta meta ON transform_keys
WHERE
    all_changes.update_ts IS NULL  -- Ошибки
    OR meta.process_ts IS NULL     -- Новые
    OR (meta.is_success = True AND all_changes.update_ts > meta.process_ts)
ORDER BY update_ts, transform_keys
```

**Ключевые особенности:**
1. **Ранняя фильтрация:** `WHERE update_ts >= offset` применяется до JOIN с метаданными
2. **Использование индекса:** Фильтр по update_ts использует индекс
3. **UNION вместо JOIN:** Дешевле для больших данных
4. **Проверка process_ts:** Предотвращает зацикливание при использовании `>=`

**Производительность:** O(M) где M — записи с `update_ts >= offset`. **Константная** производительность.

---

## Критически важно: >= а не >

```python
# ✅ Правильно
WHERE update_ts >= offset

# ❌ Неправильно - потеря данных!
WHERE update_ts > offset  # Записи с update_ts == offset потеряны
```

**Почему >= ?**

При использовании `>` записи, у которых `update_ts` точно равен `offset`, будут пропущены. Это может произойти, когда:
- Несколько записей имеют одинаковую временную метку
- Офсет зафиксирован на границе батча

Использование `>=` гарантирует, что все записи будут обработаны, а дополнительная проверка `update_ts > process_ts` предотвращает дублирование.

---

[← Назад к обзору](./offset-optimization.md)
