# Инициализация офсетов

**Расположение в коде:** `datapipe/meta/sql_meta.py:1398-1462`

[← Назад к обзору](./offset-optimization.md)

---

## Проблема

Как включить offset-оптимизацию на существующей трансформации без потери данных?

**Сценарий:**

```
Трансформация "process_posts" работает 6 месяцев:
  - Обработано: 1,000,000 записей
  - Метаданных: 1,000,000 записей в transform_meta
  - Входная таблица: 1,200,000 постов (включая новые)

Включаем offset-оптимизацию:
  - Офсет не установлен (None)
  - Без инициализации: обработает ВСЕ 1,200,000 постов повторно!
```

**Вопрос:** Как установить начальный офсет так, чтобы не переобработать уже обработанные 1,000,000 записей?

---

## Решение: initialize_offsets_from_transform_meta()

### Алгоритм

1. **Найти MIN(process_ts)** из успешно обработанных записей трансформации
   - Это самая ранняя временная метка обработки
   - Гарантирует, что все записи до этого момента были обработаны

2. **Для каждой входной таблицы найти MAX(update_ts)** где `update_ts <= min_process_ts`
   - Это максимальная временная метка, которая гарантированно была обработана

3. **Установить найденное значение как начальный офсет**

### SQL запросы

**Шаг 1: Найти минимальную process_ts**

```sql
SELECT MIN(process_ts) as min_process_ts
FROM transform_meta
WHERE transformation_id = :transformation_id
  AND is_success = True
  AND process_ts IS NOT NULL
```

**Шаг 2: Для каждой входной таблицы найти MAX(update_ts)**

```sql
SELECT MAX(update_ts) as initial_offset
FROM input_table
WHERE update_ts <= :min_process_ts
```

**Шаг 3: Записать офсеты**

```sql
INSERT INTO transform_input_offset (transformation_id, input_table_name, update_ts_offset)
VALUES (:transformation_id, :table_name, :initial_offset)
ON CONFLICT (transformation_id, input_table_name)
DO UPDATE SET update_ts_offset = GREATEST(
    transform_input_offset.update_ts_offset,
    EXCLUDED.update_ts_offset
)
```

---

## Использование

```python
# Инициализация офсетов для существующей трансформации
initial_offsets = ds.offset_table.initialize_offsets_from_transform_meta(
    transformation_id="process_posts",
    input_tables=[
        ("posts", posts_sa_table),
        ("profiles", profiles_sa_table),
    ]
)

# Результат
# {'posts': 1702345600.123, 'profiles': 1702345500.456}
```

**После инициализации:**

```python
# Теперь можно безопасно включить оптимизацию
step.use_offset_optimization = True
step.run_full(ds)

# Обработает только новые записи (200,000 вместо 1,200,000)
```

---

## Гарантии безопасности

### 1. Нет потери данных

```
min_process_ts = 1702345600

Логика:
  - Все записи с process_ts >= 1702345600 были обработаны
  - Следовательно, все записи с update_ts <= 1702345600 гарантированно обработаны
  - Офсет = MAX(update_ts WHERE update_ts <= 1702345600) безопасен
```

### 2. Нет дублирования

```
Начальный офсет = 1702345600

Следующий run_full():
  WHERE update_ts >= 1702345600  -- Офсет
  AND update_ts > process_ts      -- Дополнительная проверка

Результат:
  - Записи до 1702345600 пропускаются (уже обработаны)
  - Записи после 1702345600 обрабатываются
  - Записи с update_ts = 1702345600 проверяются по process_ts
```

### 3. Корректная обработка границ

**Случай: записи на границе офсета**

```python
# Офсет инициализирован: 1702345600

# Записи в входной таблице
records = [
    {"id": 1, "update_ts": 1702345599},  # До офсета
    {"id": 2, "update_ts": 1702345600},  # На границе
    {"id": 3, "update_ts": 1702345601},  # После офсета
]

# Метаданные
meta = [
    {"id": 1, "process_ts": 1702345650, "is_success": True},
    {"id": 2, "process_ts": 1702345650, "is_success": True},
]

# Следующий run_full найдет:
WHERE update_ts >= 1702345600  # id=2, id=3
  AND (process_ts IS NULL OR update_ts > process_ts)

# id=2: update_ts (1702345600) < process_ts (1702345650) → пропускается
# id=3: process_ts IS NULL → обрабатывается

# Корректно! Только новые записи обрабатываются
```

---

## Граничные случаи

### Случай 1: Нет обработанных записей

```python
min_process_ts = None  # Нет записей в transform_meta

# Результат: офсеты не устанавливаются
initial_offsets = {}

# Первый run_full обработает все записи (корректное поведение для новой трансформации)
```

### Случай 2: Входная таблица пустая

```python
max_update_ts = None  # Нет записей в input_table

# Результат: офсет не устанавливается для этой таблицы
initial_offsets = {}

# Фильтр не применяется (корректное поведение для пустой таблицы)
```

### Случай 3: Все записи уже обработаны

```python
min_process_ts = 1702345600
max_update_ts = 1702345000  # Все записи до min_process_ts

# Офсет = 1702345000

# Следующий run_full:
# WHERE update_ts >= 1702345000 AND update_ts > process_ts

# Результат: обработает только новые записи после последней обработки
```

---

## Пример полного сценария

### Исходная ситуация

```
Трансформация "process_posts" без offset-оптимизации:
  - transform_meta: 1,000,000 записей
  - MIN(process_ts) = 1702000000 (6 месяцев назад)
  - MAX(process_ts) = 1702345600 (вчера)

Входная таблица posts:
  - 1,200,000 записей
  - MIN(update_ts) = 1701900000
  - MAX(update_ts) = 1702345700 (сегодня)
```

### Шаг 1: Инициализация

```python
initial_offsets = ds.offset_table.initialize_offsets_from_transform_meta(
    transformation_id="process_posts",
    input_tables=[("posts", posts_sa_table)]
)
```

**SQL выполняется:**

```sql
-- Найти MIN(process_ts)
SELECT MIN(process_ts) FROM transform_meta
WHERE transformation_id = 'process_posts' AND is_success = True
-- Результат: 1702000000

-- Найти MAX(update_ts) для posts
SELECT MAX(update_ts) FROM posts
WHERE update_ts <= 1702000000
-- Результат: 1701999999

-- Записать офсет
INSERT INTO transform_input_offset VALUES ('process_posts', 'posts', 1701999999)
```

### Шаг 2: Первый run_full с оптимизацией

```python
step.use_offset_optimization = True
step.run_full(ds)
```

**SQL запрос на изменения:**

```sql
SELECT post_id, update_ts
FROM posts
WHERE update_ts >= 1701999999  -- Офсет
-- Найдено: все записи от 1701999999 до 1702345700

LEFT JOIN transform_meta meta ON post_id = meta.post_id
WHERE update_ts IS NULL
   OR meta.process_ts IS NULL
   OR (meta.is_success = True AND posts.update_ts > meta.process_ts)
-- Отфильтровано: только записи после 1702345600 (последняя обработка)

-- Результат: ~200,000 новых записей вместо 1,200,000
```

### Выгода

- **Без инициализации:** обработка 1,200,000 записей (~2 часа)
- **С инициализацией:** обработка 200,000 записей (~20 минут)
- **Экономия:** 83% времени

---

[← Назад к обзору](./offset-optimization.md)
