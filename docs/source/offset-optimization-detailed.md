# Offset Optimization — Подробное описание

Этот документ содержит детальное описание компонентов и фич offset-оптимизации в Datapipe.

[← Назад к краткому обзору](./offset-optimization.md)

---

## Содержание

1. [Хранение и управление офсетами](#1-хранение-и-управление-офсетами)
2. [Оптимизированные SQL-запросы (v1 vs v2)](#2-оптимизированные-sql-запросы-v1-vs-v2)
3. [Reverse Join для референсных таблиц](#3-reverse-join-для-референсных-таблиц)
4. [Filtered Join](#4-filtered-join)
5. [Стратегия фиксации офсетов](#5-стратегия-фиксации-офсетов)
6. [Инициализация офсетов](#6-инициализация-офсетов)
7. [Метрики и мониторинг](#7-метрики-и-мониторинг)

---

## 1. Хранение и управление офсетами

**Расположение:** `datapipe/meta/sql_meta.py:1218-1396`

### Класс TransformInputOffsetTable

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

### Основные методы API

#### get_offsets_for_transformation()

Получить все офсеты для трансформации **одним запросом** (оптимизировано):

```python
offsets = ds.offset_table.get_offsets_for_transformation("process_posts")
# {'posts': 1702345678.123, 'profiles': 1702345600.456}
```

#### update_offsets_bulk()

Атомарное обновление множества офсетов в одной транзакции:

```python
offsets = {
    ("process_posts", "posts"): 1702345678.123,
    ("process_posts", "profiles"): 1702345600.456,
}
ds.offset_table.update_offsets_bulk(offsets)
```

**Критическая деталь:** Используется `GREATEST(existing, new)` — офсет **никогда не уменьшается**, что предотвращает потерю данных при race conditions.

#### reset_offset()

Сброс офсета для повторной обработки:

```python
# Сбросить офсет для одной таблицы
ds.offset_table.reset_offset("process_posts", "posts")

# Сбросить все офсеты трансформации
ds.offset_table.reset_offset("process_posts")
```

---

## 2. Оптимизированные SQL-запросы (v1 vs v2)

**Расположение:** `datapipe/meta/sql_meta.py:720-1215`

### Алгоритм v1: FULL OUTER JOIN

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

### Алгоритм v2: Offset-based

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

### Критически важно: >= а не >

```python
# ✅ Правильно
WHERE update_ts >= offset

# ❌ Неправильно - потеря данных!
WHERE update_ts > offset  # Записи с update_ts == offset потеряны
```

---

## 3. Reverse Join для референсных таблиц

**Расположение:** `datapipe/meta/sql_meta.py:917-983`

### Проблема

```python
# Основная таблица
posts = [{"post_id": 1, "user_id": 100, "content": "Hello"}]

# Референсная таблица
profiles = [{"id": 100, "name": "Alice"}]

# При изменении profiles.name для Alice
# Нужно переобработать все посты пользователя Alice
# Но таблица posts НЕ изменялась!
```

### Решение: Reverse Join

**Reverse join** = JOIN от **референсной** таблицы к **основной** таблице.

**SQL паттерн:**

```sql
-- Изменения в референсной таблице
profiles_changes AS (
    SELECT id, update_ts
    FROM profiles
    WHERE update_ts >= :profiles_offset
)

-- REVERSE JOIN к основной таблице
SELECT DISTINCT
    posts.post_id,              -- transform_keys основной таблицы
    profiles_changes.update_ts  -- update_ts из референса
FROM profiles_changes
JOIN posts ON posts.user_id = profiles_changes.id  -- ОБРАТНОЕ направление
```

**Конфигурация:**

```python
ComputeInput(
    dt=profiles_table,
    join_type="inner",
    join_keys={"user_id": "id"}  # posts.user_id = profiles.id
)
```

`join_keys` определяют связь: `{основная_таблица_колонка: референс_колонка}`.

---

## 4. Filtered Join

**Расположение:** `datapipe/step/batch_transform.py:632-686`

### Проблема

Чтение всей референсной таблицы неэффективно:

```
Обрабатываем 100 постов от 10 пользователей
Референсная таблица profiles: 10,000,000 записей
Нужны только 10 профилей, а читаем все 10 млн!
```

### Решение

**Filtered join** — читать из референса только нужные записи.

**Алгоритм:**

1. Извлечь уникальные значения внешних ключей из `idx`
2. Создать фильтрованный индекс
3. Прочитать `inp.dt.get_data(filtered_idx)`

**Код:**

```python
if inp.join_keys:
    # Извлечь уникальные user_id из idx
    filtered_idx_data = {}
    for idx_col, dt_col in inp.join_keys.items():
        if idx_col in idx.columns:
            filtered_idx_data[dt_col] = idx[idx_col].unique()

    # Создать фильтрованный индекс и прочитать
    filtered_idx = IndexDF(pd.DataFrame(filtered_idx_data))
    data = inp.dt.get_data(filtered_idx)  # Только 10 записей вместо 10 млн!
```

**Производительность:**
- Без filtered join: 10,000,000 записей, ~1 GB, ~10 сек
- С filtered join: 10 записей, ~1 KB, ~10 мс
- **Ускорение: 1000x**

---

## 5. Стратегия фиксации офсетов

**Расположение:** `datapipe/step/batch_transform.py:740-789`

### Текущая стратегия: Атомарная фиксация в конце run_full

**Принцип:** Офсеты фиксируются **только после успешной обработки всех батчей**.

```python
def run_full(self, ds, run_config=None, executor=None):
    idx = self.get_changed_idx(ds, run_config)
    changes = executor.run_process_batch(...)

    # Фиксация офсетов ТОЛЬКО в конце
    if changes.offsets:
        ds.offset_table.update_offsets_bulk(changes.offsets)

    return changes
```

**Гарантии:**
- ✅ Нет потери данных при сбое в середине обработки
- ✅ Изоляция от `run_changelist` (который не трогает офсеты)
- ✅ Корректное восстановление после перезапуска

**Проблема:** При сбое на батче 999 из 1000 — переобработка всех 1000 батчей.

### Планы развития: Побатчевый коммит

**Цель:** Фиксировать офсет после каждого успешного батча.

**Преимущества:**
- При сбое на батче N продолжение с батча N, а не с начала
- Минимизация потерь прогресса
- Обработка очень больших таблиц без риска полной переобработки

**Соображения безопасности:**
1. **Идемпотентность** — переобработка батча должна давать тот же результат
2. **Порядок обработки** — батчи должны идти в порядке возрастания update_ts
3. **Атомарность на уровне батча** — офсет только после успешного сохранения результатов

**Статус:** В планах на следующие версии.

### ChangeList как транспорт офсетов

```python
@dataclass
class ChangeList:
    changes: Dict[str, IndexDF]
    offsets: Dict[Tuple[str, str], float]  # (transformation_id, table_name) -> max_update_ts
```

Офсеты накапливаются в процессе обработки батчей и фиксируются в конце run_full.

---

## 6. Инициализация офсетов

**Расположение:** `datapipe/meta/sql_meta.py:1398-1462`

### Проблема

Как включить оптимизацию на существующей трансформации без потери данных?

```
Трансформация работает 6 месяцев, обработано 1,000,000 записей
Включаем offset-оптимизацию:
  - Офсет не установлен (None)
  - Нужно установить начальный офсет так, чтобы не обработать повторно 1 млн записей
```

### Решение: initialize_offsets_from_transform_meta()

**Алгоритм:**

1. Найти **MIN(process_ts)** из успешно обработанных записей трансформации
2. Для каждой входной таблицы найти **MAX(update_ts)** где `update_ts <= min_process_ts`
3. Установить как начальный офсет

**SQL:**

```sql
-- Шаг 1: MIN(process_ts)
SELECT MIN(process_ts) FROM transform_meta
WHERE transformation_id = :id AND is_success = True

-- Шаг 2: MAX(update_ts) для каждой входной таблицы
SELECT MAX(update_ts) FROM input_table
WHERE update_ts <= :min_process_ts
```

**Гарантии:**
- Нет потери данных (все записи до min_process_ts уже обработаны)
- Нет дублирования (новые записи после min_process_ts будут обработаны)

**Пример:**

```python
initial_offsets = ds.offset_table.initialize_offsets_from_transform_meta(
    transformation_id="process_posts",
    input_tables=[("posts", posts_sa_table), ("profiles", profiles_sa_table)]
)
# {'posts': 1702345600.123, 'profiles': 1702345500.456}
```

---

## 7. Метрики и мониторинг

### Метод get_statistics()

```python
stats = ds.offset_table.get_statistics()
# [
#   {
#     'transformation_id': 'process_posts',
#     'input_table_name': 'posts',
#     'update_ts_offset': 1702345600.0,
#     'offset_age_seconds': 120.5
#   },
#   ...
# ]
```

### Ключевая метрика: offset_age_seconds

**Назначение:** Показывает, как давно были обработаны последние данные.

**Интерпретация:**

| offset_age_seconds | Статус | Действие |
|-------------------|--------|----------|
| ~60 | ✅ Норма | Регулярные запуски каждую минуту |
| 3600 | ⚠️ Внимание | Обработка отстает на 1 час |
| 86400 | ❌ Критично | Трансформация не запускалась сутки |

**Типичные паттерны:**

1. **Стабильный** — offset_age ≈ const → нормальная работа
2. **Растущий линейно** — обработка отстает от поступления данных → увеличить частоту запусков
3. **Резкий рост** — трансформация давно не запускалась → проверить scheduler

### Prometheus алерт

```yaml
- alert: DatapipeOffsetAgeTooHigh
  expr: datapipe_offset_age_seconds > 3600
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Offset age too high for {{ $labels.transformation_id }}"
```

### Диагностика проблем

**offset_age растет + offset_value не меняется:**
- Трансформация не запускается (проблема scheduler)
- Входные данные не изменяются
- Офсет застрял из-за ошибки

**offset_age растет + offset_value растет медленно:**
- Частота запусков недостаточна
- Обработка слишком медленная
- Большой батч-размер

**run_full успешен + offset_value не меняется:**
- Ошибка фиксации офсета (проверить логи)
- run_full не находит изменений
- Используется run_changelist вместо run_full

---

## Типы данных

### JoinSpec

```python
@dataclass
class JoinSpec:
    table: TableOrName
    join_keys: Optional[Dict[str, str]] = None  # {primary_col: reference_col}
```

### ComputeInput

```python
@dataclass
class ComputeInput:
    dt: DataTable
    join_type: Literal["inner", "full"] = "full"
    join_keys: Optional[Dict[str, str]] = None  # Для reverse join и filtered join
```

### ChangeList

```python
@dataclass
class ChangeList:
    changes: Dict[str, IndexDF]                    # {step_name: idx}
    offsets: Dict[Tuple[str, str], float]          # {(transform_id, table_name): offset}

    def extend(self, other: 'ChangeList') -> 'ChangeList':
        # Объединение с MAX по офсетам
        ...
```

---

## Граничные случаи

### Записи с одинаковой update_ts

```python
posts = [
    {"post_id": 1, "update_ts": 1702345600.0},
    {"post_id": 2, "update_ts": 1702345600.0},
]
offset = 1702345600.0
```

✅ **Все записи найдены** благодаря `>=` (инклюзивное неравенство).

Дополнительная защита: проверка `update_ts > process_ts` предотвращает дублирование.

### Параллельные run_full

```
10:00 - run_full_1 начинает (offset = 100)
10:01 - run_full_2 начинает (offset = 100)
10:03 - run_full_2 завершен (фиксирует offset = 200)
10:05 - run_full_1 завершен (фиксирует offset = 150)
```

**Результат:** Офсет = 200 (благодаря `GREATEST(200, 150)` в update_offset).

**Рекомендация:** Избегать параллельных run_full. Использовать блокировки или очереди.

---

## Заключение

Offset-оптимизация — комплексная система для эффективной инкрементальной обработки данных. Ключевые принципы:

1. **Безопасность данных** — атомарная фиксация, инклюзивные неравенства, защита от race conditions
2. **Эффективность** — ранняя фильтрация, использование индексов
3. **Автоматизация** — reverse join для референсов, filtered join для оптимизации чтения
4. **Простота миграции** — инициализация офсетов из метаданных
5. **Мониторинг** — метрики для отслеживания здоровья системы

Система стабильна, протестирована и готова для production использования.

---

[← Назад к краткому обзору](./offset-optimization.md)
