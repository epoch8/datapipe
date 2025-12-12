# Гипотеза 4: "Запоздалая" запись с update_ts < current_offset

## Статус: ❌ ОПРОВЕРГНУТА (анализ кода)

## Описание гипотезы

**Предположение:**
Новая запись с `update_ts < current_offset` может быть создана МЕЖДУ запусками трансформации, что приведет к её потере.

**Сценарий:**
```
T1: Первый запуск трансформации
    - Обрабатываем записи
    - offset = T1

T2: Создается новая запись с update_ts = T0 (T0 < T1)
    - Например, из внешней системы с отстающими часами
    - Или ручная вставка с устаревшим timestamp

T3: Второй запуск трансформации
    - WHERE update_ts > T1
    - ❌ Запись с update_ts=T0 будет пропущена
```

## Анализ кода

### DataTable.store_chunk()

```python
# datapipe/datatable.py:59-98
def store_chunk(
    self,
    data_df: DataDF,
    processed_idx: Optional[IndexDF] = None,
    now: Optional[float] = None,  # ← Параметр для timestamp
    run_config: Optional[RunConfig] = None,
) -> IndexDF:
    # ...
    (
        new_index_df,
        changed_index_df,
        new_meta_df,
        changed_meta_df,
    ) = self.meta_table.get_changes_for_store_chunk(hash_df, now)  # ← Передается now
```

### MetaTable.get_changes_for_store_chunk()

```python
# datapipe/meta/sql_meta.py:243-257
def get_changes_for_store_chunk(
    self, hash_df: HashDF, now: Optional[float] = None
) -> Tuple[IndexDF, IndexDF, MetadataDF, MetadataDF]:
    """..."""

    if now is None:
        now = time.time()  # ← ТЕКУЩЕЕ время, если не указано

    # ... дальше now используется как update_ts для новых/измененных записей
```

### Вывод из анализа кода

**`store_chunk()` ВСЕГДА использует:**
1. Либо `now=time.time()` (текущее время) - **по умолчанию**
2. Либо явно переданный `now` параметр - **для тестов**

**В нормальной работе системы:**
- Все вызовы `store_chunk()` из трансформаций используют `now=process_ts`
- `process_ts = time.time()` в момент обработки батча
- Значит, `update_ts` ВСЕГДА >= текущий offset

**Невозможно** создать "запоздалую" запись в нормальной работе!

## Когда гипотеза 4 может быть актуальна?

### 1. Ручная вставка данных с устаревшим timestamp

```python
# Если кто-то СПЕЦИАЛЬНО вставляет данные с прошлым timestamp:
dt.store_chunk(new_data, now=old_timestamp)
```

**Но:** Это НЕ нормальная работа системы, это ошибка пользователя.

### 2. Внешняя система напрямую пишет в таблицу

```sql
-- Обход datapipe API:
INSERT INTO table (id, value) VALUES (...);
```

**Но:**
- Это нарушает контракт datapipe
- update_ts не устанавливается через meta_table
- Такие записи НЕ попадут в мета-таблицу корректно

### 3. Синхронизация времени (NTP drift)

**Теоретически:** Если часы сервера "прыгнули назад" между запусками...

**Но:**
- Крайне маловероятно (NTP drift < секунды)
- Защита: проверка `process_ts` (из гипотезы 1) частично защищает

## Рекомендация

**Не требуется специального исправления.**

**Обоснование:**
1. В нормальной работе системы гипотеза НЕ реализуется
2. Edge cases (ручная вставка, NTP drift) - ответственность пользователя
3. Добавление защиты усложнит код без реальной пользы

**✅ Реализовано (2025-12-12):**
- Добавлен warning в `get_changes_for_store_chunk()` при вызове с `now < current_time - 1.0s`
- Предупреждает о потенциальной потере данных с offset optimization
- Локация: `datapipe/meta/sql_meta.py:259-265`

## Связь с другими гипотезами

Проверка `process_ts` из **гипотезы 1** частично защищает от этого сценария:

```python
WHERE (
    tr_tbl.c.process_ts IS NULL
    OR union_cte.c.update_ts > tr_tbl.c.process_ts
)
```

Если запись создана с `update_ts < offset`, но она НЕ была обработана (`process_ts IS NULL`), то она всё равно попадет в выборку через этот фильтр.

**НО:** Это работает только если запись попала в мета-таблицу трансформации. При обходе API это не гарантировано.

## Ссылки

- Код: `datapipe/datatable.py:59` (store_chunk)
- Код: `datapipe/meta/sql_meta.py:243` (get_changes_for_store_chunk)
- Использование в трансформациях: `datapipe/step/batch_transform.py:553` (now=process_ts)
