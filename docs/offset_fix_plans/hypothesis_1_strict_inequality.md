# План исправления offset optimization bug

## Проблема

`datapipe/meta/sql_meta.py` - строгое неравенство `update_ts > offset` теряет записи с `update_ts == offset`.

**НО**: Простое изменение `>` на `>=` вызовет зацикливание!

## Корневая причина

**v2** (offset optimization) не проверяет `process_ts`, в отличие от **v1**:

```python
# v1 (sql_meta.py:793) - есть проверка
agg_of_aggs.c.update_ts > out.c.process_ts

# v2 (sql_meta.py:967,989,1013) - НЕТ проверки process_ts
tbl.c.update_ts > offset  # Только offset!
```

## Сценарий зацикливания

**При изменении ТОЛЬКО `>` на `>=`:**

1. Первый батч: rec_00...rec_04 (update_ts=T1) → offset=T1, process_ts=T1
2. Второй запуск: `WHERE update_ts >= T1` → вернет rec_00...rec_11 (все с T1!)
3. v2 НЕ проверяет `process_ts` → rec_00...rec_04 обработаются повторно
4. Зацикливание ❌

## Исправление (2 шага)

### 1. Изменить строгое неравенство

**Файл:** `datapipe/meta/sql_meta.py`

**Строки:** 967, 970, 989, 992, 1013, 1016

```python
# Было:
tbl.c.update_ts > offset
tbl.c.delete_ts > offset

# Должно быть:
tbl.c.update_ts >= offset
tbl.c.delete_ts >= offset
```

### 2. Добавить фильтрацию по process_ts в v2

**Проблема:** В union_cte нет `update_ts`, есть только transform_keys.

**Решение:** Включить `MAX(update_ts)` в changed_ctes, затем фильтровать.

**Локация:** `datapipe/meta/sql_meta.py:1060-1127` (после UNION, перед ORDER BY)

**Логика фильтра:**
```python
# Псевдокод
WHERE (
    tr_tbl.c.process_ts IS NULL  # Не обработано
    OR union_cte.c.update_ts > tr_tbl.c.process_ts  # Изменилось после обработки
)
```

**Детали реализации:**
- В каждый changed_cte добавить `sa.func.max(tbl.c.update_ts).label("update_ts")`
- В union_parts включить `update_ts`
- После OUTERJOIN (строка 1126) добавить `.where(...)` с проверкой

## Проверка

После исправления должны пройти:
- ✅ `test_hypothesis_1` - записи с update_ts == offset обрабатываются
- ✅ `test_antiregression` - нет зацикливания, каждый батч обрабатывает новые записи
- ❌ `test_hypothesis_2` - продолжает падать (проблема ORDER BY остается)

## Альтернатива (не рекомендуется)

Использовать `process_ts` вместо `update_ts` для offset - сложнее, требует больше изменений.
