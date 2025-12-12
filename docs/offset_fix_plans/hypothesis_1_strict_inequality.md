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

## Статус исправления

**✅ ИСПРАВЛЕНО** (2025-12-12)

**Изменения:**

1. **Изменено строгое неравенство `>` на `>=`** в 5 местах (строки 946, 972, 994, 1018, 1048):
   - `tbl.c.update_ts > offset` → `tbl.c.update_ts >= offset`
   - `tbl.c.delete_ts > offset` → `tbl.c.delete_ts >= offset`

2. **Добавлена фильтрация по `process_ts` для предотвращения зацикливания** (строки 1150-1177):
   ```python
   # Error records имеют update_ts = NULL, используем это для их идентификации
   is_error_record = union_cte.c.update_ts.is_(None)

   out = (
       sa.select(...)
       .where(
           sa.or_(
               # Error records (update_ts IS NULL) - всегда обрабатываем
               is_error_record,
               # Не обработано (первый раз)
               tr_tbl.c.process_ts.is_(None),
               # Успешно обработано, но данные обновились после обработки
               sa.and_(
                   tr_tbl.c.is_success == True,
                   union_cte.c.update_ts > tr_tbl.c.process_ts
               )
           )
       )
   )
   ```
   - Логика упрощена и аналогична v1
   - Error records уже включены в отдельный CTE, поэтому не проверяем `is_success != True`
   - Используем `update_ts IS NULL` для идентификации error_records (они всегда имеют NULL update_ts)

**Файлы:**
- `datapipe/meta/sql_meta.py` - функция `build_changed_idx_sql_v2()`
- `tests/test_offset_hypotheses.py` - убран `@pytest.mark.xfail`, исправлен тест (убран `now=` параметр)
- `tests/test_offset_production_bug_main.py` - убран `@pytest.mark.xfail`
- `tests/test_build_changed_idx_sql_v2.py` - обновлены ожидания теста (теперь возвращается `['id', 'update_ts']`)

**Ключевые решения при реализации:**

1. **Почему используем `update_ts IS NULL` для идентификации error_records?**
   - Error records берутся из transform_meta table, где нет колонки `update_ts`
   - При создании error_records CTE мы явно устанавливаем `update_ts = NULL`
   - Changed records всегда имеют реальное значение `update_ts` из data_meta table
   - Это простой и надёжный способ различить два типа записей без дополнительного literal column

2. **Почему не используем отдельный `_datapipe_offset` literal column?**
   - Изначально был добавлен `_datapipe_offset` для маркировки error_records
   - Рефакторинг показал, что можно использовать уже существующую колонку `update_ts`
   - Это упростило код на ~40 строк и сделало логику понятнее

3. **Почему не проверяем `is_success != True`?**
   - Записи с ошибками уже включены в `error_records` CTE
   - Повторная проверка приведёт к дублированию обработки

4. **Почему проверяем `is_success == True` в AND?**
   - Логика упрощена до 3 простых OR условий, как в v1:
     - `update_ts IS NULL` - error records (всегда обрабатывать)
     - `process_ts IS NULL` - запись не обработана (первый раз)
     - `is_success == True AND update_ts > process_ts` - обработана успешно, но данные обновились

## Результаты тестов после исправления

| Тест | До исправления | После исправления | Примечание |
|------|----------------|-------------------|------------|
| `test_hypothesis_1_*` | XFAIL | ✅ PASSED | Гипотеза 1 исправлена |
| `test_hypothesis_2_*` | ✅ PASSED | ✅ PASSED | Исправлено ранее |
| `test_antiregression_*` | FAILED | ✅ PASSED | Зависело от гипотезы 1 |
| `test_production_bug_*` | XFAIL | ✅ PASSED | Требовало обеих гипотез |
| `test_hypothesis_3_*` | ✅ PASSED | ✅ PASSED | Гипотеза опровергнута |
| **Все offset optimization тесты** | - | ✅ **15/15 PASSED** | Включая тесты retry ошибок |

**Вывод:** Обе гипотезы (строгое неравенство + ORDER BY) исправлены. Production баг с потерей 60% данных полностью устранён.
