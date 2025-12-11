# План исправления: ORDER BY transform_keys с mixed update_ts

## Проблема

Батчи сортируются `ORDER BY transform_keys`, но offset = `MAX(update_ts)` обработанного батча.

Это приводит к потере записей с `id` **после** последней обработанной, но с `update_ts` **меньше** offset.

## Сценарий потери данных

```
Данные (сортировка ORDER BY id):
  rec_00 → update_ts=T1
  rec_01 → update_ts=T1
  rec_02 → update_ts=T3  ← поздний timestamp
  rec_03 → update_ts=T3
  rec_04 → update_ts=T3
  rec_05 → update_ts=T2  ← средний timestamp, но id ПОСЛЕ rec_04!
  rec_06 → update_ts=T2
  rec_07 → update_ts=T2

Первый батч (chunk_size=5): rec_00..rec_04
  → offset = MAX(T1, T1, T3, T3, T3) = T3

Второй запуск: WHERE update_ts > T3
  → ❌ rec_05, rec_06, rec_07 ПОТЕРЯНЫ (update_ts=T2 < T3)
```

## Корневая причина

**Несоответствие между порядком обработки и логикой offset:**
- Обработка: `ORDER BY transform_keys` (детерминированный порядок для пользователя)
- Offset: `MAX(update_ts)` обработанных записей (временная логика)

**Когда возникает:**
- Записи создаются в порядке, НЕ соответствующем их `update_ts`
- Например: пакетная загрузка с разными timestamp'ами

## Варианты исправления

### Вариант 1: ORDER BY update_ts (рекомендуется)

**Изменить:** `datapipe/meta/sql_meta.py:1129-1142`

```python
# Было:
if order_by is None:
    out = out.order_by(
        tr_tbl.c.priority.desc().nullslast(),
        *[union_cte.c[k] for k in transform_keys],  # ← Сортировка по ключам
    )

# Должно быть:
if order_by is None:
    out = out.order_by(
        tr_tbl.c.priority.desc().nullslast(),
        union_cte.c.update_ts,  # ← Сортировка по времени (СНАЧАЛА)
        *[union_cte.c[k] for k in transform_keys],  # ← Затем по ключам (для детерминизма)
    )
```

**Требуется:**
- Добавить `update_ts` в `union_cte` (как описано в hypothesis_1)
- Изменить ORDER BY

**Плюсы:**
- ✅ Простое решение
- ✅ Гарантирует что `offset <= MIN(update_ts необработанных)`
- ✅ Сохраняет детерминизм (вторичная сортировка по transform_keys)

**Минусы:**
- ⚠️ Изменяет порядок обработки (может повлиять на поведение пользователя)

### Вариант 2: Отслеживать MIN(update_ts необработанных)

Вместо `offset = MAX(update_ts обработанных)` использовать `offset = MIN(update_ts необработанных) - ε`.

**Плюсы:**
- ✅ Сохраняет ORDER BY transform_keys

**Минусы:**
- ❌ Сложнее реализовать
- ❌ Требует дополнительный запрос для вычисления MIN
- ❌ Может замедлить работу

## Рекомендация

**Вариант 1** - ORDER BY update_ts, затем transform_keys.

**Обоснование:**
1. Простое изменение кода
2. Логично: обрабатываем данные в порядке их создания
3. Сохраняет детерминизм через вторичную сортировку

## Связь с другими гипотезами

- **Гипотеза 1** уже требует добавить `update_ts` в `union_cte`
- После исправления гипотезы 1, изменение ORDER BY - это **одна строка кода**

## Проверка

После исправления должен пройти:
- ✅ `test_hypothesis_2_order_by_transform_keys_with_mixed_update_ts`

## Статус исправления

**✅ ИСПРАВЛЕНО** (2025-12-11)

**Изменения:**
1. Добавлен `update_ts` в `all_select_keys` (строка 873-876)
2. Изменён ORDER BY для сортировки по `update_ts` перед `transform_keys` (строка 1150)
3. Добавлен `.nullslast()` к `update_ts` - error_records обрабатываются последними (строка 1150)

**Файлы:**
- `datapipe/meta/sql_meta.py` - функция `build_changed_idx_sql_v2()`
- `tests/test_offset_hypotheses.py` - убран `@pytest.mark.xfail` с теста гипотезы 2

## Результаты тестов после исправления

| Тест | До исправления | После исправления | Примечание |
|------|----------------|-------------------|------------|
| `test_hypothesis_2_*` | XFAIL | ✅ PASSED | Гипотеза 2 исправлена |
| `test_hypothesis_1_*` | XFAIL | XFAIL | Требует отдельного исправления |
| `test_antiregression_*` | FAILED | FAILED | Зависит от гипотезы 1 |
| `test_production_bug_*` | XFAIL | XFAIL | Требует исправления обеих гипотез |
| `test_hypothesis_3_*` | PASSED | ✅ PASSED | Гипотеза опровергнута |

**Вывод:** Гипотеза 2 полностью исправлена. Production баг требует дополнительного исправления гипотезы 1 (строгое неравенство).
