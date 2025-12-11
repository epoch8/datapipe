# Планы исправления Offset Optimization Bug

Этот каталог содержит подробные планы исправления проблем offset optimization, выявленных в production (08.12.2025).

## Production инцидент

- **Дата:** 08.12.2025
- **Потеряно:** 48,915 из 82,000 записей (60%)
- **Причина:** Комбинация нескольких проблем в offset optimization
- **Статус данных:** На 11.12.3025 все данные восстановлены с использованием v1

## Гипотезы и их статус

### ✅ Гипотеза 1: Строгое неравенство `update_ts > offset`
**Статус:** ПОДТВЕРЖДЕНА
**Файл:** [hypothesis_1_strict_inequality.md](hypothesis_1_strict_inequality.md)

**Проблема:** `WHERE update_ts > offset` теряет записи с `update_ts == offset`

**Тест:** `tests/test_offset_hypotheses.py::test_hypothesis_1_strict_inequality_loses_records_with_equal_update_ts`

**Исправление:**
1. Изменить `>` на `>=` в фильтрах offset
2. Добавить проверку `process_ts` для предотвращения зацикливания

---

### ✅ Гипотеза 2: ORDER BY transform_keys вместо update_ts
**Статус:** ПОДТВЕРЖДЕНА
**Файл:** [hypothesis_2_order_by_keys.md](hypothesis_2_order_by_keys.md)

**Проблема:** Батчи сортируются по `transform_keys`, но `offset = MAX(update_ts)`. Записи с `id` после последней обработанной, но с `update_ts < offset` теряются.

**Тест:** `tests/test_offset_hypotheses.py::test_hypothesis_2_order_by_transform_keys_with_mixed_update_ts`

**Исправление:**
- Сортировать батчи по `update_ts` (сначала), затем по `transform_keys` (для детерминизма)

---

### ❌ Гипотеза 3: Рассинхронизация update_ts и process_ts в multi-step pipeline
**Статус:** ОПРОВЕРГНУТА
**Файл:** [hypothesis_3_multistep_desync.md](hypothesis_3_multistep_desync.md)

**Проверка:** Рассинхронизация между `update_ts` (входной таблицы) и `process_ts` (мета-таблицы другой трансформации) НЕ влияет на корректность offset optimization.

**Тест:** `tests/test_offset_hypothesis_3_multi_step.py::test_hypothesis_3_multi_step_pipeline_update_ts_vs_process_ts_desync`

**Результат:** ✅ Все записи обработаны, ничего не потеряно

**Примечание:** Проверка `process_ts` всё равно нужна для гипотезы 1, но это проверка СВОЕГО process_ts, а не других трансформаций.

---

### ❌ Гипотеза 4: "Запоздалая" запись с update_ts < offset
**Статус:** ОПРОВЕРГНУТА (анализ кода)
**Файл:** [hypothesis_4_delayed_records.md](hypothesis_4_delayed_records.md)

**Проверка:** `store_chunk()` ВСЕГДА использует текущее время (`time.time()`) для `update_ts`. "Запоздалые" записи невозможны в нормальной работе.

**Анализ кода:**
- `datapipe/datatable.py:59` - store_chunk
- `datapipe/meta/sql_meta.py:256-257` - if now is None: now = time.time()

**Результат:** В нормальной работе системы невозможно

---

## Приоритет исправлений

### Критично (блокирует production)

1. **Гипотеза 1** - Строгое неравенство
   - Исправление: ~50 строк кода
   - Риск: средний (требует проверка process_ts)
   - Тесты: test_hypothesis_1, test_antiregression

### Критично (блокирует production)

2. **Гипотеза 2** - ORDER BY
   - Исправление: 1 строка кода (при условии что гипотеза 1 уже исправлена)
   - Риск: низкий (изменяет только порядок обработки)
   - Тесты: test_hypothesis_2

### Не требуется

3. **Гипотеза 3** - Опровергнута
4. **Гипотеза 4** - Опровергнута

## Порядок применения исправлений

```
1. Гипотеза 1 (строгое неравенство + process_ts)
   ↓
2. Гипотеза 2 (ORDER BY update_ts)
   ↓
3. Запуск всех тестов
   ↓
4. Production deployment
```

## Проверка исправлений

### Тесты должны пройти:
- ✅ `test_hypothesis_1_strict_inequality_loses_records_with_equal_update_ts`
- ✅ `test_hypothesis_2_order_by_transform_keys_with_mixed_update_ts`
- ✅ `test_antiregression_no_infinite_loop_with_equal_update_ts`
- ✅ `test_production_bug_offset_loses_records_with_equal_update_ts`
- ✅ `test_hypothesis_3_multi_step_pipeline_update_ts_vs_process_ts_desync`

### Команда для запуска:
```bash
pytest tests/test_offset_hypotheses.py tests/test_offset_production_bug_main.py tests/test_offset_hypothesis_3_multi_step.py -v
```

## Дополнительные материалы

- **Основной баг репорт:** `tests/README.md`
- **Тесты:** `tests/test_offset_*.py`
- **Код offset optimization:** `datapipe/meta/sql_meta.py` (build_changed_idx_sql_v2)

---

**Дата создания документации:** 2025-12-11
