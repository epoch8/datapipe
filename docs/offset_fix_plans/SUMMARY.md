# Сводка по проверке гипотез offset optimization bug

**Дата:** 2025-12-11  
**Проверено:** 4 гипотезы

## Результаты

| # | Гипотеза | Статус | Метод проверки | План исправления |
|---|----------|--------|----------------|------------------|
| 1 | Строгое неравенство `update_ts > offset` | ✅ **ПОДТВЕРЖДЕНА** | Тест | [hypothesis_1_strict_inequality.md](hypothesis_1_strict_inequality.md) |
| 2 | ORDER BY transform_keys с mixed update_ts | ✅ **ПОДТВЕРЖДЕНА** | Тест | [hypothesis_2_order_by_keys.md](hypothesis_2_order_by_keys.md) |
| 3 | Рассинхронизация в multi-step pipeline | ❌ **ОПРОВЕРГНУТА** | Тест | [hypothesis_3_multistep_desync.md](hypothesis_3_multistep_desync.md) |
| 4 | "Запоздалая" запись с update_ts < offset | ❌ **ОПРОВЕРГНУТА** | Анализ кода | [hypothesis_4_delayed_records.md](hypothesis_4_delayed_records.md) |

## Тесты

### ✅ Подтвержденные проблемы (XFAIL - expected to fail)
```
tests/test_offset_hypotheses.py::test_hypothesis_1_strict_inequality_loses_records_with_equal_update_ts XFAIL
tests/test_offset_hypotheses.py::test_hypothesis_2_order_by_transform_keys_with_mixed_update_ts XFAIL
tests/test_offset_production_bug_main.py::test_production_bug_offset_loses_records_with_equal_update_ts XFAIL
```

### ❌ Регрессия (FAILED - баг в production коде)
```
tests/test_offset_hypotheses.py::test_antiregression_no_infinite_loop_with_equal_update_ts FAILED
```
*Этот тест подтверждает баг гипотезы 1 - записи с update_ts == offset не обрабатываются*

### ✅ Опровергнутые гипотезы (PASSED)
```
tests/test_offset_hypothesis_3_multi_step.py::test_hypothesis_3_multi_step_pipeline_update_ts_vs_process_ts_desync PASSED
```
*Тест показывает что рассинхронизация НЕ влияет на корректность*

## Требуется исправление

### Критично
- **Гипотеза 1**: Изменить `>` на `>=` + добавить проверку `process_ts`
- **Гипотеза 2**: Изменить ORDER BY на `update_ts, transform_keys`

### Не требуется
- **Гипотеза 3**: Опровергнута, исправление не нужно
- **Гипотеза 4**: Опровергнута, исправление не нужно

## Команда для проверки после исправления

```bash
# Все offset тесты
pytest tests/test_offset_*.py -v

# Только критичные
pytest tests/test_offset_hypotheses.py tests/test_offset_production_bug_main.py --runxfail -v
```

После исправления все тесты должны **ПРОЙТИ** (PASSED), а не XFAIL.
