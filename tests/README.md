# Offset Optimization Tests

## 🎯 Главный тест

**Файл:** `test_offset_production_bug_main.py`

Воспроизводит production баг где **60% данных** (48,915 из 82,000 записей) были потеряны из-за строгого неравенства в SQL запросе и сортировки батчей по ключам трансформации (без сортировки по update_ts).

**Корневая причина:** `datapipe/meta/sql_meta.py:967`
```python
# ❌ БАГ:
tbl.c.update_ts > offset

# ✅ ДОЛЖНО БЫТЬ:
tbl.c.update_ts >= offset
```

**Механизм бага:**
1. Записи сортируются `ORDER BY id, hashtag` (не по update_ts!)
2. Батч содержит записи с разными update_ts
3. offset = MAX(update_ts) из батча
4. Следующий запуск: `WHERE update_ts > offset` пропускает записи с `update_ts == offset`

**Пример:**
```
Батч 1 (10 записей): rec_00..rec_09
  - rec_00..rec_07 имеют update_ts=T1
  - rec_08..rec_09 имеют update_ts=T2
  - offset = MAX(T1, T2) = T2

Батч 2: WHERE update_ts > T2
  - 🚨 rec_10, rec_11, rec_12 (update_ts=T2) ПОТЕРЯНЫ!
```

---

## 📁 Структура тестов

```
tests/
├── test_offset_production_bug_main.py          ← 🎯 ГЛАВНЫЙ production тест
├── test_offset_hypotheses.py                   ← 🔬 Тесты гипотез 1 и 2 + антирегрессия
├── test_offset_hypothesis_3_multi_step.py      ← 🔬 Тест гипотезы 3 (multi-step pipeline)
│
├── offset_edge_cases/                          ← Edge cases (9 тестов)
│   ├── README.md
│   ├── test_offset_production_bug.py           (4 теста)
│   ├── test_offset_first_run_bug.py            (2 теста)
│   └── test_offset_invariants.py               (3 теста)
│
└── test_offset_*.py                            ← Функциональные тесты (5 файлов)
    ├── test_offset_auto_update.py
    ├── test_offset_joinspec.py
    ├── test_offset_optimization_runtime_switch.py
    ├── test_offset_pipeline_integration.py
    └── test_offset_table.py
```

---

## 🚀 Запуск тестов

```bash
# Главный production тест
python -m pytest tests/test_offset_production_bug_main.py -xvs

# Тесты гипотез (1, 2 и антирегрессия)
python -m pytest tests/test_offset_hypotheses.py -xvs

# Тест гипотезы 3 (multi-step pipeline)
python -m pytest tests/test_offset_hypothesis_3_multi_step.py -xvs

# Все тесты гипотез вместе
python -m pytest tests/test_offset_hypotheses.py tests/test_offset_hypothesis_3_multi_step.py -v

# Все критичные тесты (production + гипотезы)
python -m pytest tests/test_offset_production_bug_main.py tests/test_offset_hypotheses.py -v

# С --runxfail (запустить тесты даже если помечены xfail)
python -m pytest tests/test_offset_production_bug_main.py tests/test_offset_hypotheses.py --runxfail -xvs

# Все offset тесты
python -m pytest tests/ -k offset -v

# Только edge cases
python -m pytest tests/offset_edge_cases/ -v
```

---

## ⚡ Оптимизация

Тесты оптимизированы по количеству данных и chunk_size:
- `test_offset_invariant_concurrent`: 2 threads × 6 iter = 12 records → 2 батча (chunk_size=10)
- `test_offset_invariant_synchronous`: 5 итераций × 3 records = 15 records → 3 батча (chunk_size=5)
- `test_first_run_with_mixed_update_ts`: 20 records → 2 батча (chunk_size=10)

**Результат:** Все offset тесты выполняются за ~15-30

---

## 🔧 Исправление бага

**Локации для изменения:**
- `datapipe/meta/sql_meta.py:967, 970, 989, 992, 1013, 1016`

**Изменение:**
```python
# Заменить все вхождения:
tbl.c.update_ts > offset  →  tbl.c.update_ts >= offset
tbl.c.delete_ts > offset  →  tbl.c.delete_ts >= offset
```

**Проверка:**
После исправления `test_offset_production_bug_main.py --runxfail` должен **ПРОЙТИ**.

---

## 🔍 Анализ причин бага в production

### Гипотезы и их статус

1. **Строгое неравенство `update_ts > offset`**
   - `WHERE update_ts > offset` пропускает записи с `update_ts == offset`
   - **Статус:** ✅ **ПОДТВЕРЖДЕНА** тестами
   - **Тест:** `test_offset_hypotheses.py::test_hypothesis_1_*`
   - **План:** [docs/offset_fix_plans/hypothesis_1_strict_inequality.md](../docs/offset_fix_plans/hypothesis_1_strict_inequality.md)

2. **ORDER BY по transform_keys, НЕ по update_ts**
   - Батчи сортируются по (id, hashtag), но offset = MAX(update_ts)
   - Записи с id ПОСЛЕ последней обработанной, но update_ts < offset теряются
   - **Статус:** ✅ **ПОДТВЕРЖДЕНА** тестами
   - **Тест:** `test_offset_hypotheses.py::test_hypothesis_2_*`
   - **План:** [docs/offset_fix_plans/hypothesis_2_order_by_keys.md](../docs/offset_fix_plans/hypothesis_2_order_by_keys.md)

3. **Рассинхронизация update_ts и process_ts в multi-step pipeline**
   - process_ts в Transform_B.meta ≠ update_ts в TableB (входная для Transform_C)
   - Создается временной разрыв (например, 4 часа)
   - **Статус:** ❌ **ОПРОВЕРГНУТА** тестом
   - **Тест:** `test_offset_hypothesis_3_multi_step.py::test_hypothesis_3_*`
   - **Результат:** Все записи обработаны (10/10), нет потерь
   - **Вывод:** У каждой трансформации своя meta table, рассинхронизация не влияет
   - **План:** [docs/offset_fix_plans/hypothesis_3_multistep_desync.md](../docs/offset_fix_plans/hypothesis_3_multistep_desync.md)

4. **"Запоздалая" запись с update_ts < current_offset**
   - Новая запись создается между запусками с устаревшим timestamp
   - **Статус:** ❌ **ОПРОВЕРГНУТА** анализом кода
   - **Причина:** `store_chunk()` ВСЕГДА использует `time.time()` для update_ts
   - **Код:** `datapipe/datatable.py:59`, `datapipe/meta/sql_meta.py:256-257`
   - **План:** [docs/offset_fix_plans/hypothesis_4_delayed_records.md](../docs/offset_fix_plans/hypothesis_4_delayed_records.md)

### Полная документация

📚 **Все планы исправлений:** [docs/offset_fix_plans/README.md](../docs/offset_fix_plans/README.md)

📊 **Сводка результатов:** [docs/offset_fix_plans/SUMMARY.md](../docs/offset_fix_plans/SUMMARY.md)

### Что показали тесты:

**Главный тест (`test_production_bug_main.py`)** - ПАДАЕТ ✅:
```
Подготовлено: 25 записей, 5 групп по update_ts
Обработка прервана после 1-го батча (10 записей)
offset = MAX(update_ts из 10 записей) = T2
Следующий запуск: WHERE update_ts > T2
Потеряно: 3 записи с update_ts == T2 (rec_10, rec_11, rec_12)
```

**Edge case тесты** - некоторые XPASS:
- Используют `step.run_full(ds)` → обрабатывают ВСЕ данные сразу
- БЕЗ прерывания обработки баг НЕ проявляется
- **Вывод:** Тесты не воспроизводят production сценарий

### Ключевой вывод:

**Баг проявляется ТОЛЬКО при КОМБИНАЦИИ факторов:**

1. Строгое неравенство `update_ts > offset` ← код
2. ORDER BY (id, hashtag), НЕ update_ts ← код
3. **ПРЕРЫВАНИЕ обработки** (джоба остановилась на середине) ← runtime

**Production сценарий (08.12.2025):**
- Накоплено: 82,000 записей
- Обработано: ~33,000 записей (40%)
- **Джоба ПРЕРВАЛАСЬ** после частичной обработки
- offset сохранился = MAX(update_ts) из последнего обработанного батча
- Следующий запуск: пропущено 48,915 записей (60%)

**Без прерывания обработки:**
- Если джоба обрабатывает ВСЕ данные за один запуск
- Баг НЕ проявляется (все записи обрабатываются)
- Именно поэтому edge case тесты XPASS

**Исправление (требуется 2 шага):**
```python
# Шаг 1: datapipe/meta/sql_meta.py:967, 989, 1013
tbl.c.update_ts >= offset  # Вместо >
tbl.c.delete_ts >= offset  # Вместо >

# Шаг 2: Добавить проверку process_ts (предотвращение зацикливания)
# И изменить ORDER BY на update_ts, transform_keys
```

См. подробные планы в [docs/offset_fix_plans/](../docs/offset_fix_plans/)

---

## 📊 Текущий статус тестов

После проверки всех гипотез (2025-12-11):

**Подтвержденные проблемы (требуют исправления):**
- ❌ `test_hypothesis_1_*` - XFAIL (ожидаемо)
- ❌ `test_hypothesis_2_*` - XFAIL (ожидаемо)
- ❌ `test_antiregression_*` - FAILED (баг подтвержден)
- ❌ `test_production_bug_main` - XFAIL (ожидаемо)

**Опровергнутые гипотезы (исправление не нужно):**
- ✅ `test_hypothesis_3_*` - PASSED (рассинхронизация не влияет)

После применения исправлений все тесты должны **ПРОЙТИ** (PASSED).

---

**Дата создания:** 2025-12-10
**Последнее обновление:** 2025-12-11
