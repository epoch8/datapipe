# Гипотеза 3: Рассинхронизация update_ts и process_ts в multi-step pipeline

## Статус: ❌ ОПРОВЕРГНУТА

## Описание гипотезы

**Предположение:**
В multi-step pipeline рассинхронизация между `update_ts` (входной таблицы) и `process_ts` (мета-таблицы трансформации) может вызывать потерю данных.

**Сценарий:**
```
Pipeline: TableA → Transform_B → TableB → Transform_C → TableC

16:21 - Transform_B создает записи в TableB (update_ts=16:21)
20:04 - Transform_C обрабатывает TableB (4 часа спустя)
        - process_ts в Transform_C.meta = 20:04
        - update_ts в TableB остается = 16:21
        - Временной разрыв: 4 часа
```

**Вопрос:** Влияет ли эта рассинхронизация на offset optimization?

## Результаты тестирования

**Тест:** `test_hypothesis_3_multi_step_pipeline_update_ts_vs_process_ts_desync`

**Результаты:**
- ✅ ВСЕ записи обработаны (5/5 в фазе 2, 10/10 в фазе 4)
- ✅ Старые записи НЕ обработаны повторно
- ✅ Новые записи обработаны корректно
- ✅ Offset optimization работает корректно

**Вывод:** Рассинхронизация **НЕ** вызывает ни потери данных, ни повторной обработки.

## Почему гипотеза опровергнута

### Архитектура мета-таблиц

У каждой трансформации СВОЯ `TransformMetaTable` с СВОИМ `process_ts`:

```
TableA → Transform_B → TableB → Transform_C → TableC
         [Meta_B]                [Meta_C]
```

- `Meta_B.process_ts` = когда Transform_B обработал записи
- `TableB.update_ts` = когда Transform_B записал данные
- `Meta_C.process_ts` = когда Transform_C обработал записи

### Логика offset optimization

**Transform_C использует:**
- `offset(Transform_C, TableB) = MAX(TableB.update_ts)` ← update_ts **входной** таблицы
- Проверяет `Meta_C.process_ts` ← process_ts **своей** мета-таблицы

**Transform_C НЕ использует:**
- ❌ `Meta_B.process_ts` ← process_ts **другой** трансформации

### Вывод

Рассинхронизация между:
- `update_ts` входной таблицы (установлен Transform_B)
- `process_ts` мета-таблицы другой трансформации (Transform_B.meta)

**НЕ влияет** на корректность offset optimization Transform_C, так как:
1. Transform_C работает со СВОЕЙ мета-таблицей (`Meta_C`)
2. Offset основан на `update_ts` входной таблицы (`TableB`)
3. Эти две сущности не пересекаются

## Исправление

**Не требуется.** Рассинхронизация - это нормальное поведение системы в multi-step pipeline.

## Связь с другими гипотезами

**Важно:** Хотя гипотеза 3 опровергнута для multi-step pipeline, проверка `process_ts` **всё равно нужна** для исправления **гипотезы 1**.

Проверка `process_ts` нужна для **одной** трансформации, чтобы не обработать одни и те же данные дважды при изменении `>` на `>=`:

```python
# В v2 (sql_meta.py) после UNION:
WHERE (
    tr_tbl.c.process_ts IS NULL  # Не обработано
    OR union_cte.c.update_ts > tr_tbl.c.process_ts  # Изменилось после обработки
)
```

Но это проверка **своего** `process_ts` (Transform_C.meta.process_ts), а не process_ts других трансформаций!

## Ссылки

- Тест: `tests/test_offset_hypothesis_3_multi_step.py`
- Детали архитектуры: `datapipe/meta/sql_meta.py` (TransformMetaTable)
