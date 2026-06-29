# План реализации: Исправление overshoot для max_records_per_run

## Проблема

**Текущее поведение:**
- `max_records_per_run=5` ограничивает changed_idx query → 5 событий
- Эти события группируются по `transform_keys` → N пар
- `get_batch_input_dfs` читает **ВСЕ события** этих пар из БД (может быть 100+)
- Offset вычисляется по всем прочитанным данным → **overshoot**

**Пример:**
```
БД содержит:
  (p1, post1): события id=1,2,3 (old_ts), id=6,7 (middle_ts), id=9,10,11 (new_ts)
  (p2, post2): id=4,5 (old_ts)
  (p3, post3): id=8 (middle_ts)

  id  | profile | post  | ts
  ----+---------+-------+----
  1   | p1      | post1 | old
  2   | p1      | post1 | old
  3   | p1      | post1 | old
  4   | p2      | post2 | old
  5   | p2      | post2 | old      <-- LIMIT 5
  6   | p1      | post1 | middle
  7   | p1      | post1 | middle
  8   | p3      | post3 | middle
  9   | p1      | post1 | new
  10  | p1      | post1 | new
  11  | p1      | post1 | new

max_records_per_run=5:
  changed_idx query LIMIT 5 → id=1-5
  Пары: (p1,post1), (p2,post2)
  get_batch_input_dfs читает: id=1,2,3,4,5,6,7,9,10,11 (10 событий вместо 5!)
  offset = new_ts → overshoot!

Следующий запрос:
  WHERE update_ts > new_ts → пропускает id=6,7,8 ❌
```

## Корневая причина

Для **ассоциативных** трансформаций (где `T(A) + T(B) = T(A+B)`) должно выполняться:
- Обработка id=1-5, затем id=6-11 = Обработка id=1-11 за раз

Но `get_batch_input_dfs` читает "лишние" данные (id=6,7,9,10,11), которые **ещё не должны быть обработаны**.

**Важно:** Мы предполагаем, что если разработчик использует `max_records_per_run`, то его трансформация ассоциативна. Проверку ассоциативности пока НЕ делаем.

## Решение

### Принцип: Имитация инкрементального поступления данных

Когда используется `max_records_per_run`, нужно вести себя так, как будто данных после LIMIT **физически не существует**.

---

## Часть 1: Strict режим для get_batch_input_dfs

### 1.1. Текущее поведение

```python
def get_batch_input_dfs(self, ds: DataStore, idx: IndexDF, ...) -> List[DataDF]:
    """
    idx содержит transform_keys: [(p1, post1), (p2, post2)]

    Читает ВСЕ события этих пар:
      SELECT * FROM events WHERE (profile_id, post_id) IN (...)

    Возвращает: все события пар (может быть намного больше чем в idx)
    """
```

### 1.2. Новое поведение (strict mode)

Нужно добавить режим, где `get_batch_input_dfs` читает **только** конкретные записи из changed_idx.

**Проблема:** `idx` после группировки по `transform_keys` теряет полный PK!

**Пример:**
```
Входная таблица PK: (id, profile_id, post_id)
transform_keys: (profile_id, post_id)

changed_idx query вернул:
  id=1, profile_id=p1, post_id=post1
  id=2, profile_id=p1, post_id=post1
  id=3, profile_id=p1, post_id=post1
  id=4, profile_id=p2, post_id=post2
  id=5, profile_id=p2, post_id=post2

idx передаваемый в process_batch (после группировки):
  profile_id=p1, post_id=post1  ← потеряли id=1,2,3!
  profile_id=p2, post_id=post2  ← потеряли id=4,5!
```

### 1.3. Решение: Сохранять исходный changed_idx

Нужно передавать **два** индекса:
1. **`idx`** - после группировки по transform_keys (для обратной совместимости)
2. **`changed_idx`** - исходный с полным PK (для strict режима)

**Способ передачи:** Через `RunConfig.labels` (минимальные breaking changes)

```python
# В run_full
for chunk_df in idx_gen:
    changed_idx = chunk_df  # Полный PK
    idx = data_to_index(chunk_df, self.transform_keys)  # Группировка

    # Сохраняем changed_idx в run_config
    run_config_with_changed = RunConfig.add_labels(
        run_config,
        {"_datapipe_changed_idx_full_pk": changed_idx}
    )

    changes = self.process_batch(ds, idx, run_config_with_changed)
```

---

## Часть 2: Изменения в get_batch_input_dfs

```python
def get_batch_input_dfs(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: Optional[RunConfig] = None,
) -> List[DataDF]:
    # Извлекаем changed_idx если есть
    changed_idx = None
    if run_config and "_datapipe_changed_idx_full_pk" in run_config.labels:
        changed_idx = run_config.labels["_datapipe_changed_idx_full_pk"]

    # Strict mode: если max_records_per_run задан и есть changed_idx
    use_strict = (
        self.max_records_per_run is not None and
        changed_idx is not None
    )

    result = []
    for inp in self.input_dts:
        if use_strict:
            # Читаем ТОЛЬКО записи из changed_idx (по полному PK)
            data = inp.dt.get_data(changed_idx)
            logger.info(
                f"[{self.get_name()}] Strict mode: reading {len(changed_idx)} records "
                f"(limited by max_records_per_run={self.max_records_per_run})"
            )
        else:
            # Legacy: читаем все данные для пар из idx
            if inp.join_keys:
                # Filtered join logic...
                data = inp.dt.get_data(filtered_idx)
            else:
                data = inp.dt.get_data(idx)

        result.append(data)

    return result
```

---

## Часть 3: Изменения в offset calculation

### 3.1. Текущая логика offset

```python
def store_batch_result(..., idx: IndexDF, ...):
    # idx = данные после get_batch_input_dfs (может содержать overshoot)
    processed_idx = data_to_index(first_output, self.transform_keys)
    max_update_ts = self._get_max_update_ts_for_batch(ds, inp, processed_idx)
    # Запрос: SELECT MAX(update_ts) WHERE (transform_keys) IN (processed_idx)
    # → overshoot!
```

### 3.2. Новая логика

```python
def store_batch_result(
    self,
    ds: DataStore,
    idx: IndexDF,
    output_dfs: Optional[TransformResult],
    process_ts: float,
    run_config: Optional[RunConfig] = None,
) -> ChangeList:
    # Извлекаем changed_idx
    changed_idx = None
    if run_config and "_datapipe_changed_idx_full_pk" in run_config.labels:
        changed_idx = run_config.labels["_datapipe_changed_idx_full_pk"]

    # ... existing code для формирования processed_idx ...

    for inp in self.input_dts:
        max_update_ts = self._get_max_update_ts_for_batch(
            ds, inp, processed_idx, changed_idx
        )
        ...
```

### 3.3. Обновленный _get_max_update_ts_for_batch

```python
def _get_max_update_ts_for_batch(
    self,
    ds: DataStore,
    compute_input: "ComputeInput",
    processed_idx: IndexDF,
    changed_idx: Optional[IndexDF] = None,
) -> Optional[float]:
    if len(processed_idx) == 0:
        return None

    # Если есть changed_idx - используем его для вычисления offset
    if changed_idx is not None and 'update_ts' in changed_idx.columns:
        # Фильтруем changed_idx: только успешно обработанные пары
        merged = changed_idx.merge(
            processed_idx,
            on=self.transform_keys,
            how='inner'
        )

        if len(merged) == 0:
            return None

        max_update = merged['update_ts'].max()

        logger.info(
            f"[{self.get_name()}] Offset from changed_idx: {max_update} "
            f"({len(merged)} records, max_records_per_run={self.max_records_per_run})"
        )

        return float(max_update)

    # Fallback: текущая логика (запрос к БД)
    input_dt = compute_input.dt
    tbl = input_dt.meta_table.sql_table
    # ... existing code ...
```

---

## Часть 4: Убрать limit_hit guard

Теперь можно безопасно обновлять offset даже при `limit_hit=True`:
- Offset вычисляется по changed_idx (не overshoot)
- Strict mode гарантирует, что обработали только данные из changed_idx

```python
# В run_full, УДАЛИТЬ:
# if limit_hit:
#     logger.warning("Skipping offset update: max_records_per_run limit hit")
# else:
#     ds.offset_table.update_offsets_bulk(changes.offsets)

# ЗАМЕНИТЬ на:
if changes.offsets:
    try:
        ds.offset_table.update_offsets_bulk(changes.offsets)
        logger.info(f"Updated offsets for {self.get_name()}: {changes.offsets}")
    except Exception as e:
        logger.warning(f"Failed to update offsets: {e}")
```

---

## Часть 5: Итоговый workflow

### 5.1. Без max_records_per_run (текущее поведение)

```
1. changed_idx query → все новые записи
2. Группировка по transform_keys → idx
3. get_batch_input_dfs(idx) → читает все данные пар
4. transform(данные)
5. offset = MAX(update_ts) из БД для обработанных пар
```

### 5.2. С max_records_per_run (новое поведение)

```
1. changed_idx query LIMIT N → первые N записей (с полным PK)
2. Сохраняем changed_idx в RunConfig
3. Группировка по transform_keys → idx
4. get_batch_input_dfs(idx, changed_idx) → читает ТОЛЬКО changed_idx (strict mode)
5. transform(данные из changed_idx)
6. offset = MAX(update_ts) из changed_idx для обработанных пар
```

---

## Часть 6: Детальный план реализации

### Этап 1: Сохранить changed_idx с полным PK

**Файл:** `datapipe/step/batch_transform.py` → `run_full()`

**Изменение:**
```python
# Текущий код в run_full:
for idx in idx_gen:
    changes = executor.run_process_batch(
        ...,
        idx_gen=iter([idx]),
        ...
    )

# Новый код:
for chunk_df in idx_gen:
    # НЕ группируем сразу! Сохраняем полный PK
    changed_idx_full_pk = chunk_df

    # Группируем для обратной совместимости
    idx = data_to_index(chunk_df, self.transform_keys)

    # Передаем через run_config
    if self.max_records_per_run is not None:
        run_config = RunConfig.add_labels(
            run_config,
            {"_datapipe_changed_idx_full_pk": changed_idx_full_pk}
        )

    changes = executor.run_process_batch(
        ...,
        idx_gen=iter([idx]),
        run_config=run_config,
        ...
    )
```

**Проблема:** Нужно проверить где происходит группировка в текущем коде.

---

### Этап 2: Strict mode в get_batch_input_dfs

**Файл:** `datapipe/step/batch_transform.py` → `get_batch_input_dfs()`

```python
def get_batch_input_dfs(
    self,
    ds: DataStore,
    idx: IndexDF,
    run_config: Optional[RunConfig] = None,
) -> List[DataDF]:
    # Извлекаем changed_idx если есть
    changed_idx = None
    if run_config and "_datapipe_changed_idx_full_pk" in run_config.labels:
        changed_idx = run_config.labels["_datapipe_changed_idx_full_pk"]

    # Strict mode: если max_records_per_run задан и есть changed_idx
    use_strict = (
        self.max_records_per_run is not None and
        changed_idx is not None
    )

    result = []
    for inp in self.input_dts:
        if use_strict:
            # Читаем ТОЛЬКО записи из changed_idx (по полному PK)
            data = inp.dt.get_data(changed_idx)
            logger.debug(
                f"[{self.get_name()}] Strict mode: reading {len(changed_idx)} records "
                f"for input {inp.dt.name} (max_records_per_run={self.max_records_per_run})"
            )
        else:
            # Legacy: читаем все данные для пар из idx
            if inp.join_keys:
                # Существующий код для filtered join
                filtered_idx = IndexDF(
                    idx[idx_cols]
                    .dropna()
                    .drop_duplicates()
                    .rename(columns=inp.join_keys)
                    .reset_index(drop=True)
                )
                data = inp.dt.get_data(filtered_idx)
            else:
                data = inp.dt.get_data(idx)

        result.append(data)

    return result
```

---

### Этап 3: Offset calculation с changed_idx

**Файл:** `datapipe/step/batch_transform.py` → `store_batch_result()`

```python
def store_batch_result(
    self,
    ds: DataStore,
    idx: IndexDF,
    output_dfs: Optional[TransformResult],
    process_ts: float,
    run_config: Optional[RunConfig] = None,
) -> ChangeList:
    # Извлекаем changed_idx
    changed_idx = None
    if run_config and "_datapipe_changed_idx_full_pk" in run_config.labels:
        changed_idx = run_config.labels["_datapipe_changed_idx_full_pk"]

    # ... existing code для формирования processed_idx ...

    for inp in self.input_dts:
        max_update_ts = self._get_max_update_ts_for_batch(
            ds, inp, processed_idx, changed_idx
        )

        if max_update_ts is not None:
            offset_key = (self.get_name(), inp.dt.name)
            changes.offsets[offset_key] = max_update_ts
```

**Файл:** `datapipe/step/batch_transform.py` → `_get_max_update_ts_for_batch()`

```python
def _get_max_update_ts_for_batch(
    self,
    ds: DataStore,
    compute_input: "ComputeInput",
    processed_idx: IndexDF,
    changed_idx: Optional[IndexDF] = None,
) -> Optional[float]:
    if len(processed_idx) == 0:
        return None

    # НОВАЯ ЛОГИКА: Если есть changed_idx - используем его
    if changed_idx is not None and 'update_ts' in changed_idx.columns:
        # Фильтруем changed_idx: только успешно обработанные пары
        merged = changed_idx.merge(
            processed_idx,
            on=self.transform_keys,
            how='inner'
        )

        if len(merged) == 0:
            return None

        max_update = merged['update_ts'].max()

        # Учитываем delete_ts если есть
        if 'delete_ts' in merged.columns:
            max_delete = merged['delete_ts'].max()
            if pd.notna(max_delete) and max_delete > max_update:
                max_update = max_delete

        logger.info(
            f"[{self.get_name()}] Offset from changed_idx (strict mode): {max_update}, "
            f"input: {compute_input.dt.name}, records: {len(merged)}"
        )

        return float(max_update)

    # СТАРАЯ ЛОГИКА (fallback): Запрос к БД
    input_dt = compute_input.dt
    tbl = input_dt.meta_table.sql_table

    # ... existing code с SELECT MAX(update_ts) ...
```

---

### Этап 4: Убрать limit_hit guard

**Файл:** `datapipe/step/batch_transform.py` → `run_full()`

```python
# УДАЛИТЬ эти строки (после 896):
# if limit_hit:
#     logger.warning(
#         f"[{self.get_name()}] Skipping offset update: max_records_per_run limit hit. "
#         ...
#     )
# else:
#     try:
#         ds.offset_table.update_offsets_bulk(changes.offsets)
#         ...

# ЗАМЕНИТЬ на:
if changes.offsets:
    try:
        ds.offset_table.update_offsets_bulk(changes.offsets)
        logger.info(f"Updated offsets for {self.get_name()}: {changes.offsets}")
    except Exception as e:
        logger.warning(
            f"Failed to update offsets for {self.get_name()}: {e}. "
            "Offset table may not exist (create_meta_table=False)"
        )

# Также можно убрать вычисление limit_hit или оставить для логирования
```

---

## Часть 7: Тестирование

### Тест 1: max_records_per_run с composite keys

```python
def test_max_records_per_run_no_overshoot(dbconn):
    """
    Проверяет, что max_records_per_run:
    1. Обрабатывает только changed_idx (strict mode)
    2. Offset не делает overshoot
    3. Все данные в итоге обрабатываются
    """
    ds = DataStore(dbconn, create_meta_table=True)

    # ... setup как в test_overshoot_composite_keys ...

    step = BatchTransformStep(
        ds=ds,
        name="parse_post_deepview",
        func=parse_post_deepview,
        input_dts=[ComputeInput(dt=input_dt)],  # БЕЗ join_type="full"
        output_dts=[output_dt],
        transform_keys=["profile_id", "post_id"],
        use_offset_optimization=True,
        max_records_per_run=5,
    )

    # Первый запуск
    step.run_full(ds)

    offset1 = ds.offset_table.get_offsets_for_transformation(step.get_name())["amplitude_events"]

    # Offset должен быть old_ts (не overshoot!)
    assert offset1 <= old_ts + 1, f"Offset {offset1} overshoot! Expected <= {old_ts}"

    # Второй запуск
    step.run_full(ds)

    offset2 = ds.offset_table.get_offsets_for_transformation(step.get_name())["amplitude_events"]
    assert offset2 <= middle_ts + 1

    # Третий запуск
    step.run_full(ds)

    offset3 = ds.offset_table.get_offsets_for_transformation(step.get_name())["amplitude_events"]
    assert offset3 <= new_ts + 1

    # Проверяем финальный результат
    output = output_dt.get_data()
    assert len(output) == 3  # Все 3 пары

    # Проверяем что все события обработались
    assert output[output['profile_id'] == 'p1']['deepview_count'].iloc[0] == 8
    assert output[output['profile_id'] == 'p2']['deepview_count'].iloc[0] == 2
    assert output[output['profile_id'] == 'p3']['deepview_count'].iloc[0] == 1
```

### Тест 2: Обратная совместимость

```python
def test_backward_compatibility_without_max_records(dbconn):
    """
    Проверяет, что без max_records_per_run всё работает как раньше
    """
    step = BatchTransformStep(
        ...,
        # max_records_per_run=None - не указан
    )

    step.run_full(ds)

    # Проверяем что всё обработалось за один запуск
    output = output_dt.get_data()
    assert len(output) == 3
```

### Тест 3: Strict mode действительно ограничивает данные

```python
def test_strict_mode_limits_data(dbconn):
    """
    Проверяет что в strict mode get_batch_input_dfs читает
    только changed_idx, а не все события пар
    """
    # ... setup ...

    # Мокаем get_data чтобы проверить что передаётся
    original_get_data = input_dt.get_data
    calls = []

    def mock_get_data(idx):
        calls.append(len(idx))
        return original_get_data(idx)

    input_dt.get_data = mock_get_data

    step = BatchTransformStep(
        ...,
        max_records_per_run=5,
    )

    step.run_full(ds)

    # Проверяем что get_data вызывался с 5 записями, а не с 11
    assert calls[0] == 5, f"Expected 5 records, got {calls[0]}"
```

---

## Часть 8: Edge cases

### 8.1. Что если transform функция отфильтровала часть данных?

```python
# changed_idx содержит 5 записей (3 пары)
# transform вернул только 2 пары (1 отфильтровалась)

# offset считается только по обработанным парам:
processed_idx = data_to_index(output, transform_keys)  # 2 пары
merged = changed_idx.merge(processed_idx, on=transform_keys)  # 2 пары из changed_idx
offset = max(merged['update_ts'])  # ✅ Правильно
```

### 8.2. Что если входная таблица НЕ имеет update_ts?

```python
if changed_idx is not None and 'update_ts' not in changed_idx.columns:
    logger.warning(
        f"[{self.get_name()}] changed_idx doesn't have update_ts, "
        "falling back to DB query for offset calculation"
    )
    changed_idx = None  # Fallback на старую логику
```

### 8.3. Что если несколько input_dts?

```python
# changed_idx относится к результату JOIN
# Если есть _datapipe_input_source - фильтруем по ней

if '_datapipe_input_source' in changed_idx.columns:
    filtered = changed_idx[
        changed_idx['_datapipe_input_source'] == compute_input.dt.name
    ]
else:
    filtered = changed_idx

# Затем вычисляем offset по filtered
```

### 8.4. Что если join_type="full"?

С `join_type="full"` параметр `max_records_per_run` не имеет смысла (читаем всю таблицу).

Можно добавить предупреждение:
```python
if self.max_records_per_run and any(inp.join_type == "full" for inp in self.input_dts):
    logger.warning(
        f"[{self.get_name()}] max_records_per_run has no effect with join_type='full'. "
        "Consider using filtered join or removing max_records_per_run."
    )
```

---

## Часть 9: Документация

### Обновить docstring BatchTransformStep

```python
"""
...

Parameters:
    max_records_per_run: Optional[int]
        Limit the number of records processed per run. This is useful for:
        - Rate limiting external API calls
        - Memory management for large datasets
        - Incremental processing in development

        IMPORTANT: This parameter assumes your transformation is associative,
        meaning T(A) + T(B) = T(A+B) for any disjoint data subsets A and B.

        When set, the step will:
        1. Use strict mode: read ONLY the limited records (not all events of pairs)
        2. Calculate offset based on processed records (prevents overshoot)

        Examples of associative transformations:
        - Filtering: df[df['value'] > 10]
        - Mapping: df['new'] = df['old'] * 2
        - Simple aggregations: groupby(['key']).agg({'col': 'sum'})

        Examples of NON-associative transformations (don't use max_records_per_run):
        - Sorting: df.sort_values()
        - Ranking: df['rank'] = df['value'].rank()
        - Top-N: df.head(10)
        - Window functions with full window

        If your transformation is non-associative, do not use this parameter.
"""
```

---

## Итоговый чеклист реализации

- [ ] **Этап 1:** Сохранить changed_idx с полным PK
  - [ ] Найти где происходит группировка по transform_keys
  - [ ] Сохранить полный PK до группировки
  - [ ] Передать через RunConfig.labels["_datapipe_changed_idx_full_pk"]

- [ ] **Этап 2:** Strict mode в `get_batch_input_dfs`
  - [ ] Извлекать changed_idx из RunConfig
  - [ ] Условие: `max_records_per_run is not None and changed_idx is not None`
  - [ ] Читать `inp.dt.get_data(changed_idx)` вместо `idx`
  - [ ] Добавить логирование

- [ ] **Этап 3:** Offset calculation с changed_idx
  - [ ] Передать changed_idx в `_get_max_update_ts_for_batch`
  - [ ] Merge changed_idx с processed_idx
  - [ ] MAX(update_ts) из merged
  - [ ] Fallback на старую логику если changed_idx=None

- [ ] **Этап 4:** Убрать limit_hit guard
  - [ ] Удалить проверку `if limit_hit` в `run_full`
  - [ ] Всегда обновлять offset

- [ ] **Этап 5:** Тестирование
  - [ ] Тест: max_records_per_run → offset правильный (не overshoot)
  - [ ] Тест: обратная совместимость (без max_records_per_run)
  - [ ] Тест: strict mode ограничивает данные
  - [ ] Тест: фильтрация в transform
  - [ ] Обновить существующий test_overshoot_composite_keys

- [ ] **Этап 6:** Документация
  - [ ] Обновить docstring BatchTransformStep
  - [ ] Добавить примеры ассоциативных/неассоциативных трансформаций
  - [ ] Предупреждение о требовании ассоциативности

- [ ] **Этап 7:** Очистка
  - [ ] Убрать отладочные print из batch_transform.py
  - [ ] Проверить что все тесты проходят
  - [ ] Убрать старые документы (solution4_implementation.py, offset_bug_solutions_v2.md)
