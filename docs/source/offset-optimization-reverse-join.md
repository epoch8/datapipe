# Reverse Join для референсных таблиц

**Расположение в коде:** `datapipe/meta/sql_meta.py:917-983`

[← Назад к обзору](./offset-optimization.md)

---

## Проблема

```python
# Основная таблица
posts = [{"post_id": 1, "user_id": 100, "content": "Hello"}]

# Референсная таблица
profiles = [{"id": 100, "name": "Alice"}]

# При изменении profiles.name для Alice
# Нужно переобработать все посты пользователя Alice
# Но таблица posts НЕ изменялась!
```

**Вопрос:** Как найти все записи в основной таблице, которые зависят от измененных записей в референсной таблице?

---

## Решение: Reverse Join

**Reverse join** = JOIN от **референсной** таблицы к **основной** таблице.

### SQL паттерн

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

**Обратите внимание:**
- JOIN идет **от** profiles_changes **к** posts (обратное направление)
- В SELECT берутся **transform_keys из основной таблицы** (post_id)
- update_ts используется **из референсной таблицы** (момент изменения профиля)

---

## Конфигурация

```python
ComputeInput(
    dt=profiles_table,
    join_type="inner",
    join_keys={"user_id": "id"}  # posts.user_id = profiles.id
)
```

**Формат join_keys:** `{колонка_в_основной_таблице: колонка_в_референсной_таблице}`

### Примеры

```python
# Простой случай - один ключ
join_keys={"user_id": "id"}
# posts.user_id = profiles.id

# Составной ключ
join_keys={"category_id": "id", "subcategory_id": "sub_id"}
# posts.category_id = categories.id AND posts.subcategory_id = categories.sub_id
```

---

## Как это работает

1. **Находим изменения в референсной таблице:**
   ```sql
   SELECT id FROM profiles WHERE update_ts >= :offset
   ```

2. **Обратный JOIN к основной таблице:**
   ```sql
   JOIN posts ON posts.user_id = profiles.id
   ```

3. **Извлекаем transform_keys:**
   ```sql
   SELECT posts.post_id
   ```

4. **Результат:** Все посты, которые нужно переобработать из-за изменений в профилях

---

## Полный пример

```python
# Конфигурация трансформации
step = BatchTransformStep(
    name="enrich_posts_with_profiles",
    input_dts=[
        # Основная таблица
        ComputeInput(
            dt=posts_table,
            join_type="full"
        ),
        # Референсная таблица с reverse join
        ComputeInput(
            dt=profiles_table,
            join_type="inner",
            join_keys={"user_id": "id"}
        ),
    ],
    transform_keys=["post_id"],
    use_offset_optimization=True
)
```

**Что произойдет при изменении профиля:**

1. Профиль Alice (id=100) обновлен → update_ts = 1702345700
2. Offset для profiles = 1702345600
3. Запрос находит profiles.id = 100 (update_ts >= offset)
4. Reverse join находит все posts где user_id = 100
5. Эти посты помечаются для переобработки
6. Трансформация обогащает посты новыми данными профиля

---

[← Назад к обзору](./offset-optimization.md)
