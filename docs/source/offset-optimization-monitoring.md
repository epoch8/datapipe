# Метрики и мониторинг

[← Назад к обзору](./offset-optimization.md)

---

## Метод get_statistics()

Таблица офсетов предоставляет метод для получения статистики, которую можно использовать для мониторинга через Prometheus.

```python
stats = ds.offset_table.get_statistics()

# Результат:
# [
#   {
#     'transformation_id': 'process_posts',
#     'input_table_name': 'posts',
#     'update_ts_offset': 1702345600.0,
#     'offset_age_seconds': 120.5
#   },
#   {
#     'transformation_id': 'process_posts',
#     'input_table_name': 'profiles',
#     'update_ts_offset': 1702345580.0,
#     'offset_age_seconds': 140.3
#   },
# ]
```

**Для конкретной трансформации:**

```python
stats = ds.offset_table.get_statistics(transformation_id="process_posts")
```

---

## Ключевая метрика: offset_age_seconds

**Назначение:** Показывает, как давно были обработаны последние данные для входной таблицы.

**Формула:**
```
offset_age_seconds = текущее_время - update_ts_offset
```

### Интерпретация

| offset_age_seconds | Статус | Действие |
|-------------------|--------|----------|
| ~60 | ✅ Норма | Регулярные запуски каждую минуту |
| 3600 | ⚠️ Внимание | Обработка отстает на 1 час |
| 86400 | ❌ Критично | Трансформация не запускалась сутки |

### Типичные паттерны

**1. Стабильный offset_age — нормальная работа**
```
offset_age_seconds ≈ const (например, ~60 секунд при запуске каждую минуту)

График: горизонтальная линия
Интерпретация: трансформация работает стабильно
```

**2. Линейно растущий offset_age — обработка отстает**
```
offset_age_seconds растет со временем

График: восходящая линия
Причина: входные данные поступают быстрее, чем обрабатываются
Действие: увеличить частоту запусков или оптимизировать обработку
```

**3. Резкий рост offset_age — трансформация не запускалась**
```
offset_age_seconds скачок с 60 до 7200

График: вертикальный скачок
Причина: трансформация не запускалась последние 2 часа
Действие: проверить scheduler, логи, доступность системы
```

---

## Prometheus алерты

### Базовый алерт: высокий возраст офсета

```yaml
- alert: DatapipeOffsetAgeTooHigh
  expr: datapipe_offset_age_seconds > 3600
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Offset age too high for {{ $labels.transformation_id }}/{{ $labels.input_table_name }}"
    description: "Offset age is {{ $value }}s, indicating processing lag or stalled transformation."
```

### Алерт: офсет продолжает расти

```yaml
- alert: DatapipeOffsetAgeGrowing
  expr: rate(datapipe_offset_age_seconds[10m]) > 0
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "Offset age growing for {{ $labels.transformation_id }}/{{ $labels.input_table_name }}"
    description: "Processing is falling behind incoming data. Consider increasing execution frequency."
```

---

## Диагностика проблем

### Проблема 1: offset_age растет + offset_value не меняется

**Симптомы:**
- offset_age_seconds растет линейно
- offset_value (update_ts_offset) не изменяется

**Возможные причины:**
- Трансформация не запускается (проблема в scheduler)
- Входные данные не изменяются
- Офсет застрял из-за ошибки

**Диагностика:**
1. Проверить логи scheduler: есть ли запуски?
2. Проверить логи последнего run_full: успешно ли завершился?
3. Проверить входные таблицы: есть ли новые данные с `update_ts > offset`?

---

### Проблема 2: offset_age растет + offset_value растет медленно

**Симптомы:**
- offset_age_seconds медленно растет
- offset_value растет, но отстает от текущего времени

**Возможные причины:**
- Частота запусков недостаточна
- Обработка батчей слишком медленная
- Слишком большой размер батча

**Диагностика:**
1. Сравнить offset_value с MAX(update_ts) во входных таблицах
2. Измерить время выполнения run_full
3. Проверить размер батчей и количество обрабатываемых записей
4. Профилировать функцию трансформации

---

### Проблема 3: run_full успешен + offset_value не меняется

**Симптомы:**
- run_full выполняется успешно (логи OK)
- offset_value не изменяется
- offset_age_seconds продолжает расти

**Возможные причины:**
- Ошибка при фиксации офсета
- run_full не находит измененных записей
- Используется run_changelist вместо run_full

**Диагностика:**
1. Проверить логи: есть ли сообщение "Updated offsets for {name}: {offsets}"?
2. Проверить, вызывается ли update_offsets_bulk()
3. Проверить, есть ли записи с `update_ts > offset` во входных таблицах
4. Проверить индекс на update_ts: используется ли он?

---

## Экспорт в Prometheus

```python
from prometheus_client import Gauge

# Определение метрик
offset_age_gauge = Gauge(
    'datapipe_offset_age_seconds',
    'Age of offset in seconds (time since last processed update_ts)',
    ['transformation_id', 'input_table_name']
)

offset_value_gauge = Gauge(
    'datapipe_offset_value',
    'Current offset value (timestamp)',
    ['transformation_id', 'input_table_name']
)

def export_offset_metrics(ds):
    """Экспорт метрик офсетов в Prometheus."""
    stats = ds.offset_table.get_statistics()

    for stat in stats:
        labels = {
            'transformation_id': stat['transformation_id'],
            'input_table_name': stat['input_table_name']
        }

        # Экспорт возраста офсета
        offset_age_gauge.labels(**labels).set(stat['offset_age_seconds'])

        # Экспорт значения офсета
        offset_value_gauge.labels(**labels).set(stat['update_ts_offset'])
```

---

## Визуализация в Grafana

### Панель 1: Offset Age Timeline

Временной график offset_age_seconds для отслеживания трендов:

```json
{
  "title": "Offset Age by Transformation",
  "targets": [{
    "expr": "datapipe_offset_age_seconds",
    "legendFormat": "{{ transformation_id }}/{{ input_table_name }}"
  }],
  "yAxes": [{
    "label": "Age (seconds)"
  }]
}
```

### Панель 2: Processing Lag

Отставание обработки от поступления данных:

```promql
# Время отставания в часах
datapipe_offset_age_seconds / 3600

# Скорость роста отставания
rate(datapipe_offset_age_seconds[10m])
```

### Панель 3: Offset Progress

График прогресса обработки:

```promql
# Значение офсета
datapipe_offset_value

# Скорость изменения офсета (записей в секунду)
rate(datapipe_offset_value[5m])
```

---

## Рекомендации

### Настройка алертов

**Порог 1: Предупреждение**
```yaml
expr: datapipe_offset_age_seconds > 3600  # 1 час
severity: warning
```

**Порог 2: Критично**
```yaml
expr: datapipe_offset_age_seconds > 86400  # 1 день
severity: critical
```

### Регулярный мониторинг

**Ежедневно проверять:**
- Максимальный offset_age по всем трансформациям
- Трансформации с растущим offset_age
- Трансформации без обновлений офсетов

**Еженедельно анализировать:**
- Тренды offset_age за неделю
- Корреляция с нагрузкой системы
- Эффективность offset-оптимизации (сравнение времени выполнения)

---

[← Назад к обзору](./offset-optimization.md)
