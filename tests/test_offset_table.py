import time

from datapipe.meta.sql_meta import TransformInputOffsetTable
from datapipe.store.database import DBConn


def test_offset_table_create(dbconn: DBConn):
    """Тест создания таблицы и индексов"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Проверяем, что таблица создана
    assert offset_table.sql_table is not None

    # Проверяем, что таблица существует в БД
    assert offset_table.sql_table.exists(dbconn.con)


def test_offset_table_get_offset_empty(dbconn: DBConn):
    """Тест получения offset'а для несуществующей трансформации"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    offset = offset_table.get_offset("test_transform", "test_table")
    assert offset is None


def test_offset_table_update_and_get(dbconn: DBConn):
    """Тест обновления и получения offset'а"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Обновляем offset
    offset_table.update_offset("test_transform", "test_table", 123.45)

    # Получаем offset
    offset = offset_table.get_offset("test_transform", "test_table")
    assert offset == 123.45


def test_offset_table_update_existing(dbconn: DBConn):
    """Тест обновления существующего offset'а"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем первый offset
    offset_table.update_offset("test_transform", "test_table", 100.0)

    # Обновляем его
    offset_table.update_offset("test_transform", "test_table", 200.0)

    # Проверяем, что offset обновился
    offset = offset_table.get_offset("test_transform", "test_table")
    assert offset == 200.0


def test_offset_table_multiple_inputs(dbconn: DBConn):
    """Тест работы с несколькими входными таблицами"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы для разных входных таблиц одной трансформации
    offset_table.update_offset("test_transform", "table1", 100.0)
    offset_table.update_offset("test_transform", "table2", 200.0)

    # Проверяем, что каждая таблица имеет свой offset
    assert offset_table.get_offset("test_transform", "table1") == 100.0
    assert offset_table.get_offset("test_transform", "table2") == 200.0


def test_offset_table_bulk_update(dbconn: DBConn):
    """Тест bulk обновления offset'ов"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Обновляем несколько offset'ов за раз
    offsets = {
        ("transform1", "table1"): 100.0,
        ("transform1", "table2"): 200.0,
        ("transform2", "table1"): 300.0,
    }
    offset_table.update_offsets_bulk(offsets)

    # Проверяем, что все offset'ы установлены
    assert offset_table.get_offset("transform1", "table1") == 100.0
    assert offset_table.get_offset("transform1", "table2") == 200.0
    assert offset_table.get_offset("transform2", "table1") == 300.0


def test_offset_table_bulk_update_existing(dbconn: DBConn):
    """Тест bulk обновления существующих offset'ов"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем начальные offset'ы
    offset_table.update_offset("transform1", "table1", 100.0)
    offset_table.update_offset("transform1", "table2", 200.0)

    # Обновляем их через bulk
    offsets = {
        ("transform1", "table1"): 150.0,
        ("transform1", "table2"): 250.0,
    }
    offset_table.update_offsets_bulk(offsets)

    # Проверяем, что offset'ы обновились
    assert offset_table.get_offset("transform1", "table1") == 150.0
    assert offset_table.get_offset("transform1", "table2") == 250.0


def test_offset_table_reset_single_table(dbconn: DBConn):
    """Тест сброса offset'а для одной входной таблицы"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы
    offset_table.update_offset("test_transform", "table1", 100.0)
    offset_table.update_offset("test_transform", "table2", 200.0)

    # Сбрасываем offset для одной таблицы
    offset_table.reset_offset("test_transform", "table1")

    # Проверяем
    assert offset_table.get_offset("test_transform", "table1") is None
    assert offset_table.get_offset("test_transform", "table2") == 200.0


def test_offset_table_reset_all_tables(dbconn: DBConn):
    """Тест сброса всех offset'ов для трансформации"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы
    offset_table.update_offset("test_transform", "table1", 100.0)
    offset_table.update_offset("test_transform", "table2", 200.0)

    # Сбрасываем все offset'ы для трансформации
    offset_table.reset_offset("test_transform")

    # Проверяем
    assert offset_table.get_offset("test_transform", "table1") is None
    assert offset_table.get_offset("test_transform", "table2") is None


def test_offset_table_get_statistics(dbconn: DBConn):
    """Тест получения статистики по offset'ам"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы
    now = time.time()
    offset_table.update_offset("transform1", "table1", now - 100)
    offset_table.update_offset("transform1", "table2", now - 200)
    offset_table.update_offset("transform2", "table1", now - 300)

    # Получаем статистику для всех трансформаций
    stats = offset_table.get_statistics()
    assert len(stats) == 3

    # Проверяем структуру
    for stat in stats:
        assert "transformation_id" in stat
        assert "input_table_name" in stat
        assert "update_ts_offset" in stat
        assert "offset_age_seconds" in stat
        assert isinstance(stat["offset_age_seconds"], float)
        assert stat["offset_age_seconds"] >= 0

    # Получаем статистику для конкретной трансформации
    stats_t1 = offset_table.get_statistics("transform1")
    assert len(stats_t1) == 2
    assert all(s["transformation_id"] == "transform1" for s in stats_t1)


def test_offset_table_get_offset_count(dbconn: DBConn):
    """Тест подсчета количества offset'ов"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Изначально offset'ов нет
    assert offset_table.get_offset_count() == 0

    # Добавляем offset'ы
    offset_table.update_offset("transform1", "table1", 100.0)
    assert offset_table.get_offset_count() == 1

    offset_table.update_offset("transform1", "table2", 200.0)
    assert offset_table.get_offset_count() == 2

    offset_table.update_offset("transform2", "table1", 300.0)
    assert offset_table.get_offset_count() == 3

    # Обновление существующего offset'а не увеличивает счетчик
    offset_table.update_offset("transform1", "table1", 150.0)
    assert offset_table.get_offset_count() == 3

    # Сброс offset'а уменьшает счетчик
    offset_table.reset_offset("transform1", "table1")
    assert offset_table.get_offset_count() == 2


def test_offset_table_multiple_transformations(dbconn: DBConn):
    """Тест изоляции offset'ов между трансформациями"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы для разных трансформаций с одинаковыми именами таблиц
    offset_table.update_offset("transform1", "common_table", 100.0)
    offset_table.update_offset("transform2", "common_table", 200.0)
    offset_table.update_offset("transform3", "common_table", 300.0)

    # Проверяем, что offset'ы не смешиваются
    assert offset_table.get_offset("transform1", "common_table") == 100.0
    assert offset_table.get_offset("transform2", "common_table") == 200.0
    assert offset_table.get_offset("transform3", "common_table") == 300.0


def test_offset_table_get_offsets_for_transformation(dbconn: DBConn):
    """Тест получения всех offset'ов для трансформации одним запросом"""
    offset_table = TransformInputOffsetTable(dbconn, create_table=True)

    # Создаем offset'ы для одной трансформации с разными входными таблицами
    offset_table.update_offset("test_transform", "table1", 100.0)
    offset_table.update_offset("test_transform", "table2", 200.0)
    offset_table.update_offset("test_transform", "table3", 300.0)

    # Создаем offset'ы для другой трансформации (не должны попасть в результат)
    offset_table.update_offset("other_transform", "table1", 999.0)

    # Получаем все offset'ы для test_transform одним запросом
    offsets = offset_table.get_offsets_for_transformation("test_transform")

    # Проверяем результат
    assert offsets == {
        "table1": 100.0,
        "table2": 200.0,
        "table3": 300.0,
    }

    # Проверяем для несуществующей трансформации
    empty_offsets = offset_table.get_offsets_for_transformation("nonexistent")
    assert empty_offsets == {}
