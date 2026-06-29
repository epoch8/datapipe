"""
Wrapper для IndexDF с дополнительными метаданными.

Используется для передачи исходного changed_idx (с полным PK) вместе
со сгруппированным idx (по transform_keys) для поддержки strict mode
при использовании max_records_per_run.
"""

from typing import Optional

from datapipe.types import IndexDF


class IndexDFWithMetadata:
    """
    Wrapper для IndexDF с дополнительными метаданными.

    Позволяет передавать исходный changed_idx (с полным PK) вместе
    со сгруппированным idx (по transform_keys) для поддержки strict mode
    при использовании max_records_per_run.

    Ведёт себя как обычный IndexDF для обратной совместимости через делегирование.

    Example:
        >>> idx_grouped = data_to_index(chunk_df, transform_keys)
        >>> idx_with_meta = IndexDFWithMetadata(
        ...     idx=idx_grouped,
        ...     changed_idx=chunk_df  # Исходный с полным PK
        ... )
        >>> len(idx_with_meta)  # Работает как обычный DataFrame
        3
        >>> idx_with_meta.has_changed_idx()
        True
    """

    def __init__(
        self,
        idx: IndexDF,
        changed_idx: Optional[IndexDF] = None
    ):
        """
        Args:
            idx: Основной индекс (обычно сгруппированный по transform_keys)
            changed_idx: Исходный индекс с полным PK из changed_idx query
                        (опционально, используется для strict mode)
        """
        self._idx = idx
        self._changed_idx = changed_idx

    @property
    def idx(self) -> IndexDF:
        """Основной индекс (сгруппированный по transform_keys)"""
        return self._idx

    @property
    def changed_idx(self) -> Optional[IndexDF]:
        """Исходный индекс с полным PK (если есть)"""
        return self._changed_idx

    def has_changed_idx(self) -> bool:
        """Проверка наличия changed_idx"""
        return self._changed_idx is not None

    # Делегирование к pandas DataFrame для обратной совместимости
    def __getattr__(self, name):
        """Делегируем все неизвестные атрибуты к idx"""
        return getattr(self._idx, name)

    def __getitem__(self, key):
        """Поддержка индексации df[key]"""
        return self._idx[key]

    def __len__(self):
        """len(idx_with_meta) возвращает длину основного индекса"""
        return len(self._idx)

    def __repr__(self):
        return (
            f"IndexDFWithMetadata("
            f"idx={len(self._idx)} rows, "
            f"has_changed_idx={self.has_changed_idx()}"
            f")"
        )

    def __str__(self):
        return str(self._idx)

    # Поддержка итерации
    def __iter__(self):
        return iter(self._idx)

    # Поддержка pandas операций
    @property
    def columns(self):
        return self._idx.columns

    @property
    def shape(self):
        return self._idx.shape

    def to_records(self, *args, **kwargs):
        """Делегируем to_records к основному DataFrame"""
        return self._idx.to_records(*args, **kwargs)


def unwrap_idx(idx_or_wrapped) -> tuple[IndexDF, Optional[IndexDF]]:
    """
    Извлекает idx и changed_idx из обёртки или возвращает как есть.

    Этот хелпер позволяет функциям принимать как обычный IndexDF,
    так и IndexDFWithMetadata без явной проверки типа.

    Args:
        idx_or_wrapped: IndexDF или IndexDFWithMetadata

    Returns:
        Кортеж (idx, changed_idx):
            - idx: основной индекс (IndexDF)
            - changed_idx: опциональный исходный индекс с полным PK

    Example:
        >>> idx, changed_idx = unwrap_idx(some_idx)
        >>> if changed_idx is not None:
        ...     # Используем strict mode
        ...     data = dt.get_data(changed_idx)
        >>> else:
        ...     # Legacy mode
        ...     data = dt.get_data(idx)
    """
    if isinstance(idx_or_wrapped, IndexDFWithMetadata):
        return idx_or_wrapped.idx, idx_or_wrapped.changed_idx
    else:
        return idx_or_wrapped, None
