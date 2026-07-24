from __future__ import annotations

from typing import Any, Mapping, Protocol, runtime_checkable


class TrainConfigValidationError(ValueError):
    """Raised when training config params fail validation.

    ``errors`` carries a structured list of per-field problems so that API/UI
    layers can render them without parsing the message string.
    """

    def __init__(self, message: str, *, errors: list[dict[str, Any]] | None = None) -> None:
        super().__init__(message)
        self.errors: list[dict[str, Any]] = errors or []


@runtime_checkable
class TrainConfigCodec(Protocol):
    config_type: str

    def normalize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        ...

    def validate(self, params: Mapping[str, Any]) -> dict[str, Any]:
        ...

    def json_schema(self) -> dict[str, Any]:
        ...

    def summarize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        ...


_CODECS: dict[str, TrainConfigCodec] = {}


def register_train_config_codec(codec: TrainConfigCodec) -> None:
    if codec.config_type in _CODECS:
        raise ValueError(f"Train config codec already registered for type: {codec.config_type!r}")
    _CODECS[codec.config_type] = codec


def get_train_config_codec(config_type: str) -> TrainConfigCodec:
    try:
        return _CODECS[config_type]
    except KeyError:
        raise ValueError(f"Unknown train config type: {config_type}")
