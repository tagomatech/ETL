"""Domain exceptions for the Barchart data clients."""

from __future__ import annotations


class BarchartError(RuntimeError):
    """Base class for failures raised by the Barchart clients."""


class BarchartTransportError(BarchartError):
    """A request could not be sent or completed."""


class BarchartResponseError(BarchartError):
    """Barchart returned an HTTP error response."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class BarchartDecodeError(ValueError):
    """Barchart returned a body that could not be decoded."""


class FuturesDataError(BarchartError):
    """A futures contract history could not be fetched or normalized."""
